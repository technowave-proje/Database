import os, csv, hashlib, datetime, time, logging
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import pooling
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

load_dotenv()
logging.basicConfig(level=logging.INFO, filename='etl.log',
                    format='%(asctime)s %(levelname)s %(message)s')

DB_CONFIG = {
    'host': os.getenv('DB_HOST','localhost'),
    'user': os.getenv('DB_USER','root'),
    'password': os.getenv('DB_PASS',''),
    'database': os.getenv('DB_NAME','verikontroletl_db'),
}

DATA_FILE = os.getenv('DATA_FILE','./data/raw/sample_pm25_kısa.csv')

pool = pooling.MySQLConnectionPool(pool_name="mypool",
                                   pool_size=5,
                                   **DB_CONFIG)

def file_md5(path):
    h = hashlib.md5()
    with open(path,'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
    return h.hexdigest()

def compute_quality(row):
    """
    Basit kurallar:
      - pm25 boş -> flag 1
      - pm25 non-numeric veya <0 veya >500 -> flag 2
      - ts parse hatası -> flag 3
    """
    notes = []
    flag = 0
    pm = row.get('pm25','')
    ts = row.get('ts','')
    if pm == '' or pm is None:
        flag = max(flag,1); notes.append('pm25 missing')
    else:
        try:
            val = float(pm)
            if val < 0 or val > 500:
                flag = max(flag,2); notes.append('pm25 out of range')
        except Exception:
            flag = max(flag,2); notes.append('pm25 not numeric')
    # timestamp kontrolü
    try:
        datetime.datetime.fromisoformat(ts)
    except Exception:
        flag = max(flag,3); notes.append('timestamp parse error')

    print(f"TS value: '{ts}'")
    return flag, ';'.join(notes)

def upsert_rows(conn, tuples):
    sql = """
    INSERT INTO measurements (sensor_id, ts, pm25, quality_flag, quality_notes, source_file_hash)
    VALUES (%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      pm25 = VALUES(pm25),
      quality_flag = GREATEST(quality_flag, VALUES(quality_flag)),
      quality_notes = CONCAT(IFNULL(quality_notes,''),' ; ',VALUES(quality_notes)),
      source_file_hash = VALUES(source_file_hash)
    """
    cur = conn.cursor()
    cur.executemany(sql, tuples)
    conn.commit()
    cur.close()

def simulate_fetch_and_insert():
    started = datetime.datetime.now()
    t0 = time.time()
    conn = pool.get_connection()
    run_id = None
    rows_read = rows_valid = rows_invalid = rows_deduped = 0
    try:
        filehash = file_md5(DATA_FILE)
        t1 = time.time()
        with open(DATA_FILE, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            to_insert = []
            for r in reader:
                rows_read += 1
                flag, notes = compute_quality(r)
                sensor = r.get('sensor_id') or r.get('id') or 'UNKNOWN'
                ts = (r.get('ts') or '').strip()
                pm = r.get('pm25') or None
                if flag == 0:
                    rows_valid += 1
                else:
                    rows_invalid += 1
                to_insert.append((sensor, ts, pm, flag, notes, filehash))
        logging.info(f"File reading time: {time.time()-t1:.2f} sn")

        t2 = time.time()
        upsert_rows(conn, to_insert)
        logging.info(f"db insert time: {time.time()-t2:.2f} sn")

        rows_deduped = 0
        logging.info(f'Run finished. Read={rows_read} valid={rows_valid} invalid={rows_invalid}')
    except Exception as e:
        logging.exception("simulate_fetch failed")
        raise
    finally:
        conn.close()
        finished = datetime.datetime.now()
        conn2 = pool.get_connection()
        cur = conn2.cursor()
        cur.execute("""INSERT INTO etl_run (source_file, started_at, finished_at, row_read, row_valid, row_invalid, row_deduped, md5_hash)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (os.path.basename(DATA_FILE), started, finished, rows_read, rows_valid, rows_invalid, rows_deduped, filehash))
        conn2.commit()
        cur.close()
        conn2.close()
    logging.info(f"total job time: {time.time()-t0:.2f} sn")

sched = BackgroundScheduler()
sched.add_job(simulate_fetch_and_insert, IntervalTrigger(seconds=30), id='fetch_job', max_instances=1)

from apscheduler.triggers.cron import CronTrigger
def daily_report():
    conn = pool.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM measurements WHERE DATE(ts)=CURDATE()")
    total = cur.fetchone()[0]
    cur.execute("SELECT quality_flag, COUNT(*) FROM measurements WHERE DATE(ts)=CURDATE() GROUP BY quality_flag")
    breakdown = cur.fetchall()
    with open('reports/daily_report_'+datetime.date.today().isoformat()+'.txt','w', encoding='utf-8') as f:
        f.write(f"Total today: {total}\n")
        for flag, cnt in breakdown:
            f.write(f"flag {flag}: {cnt}\n")
    cur.close()
    conn.close()

sched.add_job(daily_report, CronTrigger(hour=23, minute=59), id='daily_report')

if __name__ == '__main__':
    sched.start()
    logging.info("Scheduler started. Ctrl+C to exit.")
    try:
        while True:
            time.sleep(10)
    except (KeyboardInterrupt, SystemExit):
        sched.shutdown()
        logging.info("Scheduler stopped.")
