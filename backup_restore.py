import subprocess, os, datetime

db_user = "root"
db_pass = "senin_sifren"   # NOT: prod ortamda .my.cnf kullan, koda yazma
db_name = "verikontroletl_db"
backup_file = f"backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"

env = os.environ.copy()
env["MYSQL_PWD"] = db_pass  # parolayı process args'ta göstermemek için

# --- Backup ---
cmd = [
    "mysqldump",
    "-u", db_user,
    "--single-transaction",
    "--routines",
    "--triggers",
    db_name
]
with open(backup_file, "wb") as out:
    subprocess.run(cmd, stdout=out, env=env, check=True)

print("Yedek alındı:", backup_file)

# --- Restore (örnek: testdb'ye) ---
test_db = "testdb"
with open(backup_file, "rb") as inf:
    subprocess.run(["mysql", "-u", db_user, test_db], stdin=inf, env=env, check=True)

print("Restore tamam.")
