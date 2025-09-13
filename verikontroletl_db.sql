-- 1) ETL run log (senin önceki öneriye çok benzer)
CREATE TABLE IF NOT EXISTS etl_run (
  run_id INT AUTO_INCREMENT PRIMARY KEY,
  source_file VARCHAR(255),
  started_at DATETIME,
  finished_at DATETIME,
  row_read INT DEFAULT 0,
  row_valid INT DEFAULT 0,
  row_invalid INT DEFAULT 0,
  row_deduped INT DEFAULT 0,
  md5_hash VARCHAR(64),
  notes TEXT
) ENGINE=InnoDB;

-- 2) Sensör/veri tablosu
CREATE TABLE IF NOT EXISTS measurements (
  id INT AUTO_INCREMENT PRIMARY KEY,
  sensor_id VARCHAR(100) NOT NULL,
  ts DATETIME NOT NULL,
  pm25 DOUBLE,
  quality_flag TINYINT DEFAULT 0,         -- 0 = OK, 1 = missing, 2 = outlier, 3 = timestamp problem ...
  quality_notes TEXT,
  source_file_hash VARCHAR(64),
  UNIQUE KEY uniq_sensor_ts (sensor_id, ts),  -- dedupe için
  INDEX idx_ts (ts),
  INDEX idx_qflag (quality_flag)
) ENGINE=InnoDB;