-- utf8mb4 everywhere
CREATE DATABASE IF NOT EXISTS `hma` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE `hma`;

-- logs which S3 files we already loaded (idempotency)
CREATE TABLE IF NOT EXISTS import_log (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  s3_bucket VARCHAR(255) NOT NULL,
  s3_key    VARCHAR(512) NOT NULL,
  etag      VARCHAR(128) NULL,
  file_bytes BIGINT NULL,
  loaded_rows INT NOT NULL DEFAULT 0,
  status    ENUM('LOADED','SKIPPED','ERROR') NOT NULL,
  message   VARCHAR(1000) NULL,
  loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_file (s3_bucket, s3_key)
);

-- STAGING tables mirror CSVs (looser typing so we don’t block on schema guess)
CREATE TABLE IF NOT EXISTS stg_member_data (
  member_id         VARCHAR(64),
  first_name        VARCHAR(128),
  last_name         VARCHAR(128),
  dob               VARCHAR(32),
  gender            VARCHAR(32),
  plan_id           VARCHAR(64),
  group_number      VARCHAR(64),
  raw_json          JSON NULL
);

CREATE TABLE IF NOT EXISTS stg_plan_details (
  plan_id           VARCHAR(64),
  plan_name         VARCHAR(255),
  group_number      VARCHAR(64),
  start_date        VARCHAR(32),
  end_date          VARCHAR(32),
  network           VARCHAR(128),
  raw_json          JSON NULL
);

CREATE TABLE IF NOT EXISTS stg_deductibles_oop (
  plan_id                 VARCHAR(64),
  member_id               VARCHAR(64),
  deductible_individual   DECIMAL(12,2) NULL,
  deductible_family       DECIMAL(12,2) NULL,
  oop_max_individual      DECIMAL(12,2) NULL,
  oop_max_family          DECIMAL(12,2) NULL,
  period                  VARCHAR(32),
  raw_json                JSON NULL
);

CREATE TABLE IF NOT EXISTS stg_benefit_accumulator (
  plan_id            VARCHAR(64),
  member_id          VARCHAR(64),
  benefit_name       VARCHAR(255),
  allowed_amount     DECIMAL(12,2) NULL,
  used_amount        DECIMAL(12,2) NULL,
  remaining_amount   DECIMAL(12,2) NULL,
  period             VARCHAR(32),
  raw_json           JSON NULL
);
