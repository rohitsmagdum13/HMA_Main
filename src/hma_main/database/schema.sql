-- src/hma_main/database/schema.sql
-- Minimal, normalized tables for the 4 MBA CSVs. Extend constraints as needed.

CREATE TABLE IF NOT EXISTS member_data (
  member_id        VARCHAR(64) PRIMARY KEY,
  first_name       VARCHAR(128),
  last_name        VARCHAR(128),
  gender           VARCHAR(32),
  dob              DATE,
  plan_id          VARCHAR(64),
  created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS plan_details (
  plan_id          VARCHAR(64) PRIMARY KEY,
  plan_name        VARCHAR(255),
  coverage_start   DATE,
  coverage_end     DATE,
  network          VARCHAR(128),
  created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS deductibles_oop (
  id               BIGINT AUTO_INCREMENT PRIMARY KEY,
  member_id        VARCHAR(64) NOT NULL,
  plan_id          VARCHAR(64),
  calendar_year    INT,
  deductible_total DECIMAL(12,2),
  deductible_used  DECIMAL(12,2),
  oop_max_total    DECIMAL(12,2),
  oop_max_used     DECIMAL(12,2),
  created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_ded_member (member_id),
  KEY idx_ded_plan (plan_id)
);

CREATE TABLE IF NOT EXISTS benefit_accumulator (
  id                   BIGINT AUTO_INCREMENT PRIMARY KEY,
  member_id            VARCHAR(64) NOT NULL,
  plan_id              VARCHAR(64),
  service_category     VARCHAR(128),
  allowed_amount       DECIMAL(12,2),
  utilized_amount      DECIMAL(12,2),
  remaining_amount     AS (allowed_amount - utilized_amount) STORED,
  last_updated         DATE,
  created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_ba_member (member_id),
  KEY idx_ba_plan (plan_id)
);
