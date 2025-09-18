-- Create database
CREATE DATABASE IF NOT EXISTS hma_data;
USE hma_data;

-- Member Data table
CREATE TABLE IF NOT EXISTS member_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    member_id VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_member_id (member_id),
    INDEX idx_name (last_name, first_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Deductibles and Out of Pocket table
CREATE TABLE IF NOT EXISTS deductibles_oop (
    id INT AUTO_INCREMENT PRIMARY KEY,
    metric VARCHAR(100) NOT NULL,
    member_id VARCHAR(50),
    m1001 DECIMAL(10,2),
    m1002 DECIMAL(10,2),
    m1003 DECIMAL(10,2),
    m1004 DECIMAL(10,2),
    m1005 DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (member_id) REFERENCES member_data(member_id) ON DELETE CASCADE,
    INDEX idx_metric (metric),
    INDEX idx_member (member_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Benefit Accumulator table
CREATE TABLE IF NOT EXISTS benefit_accumulator (
    id INT AUTO_INCREMENT PRIMARY KEY,
    member_id VARCHAR(50) NOT NULL,
    service VARCHAR(200),
    allowed_limit VARCHAR(100),
    used INT DEFAULT 0,
    remaining INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (member_id) REFERENCES member_data(member_id) ON DELETE CASCADE,
    INDEX idx_member_service (member_id, service)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Plan Details table
CREATE TABLE IF NOT EXISTS plan_details (
    id INT AUTO_INCREMENT PRIMARY KEY,
    member_id VARCHAR(50) NOT NULL,
    group_number INT,
    plan_id VARCHAR(50),
    plan_name VARCHAR(200),
    plan_detail DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (member_id) REFERENCES member_data(member_id) ON DELETE CASCADE,
    INDEX idx_plan (plan_id),
    INDEX idx_member_plan (member_id, plan_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ETL Job Tracking table
CREATE TABLE IF NOT EXISTS etl_jobs (
    job_id VARCHAR(100) PRIMARY KEY,
    job_type VARCHAR(50),
    source_file VARCHAR(500),
    status ENUM('pending', 'processing', 'completed', 'failed') DEFAULT 'pending',
    records_processed INT DEFAULT 0,
    records_failed INT DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_status (status),
    INDEX idx_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Data Quality Checks table
CREATE TABLE IF NOT EXISTS data_quality_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    job_id VARCHAR(100),
    check_type VARCHAR(100),
    table_name VARCHAR(100),
    check_result ENUM('pass', 'fail', 'warning'),
    details JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES etl_jobs(job_id) ON DELETE CASCADE,
    INDEX idx_job (job_id),
    INDEX idx_result (check_result)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;