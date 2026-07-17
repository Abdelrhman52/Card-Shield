-- =============================================================================
-- CardShield — Create Airflow metadata database
-- This script runs before schema.sql via docker-entrypoint-initdb.d ordering.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS airflow CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'airflow'@'%' IDENTIFIED BY 'changeme_airflow';
GRANT ALL PRIVILEGES ON airflow.* TO 'airflow'@'%';
FLUSH PRIVILEGES;
