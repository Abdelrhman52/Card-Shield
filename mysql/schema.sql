-- =============================================================================
-- CardShield — MySQL Monitoring & Analytics Schema
-- Database: cardshield_monitoring
-- Purpose : Staging layer for Power BI dashboards, KPI queries, and
--           operational event logging.
--
-- Table classification:
--   Fact tables : fraud_alerts, transaction_summaries, pipeline_events
--   Dimension   : dim_date, dim_transaction_type, dim_rule
-- =============================================================================

USE cardshield_monitoring;

-- ---------------------------------------------------------------------------
-- Dimension: dim_date
-- Pre-populated date spine for time-based Power BI modeling.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INT           NOT NULL COMMENT 'YYYYMMDD integer key',
    full_date       DATE          NOT NULL,
    year            SMALLINT      NOT NULL,
    quarter         TINYINT       NOT NULL,
    month           TINYINT       NOT NULL,
    month_name      VARCHAR(10)   NOT NULL,
    week_of_year    TINYINT       NOT NULL,
    day_of_month    TINYINT       NOT NULL,
    day_of_week     TINYINT       NOT NULL,
    day_name        VARCHAR(10)   NOT NULL,
    is_weekend      TINYINT(1)    NOT NULL DEFAULT 0,
    PRIMARY KEY (date_key),
    INDEX idx_full_date (full_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Date dimension for Power BI time intelligence';


-- ---------------------------------------------------------------------------
-- Dimension: dim_transaction_type
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_transaction_type (
    type_id         TINYINT       NOT NULL AUTO_INCREMENT,
    type_name       VARCHAR(20)   NOT NULL COMMENT 'PAYMENT|TRANSFER|CASH_OUT|DEBIT|CASH_IN',
    description     VARCHAR(200)  NULL,
    PRIMARY KEY (type_id),
    UNIQUE KEY uq_type_name (type_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO dim_transaction_type (type_name, description) VALUES
('PAYMENT',  'Point-of-sale or online payment'),
('TRANSFER', 'Account-to-account transfer'),
('CASH_OUT', 'ATM or cash withdrawal'),
('DEBIT',    'Direct debit charge'),
('CASH_IN',  'Cash deposit');


-- ---------------------------------------------------------------------------
-- Dimension: dim_rule
-- Maps fraud rule codes to human-readable descriptions.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_rule (
    rule_id         TINYINT       NOT NULL AUTO_INCREMENT,
    rule_code       VARCHAR(30)   NOT NULL COMMENT 'e.g. R1_BLACKLIST',
    rule_name       VARCHAR(100)  NOT NULL,
    description     TEXT          NULL,
    severity        ENUM('LOW','MEDIUM','HIGH','CRITICAL') NOT NULL DEFAULT 'HIGH',
    PRIMARY KEY (rule_id),
    UNIQUE KEY uq_rule_code (rule_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO dim_rule (rule_code, rule_name, severity, description) VALUES
('R1_BLACKLIST',         'Blacklist Hit',             'CRITICAL', 'Card found in HBase blacklist — immediate reject'),
('R2_CREDIT_LIMIT',      'Credit Limit Breach',       'HIGH',     'Transaction amount exceeds cardholder credit limit'),
('R3_GEO_VELOCITY',      'Geographic Velocity',       'HIGH',     'Multiple locations within 5-minute window'),
('R4_HIGH_VALUE_BURST',  'High-Value Burst',          'HIGH',     '3+ large transactions within 5-minute window'),
('R5_BEHAVIORAL_ANOMALY','Behavioral Anomaly',        'MEDIUM',   'Amount > 5× user rolling average'),
('R6_VELOCITY_BURST',    'Velocity Burst',            'MEDIUM',   '10+ transactions within 5-minute window');


-- ---------------------------------------------------------------------------
-- Fact: fraud_alerts
-- One row per fraud alert emitted by the Flink rules engine.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id            BIGINT          NOT NULL AUTO_INCREMENT,
    alert_ts            DATETIME(3)     NOT NULL              COMMENT 'Timestamp of the alert (ms precision)',
    date_key            INT             NOT NULL              COMMENT 'FK → dim_date',
    card_id             VARCHAR(50)     NOT NULL              COMMENT 'nameOrig — source customer ID',
    dest_id             VARCHAR(50)     NULL                  COMMENT 'nameDest — recipient customer ID',
    transaction_type    VARCHAR(20)     NOT NULL              COMMENT 'FK → dim_transaction_type.type_name',
    amount              DECIMAL(18, 2)  NOT NULL,
    rules_triggered     VARCHAR(300)    NOT NULL              COMMENT 'Comma-separated rule codes',
    risk_score          DECIMAL(5, 4)   NOT NULL              COMMENT '0.0000 – 1.0000',
    is_fraud_label      TINYINT(1)      NOT NULL DEFAULT 0    COMMENT 'Ground truth from dataset (1=fraud)',
    is_flagged_fraud    TINYINT(1)      NOT NULL DEFAULT 0    COMMENT 'isFlaggedFraud from dataset',
    review_status       ENUM('PENDING','CONFIRMED_FRAUD','FALSE_POSITIVE','UNDER_REVIEW')
                                        NOT NULL DEFAULT 'PENDING',
    reviewed_by         VARCHAR(50)     NULL,
    reviewed_at         DATETIME        NULL,
    created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (alert_id),
    INDEX idx_card_ts   (card_id, alert_ts),
    INDEX idx_date_key  (date_key),
    INDEX idx_risk_score (risk_score),
    INDEX idx_review    (review_status),
    INDEX idx_alert_ts  (alert_ts),
    CONSTRAINT fk_alert_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Fact: one row per fraud alert from Flink';


-- ---------------------------------------------------------------------------
-- Fact: transaction_summaries
-- Hourly rollups of all transaction activity (fraud + legitimate).
-- Populated by a Flink sink or a scheduled aggregation job.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transaction_summaries (
    summary_id              BIGINT          NOT NULL AUTO_INCREMENT,
    summary_hour            DATETIME        NOT NULL              COMMENT 'Truncated to the hour',
    date_key                INT             NOT NULL,
    transaction_type        VARCHAR(20)     NOT NULL,
    total_transactions      BIGINT          NOT NULL DEFAULT 0,
    total_amount            DECIMAL(22, 2)  NOT NULL DEFAULT 0.00,
    avg_amount              DECIMAL(18, 2)  NOT NULL DEFAULT 0.00,
    max_amount              DECIMAL(18, 2)  NOT NULL DEFAULT 0.00,
    fraud_count             INT             NOT NULL DEFAULT 0,
    fraud_amount            DECIMAL(18, 2)  NOT NULL DEFAULT 0.00,
    fraud_rate_pct          DECIMAL(8, 4)   NOT NULL DEFAULT 0.0000,
    alerts_generated        INT             NOT NULL DEFAULT 0,
    created_at              DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (summary_id),
    UNIQUE KEY uq_hour_type (summary_hour, transaction_type),
    INDEX idx_date_key (date_key),
    INDEX idx_summary_hour (summary_hour),
    CONSTRAINT fk_summary_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Fact: hourly transaction rollups for Power BI';


-- ---------------------------------------------------------------------------
-- Fact: pipeline_health_metrics
-- Operational health of each CardShield pipeline component.
-- Written by Airflow after each DAG run.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_health_metrics (
    metric_id           BIGINT          NOT NULL AUTO_INCREMENT,
    recorded_at         DATETIME(3)     NOT NULL,
    date_key            INT             NOT NULL,
    component           VARCHAR(30)     NOT NULL COMMENT 'KAFKA|FLINK|HBASE|HDFS|AIRFLOW',
    metric_name         VARCHAR(80)     NOT NULL,
    metric_value        DOUBLE          NOT NULL,
    unit                VARCHAR(20)     NULL      COMMENT 'ms, count, pct, MB, etc.',
    status              ENUM('OK','WARNING','CRITICAL','UNKNOWN') NOT NULL DEFAULT 'OK',
    detail              TEXT            NULL,
    PRIMARY KEY (metric_id),
    INDEX idx_component (component, recorded_at),
    INDEX idx_date_key  (date_key),
    CONSTRAINT fk_metric_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Fact: component health metrics from Airflow';


-- ---------------------------------------------------------------------------
-- Fact: pipeline_events (operational audit log)
-- One row per significant pipeline event: DAG run, Safe Mode, backup, etc.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_events (
    event_id            BIGINT          NOT NULL AUTO_INCREMENT,
    event_ts            DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    date_key            INT             NOT NULL,
    event_type          VARCHAR(50)     NOT NULL COMMENT 'DAG_RUN|SAFE_MODE|BACKUP|FLINK_START|FLINK_FAIL|…',
    component           VARCHAR(30)     NOT NULL,
    severity            ENUM('INFO','WARNING','ERROR','CRITICAL') NOT NULL DEFAULT 'INFO',
    dag_run_id          VARCHAR(200)    NULL,
    task_id             VARCHAR(100)    NULL,
    message             TEXT            NULL,
    PRIMARY KEY (event_id),
    INDEX idx_event_ts  (event_ts),
    INDEX idx_event_type (event_type),
    INDEX idx_date_key  (date_key),
    CONSTRAINT fk_event_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Operational audit log for all pipeline events';


-- ---------------------------------------------------------------------------
-- View: vw_fraud_alert_detail
-- Power BI-ready view joining fraud_alerts with dimensions.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_fraud_alert_detail AS
SELECT
    fa.alert_id,
    fa.alert_ts,
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.day_of_week,
    d.day_name,
    d.is_weekend,
    fa.card_id,
    fa.dest_id,
    fa.transaction_type,
    fa.amount,
    fa.rules_triggered,
    fa.risk_score,
    fa.is_fraud_label,
    fa.is_flagged_fraud,
    fa.review_status
FROM fraud_alerts fa
JOIN dim_date d ON d.date_key = fa.date_key;


-- ---------------------------------------------------------------------------
-- View: vw_hourly_fraud_rate
-- Power BI line chart source: fraud rate over time.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_hourly_fraud_rate AS
SELECT
    ts.summary_hour,
    ts.transaction_type,
    ts.total_transactions,
    ts.fraud_count,
    ts.fraud_rate_pct,
    ts.total_amount,
    ts.fraud_amount,
    ts.alerts_generated
FROM transaction_summaries ts
ORDER BY ts.summary_hour DESC;


-- ---------------------------------------------------------------------------
-- View: vw_pipeline_health_latest
-- Latest health status per component — Power BI card visuals.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_pipeline_health_latest AS
SELECT
    m.component,
    m.metric_name,
    m.metric_value,
    m.unit,
    m.status,
    m.recorded_at,
    m.detail
FROM pipeline_health_metrics m
WHERE m.recorded_at = (
    SELECT MAX(m2.recorded_at)
    FROM pipeline_health_metrics m2
    WHERE m2.component = m.component
      AND m2.metric_name = m.metric_name
);
