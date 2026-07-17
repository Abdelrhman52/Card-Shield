-- =============================================================================
-- CardShield — KPI SQL Queries for Power BI
-- Database: cardshield_monitoring
--
-- Each query is labelled by its intended Power BI visual type.
-- =============================================================================

USE cardshield_monitoring;

-- ---------------------------------------------------------------------------
-- KPI 1: Total Alerts Today
-- Visual: Card
-- ---------------------------------------------------------------------------
SELECT COUNT(*) AS alerts_today
FROM fraud_alerts
WHERE DATE(alert_ts) = CURDATE();


-- ---------------------------------------------------------------------------
-- KPI 2: Fraud Detection Rate (overall)
-- Visual: Card / Gauge
-- ---------------------------------------------------------------------------
SELECT
    COUNT(*)                                        AS total_alerts,
    SUM(CASE WHEN is_fraud_label = 1 THEN 1 ELSE 0 END) AS true_positives,
    SUM(CASE WHEN is_fraud_label = 0 THEN 1 ELSE 0 END) AS false_positives,
    ROUND(
        100.0 * SUM(CASE WHEN is_fraud_label = 1 THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0),
        2
    )                                               AS precision_pct
FROM fraud_alerts;


-- ---------------------------------------------------------------------------
-- KPI 3: Average Risk Score by Transaction Type
-- Visual: Bar chart
-- ---------------------------------------------------------------------------
SELECT
    transaction_type,
    ROUND(AVG(risk_score), 4)   AS avg_risk_score,
    COUNT(*)                     AS alert_count,
    SUM(amount)                  AS total_flagged_amount
FROM fraud_alerts
GROUP BY transaction_type
ORDER BY avg_risk_score DESC;


-- ---------------------------------------------------------------------------
-- KPI 4: Top 10 Most-Flagged Cards (last 7 days)
-- Visual: Table
-- ---------------------------------------------------------------------------
SELECT
    card_id,
    COUNT(*)                    AS total_alerts,
    ROUND(AVG(risk_score), 4)  AS avg_risk_score,
    SUM(amount)                 AS total_flagged_amount,
    MAX(alert_ts)               AS last_seen
FROM fraud_alerts
WHERE alert_ts >= DATE_SUB(NOW(), INTERVAL 7 DAY)
GROUP BY card_id
ORDER BY total_alerts DESC
LIMIT 10;


-- ---------------------------------------------------------------------------
-- KPI 5: Hourly Fraud Volume (rolling 24 hours)
-- Visual: Line chart
-- ---------------------------------------------------------------------------
SELECT
    DATE_FORMAT(summary_hour, '%Y-%m-%d %H:00') AS hour_label,
    SUM(total_transactions)                       AS total_txn,
    SUM(fraud_count)                              AS fraud_txn,
    ROUND(AVG(fraud_rate_pct), 4)                AS avg_fraud_rate_pct,
    SUM(total_amount)                             AS total_volume,
    SUM(fraud_amount)                             AS fraud_volume
FROM transaction_summaries
WHERE summary_hour >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
GROUP BY hour_label
ORDER BY hour_label ASC;


-- ---------------------------------------------------------------------------
-- KPI 6: Rule Trigger Frequency
-- Visual: Donut / Horizontal bar
-- ---------------------------------------------------------------------------
SELECT
    r.rule_code,
    r.rule_name,
    r.severity,
    COUNT(fa.alert_id)          AS trigger_count,
    ROUND(
        100.0 * COUNT(fa.alert_id) / NULLIF((SELECT COUNT(*) FROM fraud_alerts), 0),
        2
    )                           AS pct_of_all_alerts
FROM dim_rule r
LEFT JOIN fraud_alerts fa
    ON FIND_IN_SET(r.rule_code, REPLACE(fa.rules_triggered, ' ', '')) > 0
GROUP BY r.rule_code, r.rule_name, r.severity
ORDER BY trigger_count DESC;


-- ---------------------------------------------------------------------------
-- KPI 7: Daily Fraud Summary (last 30 days)
-- Visual: Line + bar combo
-- ---------------------------------------------------------------------------
SELECT
    d.full_date,
    d.day_name,
    COALESCE(SUM(ts.total_transactions), 0)    AS total_transactions,
    COALESCE(SUM(ts.fraud_count), 0)           AS fraud_count,
    COALESCE(SUM(ts.fraud_amount), 0)          AS fraud_amount,
    COALESCE(SUM(ts.alerts_generated), 0)      AS alerts_generated,
    COALESCE(AVG(ts.fraud_rate_pct), 0)        AS avg_fraud_rate_pct
FROM dim_date d
LEFT JOIN transaction_summaries ts ON ts.date_key = d.date_key
WHERE d.full_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY d.full_date, d.day_name
ORDER BY d.full_date ASC;


-- ---------------------------------------------------------------------------
-- KPI 8: Pipeline Health Summary (latest per component)
-- Visual: Status table / traffic lights
-- ---------------------------------------------------------------------------
SELECT
    component,
    metric_name,
    metric_value,
    unit,
    status,
    recorded_at,
    detail
FROM vw_pipeline_health_latest
ORDER BY component, metric_name;


-- ---------------------------------------------------------------------------
-- KPI 9: Pending Reviews (workqueue for fraud analysts)
-- Visual: Table with row-level drill-through
-- ---------------------------------------------------------------------------
SELECT
    alert_id,
    alert_ts,
    card_id,
    transaction_type,
    amount,
    risk_score,
    rules_triggered,
    is_fraud_label,
    review_status
FROM fraud_alerts
WHERE review_status IN ('PENDING', 'UNDER_REVIEW')
ORDER BY risk_score DESC, alert_ts DESC
LIMIT 200;


-- ---------------------------------------------------------------------------
-- KPI 10: Weekly Trend — Confirmed Fraud vs False Positives
-- Visual: Stacked column chart
-- ---------------------------------------------------------------------------
SELECT
    YEARWEEK(alert_ts, 1)       AS iso_week,
    MIN(DATE(alert_ts))         AS week_start,
    SUM(CASE WHEN review_status = 'CONFIRMED_FRAUD'  THEN 1 ELSE 0 END) AS confirmed_fraud,
    SUM(CASE WHEN review_status = 'FALSE_POSITIVE'   THEN 1 ELSE 0 END) AS false_positives,
    SUM(CASE WHEN review_status = 'PENDING'          THEN 1 ELSE 0 END) AS pending,
    COUNT(*)                    AS total_alerts
FROM fraud_alerts
WHERE alert_ts >= DATE_SUB(NOW(), INTERVAL 12 WEEK)
GROUP BY iso_week
ORDER BY iso_week ASC;


-- ---------------------------------------------------------------------------
-- KPI 11: High-Risk Amount Exposure (unreviewed, risk_score > 0.7)
-- Visual: Card — financial exposure metric
-- ---------------------------------------------------------------------------
SELECT
    COUNT(*)            AS high_risk_pending_alerts,
    SUM(amount)         AS total_exposure,
    MAX(risk_score)     AS max_risk_score,
    AVG(amount)         AS avg_high_risk_amount
FROM fraud_alerts
WHERE risk_score >= 0.7
  AND review_status = 'PENDING';


-- ---------------------------------------------------------------------------
-- KPI 12: Safe Mode Events (last 30 days)
-- Visual: Table / timeline
-- ---------------------------------------------------------------------------
SELECT
    event_ts,
    event_type,
    component,
    severity,
    dag_run_id,
    message
FROM pipeline_events
WHERE event_type = 'SAFE_MODE'
  AND event_ts >= DATE_SUB(NOW(), INTERVAL 30 DAY)
ORDER BY event_ts DESC;
