"""
CardShield — Apache Airflow DAG: Fraud Protection Pipeline
===========================================================
DAG Name  : fraud_protection_pipeline
Schedule  : Daily at 12:30 AM UTC
Tasks     :
  1. test_card_gateway_stream  — verify Kafka stream health
  2. run_rules_guard_job       — trigger & monitor the Flink fraud rules job
  3. hbase_table_backup        — backup HBase Blacklist + UserProfiles to HDFS

Retry policy  : 4 retries, 5-minute delay between attempts
Safe Mode     : Enabled automatically when Flink job fails
Alerting      : Slack webhook on task/DAG failure (configure SLACK_WEBHOOK_URL)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Helpers (in dags/helpers/)
from helpers.kafka_health import check_kafka_health
from helpers.flink_manager import trigger_flink_job, wait_for_flink_job
from helpers.hbase_backup import backup_hbase_tables

logger = logging.getLogger("cardshield.dag")

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "cardshield",
    "depends_on_past": False,
    "email_on_failure": False,         # using Slack instead
    "email_on_retry": False,
    "retries": 4,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": False,
    "max_retry_delay": timedelta(minutes=30),
}

# ---------------------------------------------------------------------------
# Slack alert callback
# ---------------------------------------------------------------------------
def _slack_failure_callback(context: dict):
    """Send a Slack alert on task/DAG failure."""
    import os
    import requests
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not set — Slack alert skipped.")
        return

    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    exec_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    message = {
        "text": (
            f":red_circle: *CardShield Alert — Task Failed*\n"
            f">  *DAG*: `{dag_id}`\n"
            f">  *Task*: `{task_id}`\n"
            f">  *Execution date*: `{exec_date}`\n"
            f">  <{log_url}|View task logs>"
        )
    }
    try:
        resp = requests.post(webhook_url, json=message, timeout=10)
        resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        logger.error("Slack notification failed: %s", exc)


def _slack_dag_failure_callback(context: dict):
    """DAG-level failure callback — sent when the whole DAG fails."""
    import os
    import requests
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        return
    dag_id = context.get("dag").dag_id
    requests.post(
        webhook_url,
        json={"text": f":fire: *CardShield DAG FAILED*: `{dag_id}` — Safe Mode may be active."},
        timeout=10,
    )


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def task_test_card_gateway_stream(**context):
    """
    Verifies that the Kafka `card-transactions` topic is healthy:
      - broker reachability
      - consumer lag within acceptable bounds
      - partition leadership checks
    Raises an exception on failure, which triggers the retry policy.
    """
    logger.info("=== Task: test_card_gateway_stream ===")
    result = check_kafka_health()
    if not result["healthy"]:
        raise RuntimeError(
            f"Kafka health check failed: {result['reason']} — "
            "retrying per retry policy. Safe Mode will activate after all retries exhaust."
        )
    logger.info("Kafka stream health OK: %s", result)
    context["ti"].xcom_push(key="kafka_health", value=result)


def task_run_rules_guard_job(**context):
    """
    Triggers the Apache Flink fraud detection job and monitors it until completion.
    If Flink fails, Airflow enters Safe Mode:
      - automatic transaction approval is paused
      - security team is notified
      - incident is logged for post-mortem
    """
    logger.info("=== Task: run_rules_guard_job ===")

    # Submit the Flink job
    job_id = trigger_flink_job()
    logger.info("Flink job submitted: job_id=%s", job_id)
    context["ti"].xcom_push(key="flink_job_id", value=job_id)

    # Wait for job to reach RUNNING or FINISHED state
    status = wait_for_flink_job(job_id, timeout_seconds=600)

    if status not in ("RUNNING", "FINISHED"):
        _activate_safe_mode(context, reason=f"Flink job {job_id} status={status}")
        raise RuntimeError(
            f"Flink job {job_id} did not reach healthy state (status={status}). "
            "Safe Mode activated — automatic approvals paused."
        )

    logger.info("Flink rules guard job is healthy (status=%s)", status)


def _activate_safe_mode(context: dict, reason: str):
    """
    Safe Mode activation:
      - Sets an Airflow Variable `CARDSHIELD_SAFE_MODE=true`
      - Sends an immediate Slack alert (beyond the regular failure callback)
      - Logs a structured incident record
    """
    import os
    import requests
    from airflow.models import Variable

    logger.critical("SAFE MODE ACTIVATED — %s", reason)
    Variable.set("CARDSHIELD_SAFE_MODE", "true")

    # Notify security team
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if webhook_url:
        try:
            requests.post(
                webhook_url,
                json={
                    "text": (
                        f":rotating_light: *SAFE MODE ACTIVE*\n"
                        f"> Automatic transaction approval has been *PAUSED*.\n"
                        f"> Reason: `{reason}`\n"
                        f"> All transactions require manual review until Flink is restored.\n"
                        f"> Run the Flink job manually and set `CARDSHIELD_SAFE_MODE=false` to resume."
                    )
                },
                timeout=10,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Safe Mode Slack notification failed: %s", exc)


def task_hbase_table_backup(**context):
    """
    Compresses and archives HBase tables (Blacklist, UserProfiles, FraudAlerts)
    to HDFS under /cardshield/hbase-backups/YYYY-MM-DD/.
    Used for security auditing, disaster recovery, and regulatory compliance.
    """
    logger.info("=== Task: hbase_table_backup ===")
    backup_result = backup_hbase_tables()
    if not backup_result["success"]:
        raise RuntimeError(
            f"HBase backup failed: {backup_result['reason']}"
        )
    logger.info("HBase backup completed: %s", backup_result)
    context["ti"].xcom_push(key="hbase_backup_result", value=backup_result)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="fraud_protection_pipeline",
    description="CardShield — real-time fraud pipeline orchestration and health supervision",
    schedule_interval="30 0 * * *",      # Daily at 12:30 AM UTC
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["cardshield", "fraud", "kafka", "flink", "hbase"],
    on_failure_callback=_slack_dag_failure_callback,
    doc_md=__doc__,
) as dag:

    # -----------------------------------------------------------------------
    # Task 1: Verify Kafka stream
    # -----------------------------------------------------------------------
    test_card_gateway_stream = PythonOperator(
        task_id="test_card_gateway_stream",
        python_callable=task_test_card_gateway_stream,
        on_failure_callback=_slack_failure_callback,
        doc_md=(
            "Verifies Kafka topic `card-transactions` is reachable and healthy. "
            "4 retries × 5-minute delay before DAG fails."
        ),
    )

    # -----------------------------------------------------------------------
    # Task 2: Trigger and monitor Flink fraud rules job
    # -----------------------------------------------------------------------
    run_rules_guard_job = PythonOperator(
        task_id="run_rules_guard_job",
        python_callable=task_run_rules_guard_job,
        on_failure_callback=_slack_failure_callback,
        doc_md=(
            "Triggers the Apache Flink fraud detection job and monitors it. "
            "Activates Safe Mode on failure."
        ),
    )

    # -----------------------------------------------------------------------
    # Task 3: HBase backup to HDFS
    # Runs regardless of Flink status (TriggerRule.ALL_DONE)
    # to ensure backup is never skipped by upstream failures.
    # -----------------------------------------------------------------------
    hbase_table_backup = PythonOperator(
        task_id="hbase_table_backup",
        python_callable=task_hbase_table_backup,
        trigger_rule=TriggerRule.ALL_DONE,   # always run backup
        on_failure_callback=_slack_failure_callback,
        doc_md=(
            "Backs up HBase tables to HDFS. "
            "Uses TriggerRule.ALL_DONE to guarantee backup runs even if Flink fails."
        ),
    )

    # -----------------------------------------------------------------------
    # Task dependencies:
    #   test_card_gateway_stream >> run_rules_guard_job >> hbase_table_backup
    # -----------------------------------------------------------------------
    test_card_gateway_stream >> run_rules_guard_job >> hbase_table_backup
