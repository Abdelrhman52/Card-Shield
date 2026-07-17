"""
CardShield — Flink Job Manager Helper
======================================
Used by the Airflow DAG task `run_rules_guard_job`.

Responsibilities:
  - Submit the Flink fraud detection job via REST API
  - Poll job status until RUNNING/FINISHED/FAILED
  - Expose status for Safe Mode decision in the DAG
"""

import logging
import os
import time
from typing import Optional

import requests

logger = logging.getLogger("cardshield.flink_manager")

FLINK_HOST = os.environ.get("FLINK_JOBMANAGER_HOST", "flink-jobmanager")
FLINK_PORT = int(os.environ.get("FLINK_JOBMANAGER_PORT", "8081"))
FLINK_BASE_URL = f"http://{FLINK_HOST}:{FLINK_PORT}"

# Path to the compiled Flink job JAR (or .py for PyFlink)
# In production, upload the job JAR once and reference its jarId here.
FLINK_JAR_ID = os.environ.get("FLINK_JAR_ID", "")
FLINK_ENTRY_CLASS = os.environ.get(
    "FLINK_ENTRY_CLASS", "com.cardshield.flink.FraudDetectionJob"
)
FLINK_JOB_NAME = "CardShield Fraud Detection Job"

# Terminal job states
TERMINAL_STATES = {"FINISHED", "FAILED", "CANCELED", "SUSPENDED"}
HEALTHY_STATES = {"RUNNING", "FINISHED"}


def _get(path: str, timeout: int = 15) -> dict:
    """GET request to Flink REST API."""
    url = f"{FLINK_BASE_URL}{path}"
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _post(path: str, payload: dict = None, timeout: int = 30) -> dict:
    """POST request to Flink REST API."""
    url = f"{FLINK_BASE_URL}{path}"
    r = requests.post(url, json=payload or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def get_running_job_id() -> Optional[str]:
    """
    Returns the job ID of a currently running CardShield fraud job,
    or None if no such job is found.
    """
    try:
        jobs = _get("/jobs")
        for job in jobs.get("jobs", []):
            if job.get("status") in ("RUNNING", "CREATED"):
                # Check name if available
                try:
                    details = _get(f"/jobs/{job['id']}")
                    if FLINK_JOB_NAME in details.get("name", ""):
                        return job["id"]
                except Exception:  # noqa: BLE001
                    pass
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not list Flink jobs: %s", exc)
    return None


def trigger_flink_job() -> str:
    """
    Submit the CardShield Flink job.

    Strategy:
      1. Check if the job is already RUNNING → return existing job ID.
      2. If a JAR is registered (FLINK_JAR_ID), submit it.
      3. Otherwise, attempt to run via PyFlink REST submission
         (for local/dev mode where fraud_job.py is mounted).

    Returns:
        job_id (str) — the submitted or existing Flink job ID.
    """
    # Check for existing healthy job
    existing = get_running_job_id()
    if existing:
        logger.info("Flink job already running: %s", existing)
        return existing

    if FLINK_JAR_ID:
        # Submit JAR-based job
        logger.info("Submitting Flink JAR job (jarId=%s)…", FLINK_JAR_ID)
        payload = {
            "entryClass": FLINK_ENTRY_CLASS,
            "programArgsList": [],
            "parallelism": 4,
        }
        resp = _post(f"/jars/{FLINK_JAR_ID}/run", payload)
        job_id = resp.get("jobid")
        if not job_id:
            raise RuntimeError(f"Flink job submission returned no jobid: {resp}")
        logger.info("Flink job submitted: job_id=%s", job_id)
        return job_id
    else:
        # Dev/local mode: assume job was started via docker-compose CMD
        # Airflow just monitors health rather than submitting a new job.
        logger.warning(
            "FLINK_JAR_ID not set — skipping submission. "
            "Monitoring existing jobs only."
        )
        existing = get_running_job_id()
        if not existing:
            raise RuntimeError(
                "No running Flink job found and FLINK_JAR_ID is not configured. "
                "Start the Flink job manually or set FLINK_JAR_ID."
            )
        return existing


def wait_for_flink_job(job_id: str, timeout_seconds: int = 600) -> str:
    """
    Poll Flink REST API until the job reaches a terminal or healthy state.

    Returns:
        Final job status string (e.g. "RUNNING", "FINISHED", "FAILED").
    """
    logger.info("Waiting for Flink job %s (timeout=%ds)…", job_id, timeout_seconds)
    deadline = time.monotonic() + timeout_seconds
    poll_interval = 10

    while time.monotonic() < deadline:
        try:
            details = _get(f"/jobs/{job_id}")
            status = details.get("state", "UNKNOWN")
            logger.info("Flink job %s status: %s", job_id, status)

            if status in HEALTHY_STATES or status in TERMINAL_STATES:
                return status

        except Exception as exc:  # noqa: BLE001
            logger.warning("Flink status poll error: %s — retrying in %ds", exc, poll_interval)

        time.sleep(poll_interval)

    logger.error("Timeout waiting for Flink job %s", job_id)
    return "TIMEOUT"
