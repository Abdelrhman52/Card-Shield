#!/usr/bin/env python3
"""
CardShield — Kafka/Avro to Power BI Streaming Dataset Bridge
==============================================================

Consumes Avro-encoded transactions from Kafka (decoded via Confluent
Schema Registry), enriches each record with fields a live fraud-ops
dashboard needs, batches them, and POSTs them as JSON rows to a Power BI
push/streaming dataset. A background thread separately probes the
CardShield infra services (Kafka, Flink, HBase, HDFS, Airflow) and pushes
their health into a second streaming table.

Environment variables (see .env.example):
    KAFKA_BOOTSTRAP_SERVERS       e.g. localhost:9092
    KAFKA_TOPIC                   default: card-transactions
    KAFKA_GROUP_ID                default: cardshield-powerbi-bridge
    SCHEMA_REGISTRY_URL           e.g. http://localhost:8081
    POWER_BI_PUSH_URL_TXN         Push URL for the Transactions table
    POWER_BI_PUSH_URL_HEALTH      Push URL for the SystemHealth table
    HEALTH_CHECK_INTERVAL_SEC     default: 15
    BATCH_SIZE                    default: 20
    BATCH_INTERVAL_SEC            default: 1.0
    LOG_LEVEL                     default: INFO

    # Endpoints used for real health probes (adjust to your compose stack)
    HBASE_THRIFT_HOST / HBASE_THRIFT_PORT   default: localhost / 9090
    HDFS_NAMENODE_URL                       default: http://localhost:9870
    FLINK_JOBMANAGER_URL                    default: http://localhost:8082/overview
    AIRFLOW_WEBSERVER_URL                   default: http://localhost:8083/health
"""

import json
import logging
import os
import random
import signal
import socket
import threading
import time
import uuid
from datetime import datetime, timezone
from queue import Empty, Queue

import requests
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from dotenv import load_dotenv

load_dotenv()

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "card-transactions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "cardshield-powerbi-bridge")
SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]

POWER_BI_PUSH_URL_TXN = os.environ["POWER_BI_PUSH_URL_TXN"]
POWER_BI_PUSH_URL_HEALTH = os.environ["POWER_BI_PUSH_URL_HEALTH"]

HEALTH_CHECK_INTERVAL_SEC = float(os.getenv("HEALTH_CHECK_INTERVAL_SEC", "15"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
BATCH_INTERVAL_SEC = float(os.getenv("BATCH_INTERVAL_SEC", "1.0"))

HBASE_THRIFT_HOST = os.getenv("HBASE_THRIFT_HOST", "localhost")
HBASE_THRIFT_PORT = int(os.getenv("HBASE_THRIFT_PORT", "9090"))
HDFS_NAMENODE_URL = os.getenv("HDFS_NAMENODE_URL", "http://localhost:9870")
FLINK_JOBMANAGER_URL = os.getenv("FLINK_JOBMANAGER_URL", "http://localhost:8082/overview")
AIRFLOW_WEBSERVER_URL = os.getenv("AIRFLOW_WEBSERVER_URL", "http://localhost:8083/health")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | %(threadName)-18s | %(message)s",
)
log = logging.getLogger("cardshield.bridge")

COUNTRY_POOL = ["Egypt", "India", "USA", "Brazil", "UK"]
FRAUD_RULES = ["Geographic Velocity", "Card Testing", "High Amount", "Blacklisted Card", "Velocity (Time)"]

HIGH_AMOUNT_THRESHOLD = 2000.0

shutdown_event = threading.Event()


# --------------------------------------------------------------------------- #
# Enrichment helpers
# --------------------------------------------------------------------------- #
def mask_card(account_name: str) -> str:
    """Deterministic pseudo-PAN mask, stable per account so the same
    customer always renders the same masked card in the UI."""
    digits = "".join(ch for ch in account_name if ch.isdigit()) or "0"
    last4 = (digits * 4)[-4:]
    return f"**** **** **** {last4}"


def assign_country(account_name: str) -> str:
    """Logically (deterministically) bucket an account into a country
    for the map visual, based on a stable hash of its ID."""
    idx = hash(account_name) % len(COUNTRY_POOL)
    return COUNTRY_POOL[idx]


def assign_score_and_rule(is_fraud: int, amount: float) -> tuple[float, str]:
    if is_fraud:
        score = round(random.uniform(0.85, 1.0), 2)
        if amount >= HIGH_AMOUNT_THRESHOLD:
            rule = "High Amount"
        else:
            rule = random.choice(FRAUD_RULES)
        return score, rule
    return round(random.uniform(0.01, 0.20), 2), "None"


def enrich_transaction(record: dict) -> dict:
    is_fraud = int(record.get("isFraud", 0))
    amount = float(record.get("amount", 0.0))
    score, rule = assign_score_and_rule(is_fraud, amount)

    ts_ms = record.get("ingestion_timestamp_ms")
    ts = (
        datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        if ts_ms
        else datetime.now(tz=timezone.utc)
    )

    return {
        "Time": ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "Transaction_ID": f"TXN-{uuid.uuid4().hex[:8].upper()}",
        "Card_Mask": mask_card(record.get("nameOrig", "")),
        "Amount": amount,
        "Country": assign_country(record.get("nameOrig", "")),
        "Status": "Blocked" if is_fraud else "Approved",
        "Score": score,
        "Rule": rule,
        "isFraud": is_fraud,
    }


# --------------------------------------------------------------------------- #
# Power BI push client (batched, retried)
# --------------------------------------------------------------------------- #
class PowerBIPusher:
    def __init__(self, push_url: str, table_label: str, timeout_sec: float = 5.0):
        self.push_url = push_url
        self.table_label = table_label
        self.timeout_sec = timeout_sec
        self.session = requests.Session()

    def push(self, rows: list[dict], max_retries: int = 4) -> bool:
        if not rows:
            return True
        payload = json.dumps({"rows": rows})
        backoff = 1.0
        for attempt in range(1, max_retries + 1):
            try:
                resp = self.session.post(
                    self.push_url,
                    data=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=self.timeout_sec,
                )
                if resp.status_code in (200, 202):
                    log.debug("Pushed %d row(s) to %s", len(rows), self.table_label)
                    return True
                log.warning(
                    "%s push rejected (%s attempt %d/%d): %s",
                    self.table_label, resp.status_code, attempt, max_retries, resp.text[:200],
                )
            except requests.exceptions.Timeout:
                log.warning("%s push timed out (attempt %d/%d)", self.table_label, attempt, max_retries)
            except requests.exceptions.RequestException as exc:
                log.warning("%s push failed (attempt %d/%d): %s", self.table_label, attempt, max_retries, exc)

            if attempt < max_retries:
                time.sleep(backoff)
                backoff = min(backoff * 2, 15)

        log.error("%s push permanently failed after %d attempts — dropping %d row(s)",
                   self.table_label, max_retries, len(rows))
        return False


# --------------------------------------------------------------------------- #
# System health probes (real checks against the CardShield compose stack)
# --------------------------------------------------------------------------- #
def check_tcp(host: str, port: int, timeout: float = 2.0) -> float | None:
    start = time.monotonic()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return (time.monotonic() - start) * 1000.0
    except OSError:
        return None


def check_http(url: str, timeout: float = 2.0) -> float | None:
    start = time.monotonic()
    try:
        resp = requests.get(url, timeout=timeout)
        if resp.status_code < 500:
            return (time.monotonic() - start) * 1000.0
    except requests.exceptions.RequestException:
        pass
    return None


def probe_system_health() -> list[dict]:
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    kafka_host, _, kafka_port = KAFKA_BOOTSTRAP_SERVERS.partition(":")
    checks = {
        "Kafka": check_tcp(kafka_host, int(kafka_port or 9092)),
        "Flink": check_http(FLINK_JOBMANAGER_URL),
        "HBase": check_tcp(HBASE_THRIFT_HOST, HBASE_THRIFT_PORT),
        "HDFS": check_http(HDFS_NAMENODE_URL),
        "Airflow": check_http(AIRFLOW_WEBSERVER_URL),
    }
    rows = []
    for component, latency_ms in checks.items():
        rows.append({
            "Time": now,
            "System_Component": component,
            "Component_Status": "Healthy" if latency_ms is not None else "Down",
            "Latency_ms": round(latency_ms, 1) if latency_ms is not None else -1,
        })
    return rows


def health_check_loop(pusher: PowerBIPusher):
    while not shutdown_event.is_set():
        try:
            rows = probe_system_health()
            pusher.push(rows)
        except Exception:
            log.exception("Unexpected error in health check loop")
        shutdown_event.wait(HEALTH_CHECK_INTERVAL_SEC)
    log.info("Health check loop exited cleanly")


# --------------------------------------------------------------------------- #
# Kafka consumer + batching loop
# --------------------------------------------------------------------------- #
def build_consumer() -> DeserializingConsumer:
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client)

    return DeserializingConsumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "key.deserializer": lambda k, ctx: k,
        "value.deserializer": avro_deserializer,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })


def consume_loop(consumer: DeserializingConsumer, pusher: PowerBIPusher):
    buffer: list[dict] = []
    last_flush = time.monotonic()

    def flush():
        nonlocal buffer, last_flush
        if buffer:
            pusher.push(buffer)
            buffer = []
        last_flush = time.monotonic()

    try:
        consumer.subscribe([KAFKA_TOPIC])
        log.info("Subscribed to topic '%s'", KAFKA_TOPIC)

        while not shutdown_event.is_set():
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                if time.monotonic() - last_flush >= BATCH_INTERVAL_SEC:
                    flush()
                continue
            if msg.error():
                log.error("Kafka consumer error: %s", msg.error())
                continue

            try:
                record = msg.value()
                if record is None:
                    continue
                enriched = enrich_transaction(record)
                buffer.append(enriched)
            except Exception:
                log.exception("Failed to decode/enrich message at offset %s", msg.offset())
                continue

            if len(buffer) >= BATCH_SIZE or time.monotonic() - last_flush >= BATCH_INTERVAL_SEC:
                flush()

    finally:
        flush()
        log.info("Closing Kafka consumer")
        consumer.close()


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #
def handle_signal(signum, _frame):
    log.info("Received signal %s — shutting down gracefully", signum)
    shutdown_event.set()


def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    txn_pusher = PowerBIPusher(POWER_BI_PUSH_URL_TXN, "Transactions")
    health_pusher = PowerBIPusher(POWER_BI_PUSH_URL_HEALTH, "SystemHealth")

    health_thread = threading.Thread(
        target=health_check_loop, args=(health_pusher,), name="health-check", daemon=True
    )
    health_thread.start()

    consumer = build_consumer()
    log.info("CardShield Power BI bridge starting up")
    try:
        consume_loop(consumer, txn_pusher)
    finally:
        shutdown_event.set()
        health_thread.join(timeout=5)
        log.info("Shutdown complete")


if __name__ == "__main__":
    main()
