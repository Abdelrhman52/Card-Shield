#!/usr/bin/env python3
"""
CardShield — Kafka Avro Transaction Producer
=============================================
Reads Fraud.csv line by line and publishes each transaction to the
`card-transactions` Kafka topic as an Avro-encoded binary message.

Key features:
  - Sequential CSV reading (no full-file load into memory)
  - Adjustable rate limiting (PRODUCER_RATE_LIMIT msgs/sec, 0 = unlimited)
  - Avro serialization via Confluent Schema Registry
  - Configurable batch size and linger time
  - Retry logic with exponential back-off
  - Structured logging
  - Graceful shutdown on SIGINT / SIGTERM

Environment variables (see .env.example):
  KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, KAFKA_TOPIC_TRANSACTIONS,
  PRODUCER_CSV_PATH, PRODUCER_RATE_LIMIT, PRODUCER_BATCH_SIZE, PRODUCER_LINGER_MS
"""

import csv
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Optional

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("cardshield.producer")


# ---------------------------------------------------------------------------
# Configuration (from environment)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
SCHEMA_REGISTRY_URL: str = os.environ.get(
    "SCHEMA_REGISTRY_URL", "http://localhost:8081"
)
KAFKA_TOPIC: str = os.environ.get("KAFKA_TOPIC_TRANSACTIONS", "card-transactions")
CSV_PATH: str = os.environ.get("PRODUCER_CSV_PATH", "/data/Fraud.csv")
RATE_LIMIT: float = float(os.environ.get("PRODUCER_RATE_LIMIT", "500"))  # msgs/sec
BATCH_SIZE: int = int(os.environ.get("PRODUCER_BATCH_SIZE", "100"))
LINGER_MS: int = int(os.environ.get("PRODUCER_LINGER_MS", "10"))

AVRO_SCHEMA_PATH: str = str(
    Path(__file__).parent / "transaction.avsc"
)

# Max retries for producer delivery failures
MAX_DELIVERY_RETRIES: int = 3

# ---------------------------------------------------------------------------
# Graceful shutdown flag
# ---------------------------------------------------------------------------
_shutdown_requested: bool = False


def _handle_signal(sig, _frame):
    global _shutdown_requested
    logger.info("Shutdown signal received (%s). Flushing and exiting…", sig)
    _shutdown_requested = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ---------------------------------------------------------------------------
# Avro schema loading
# ---------------------------------------------------------------------------
def load_avro_schema_string(path: str) -> str:
    with open(path, "r") as f:
        return f.read()


# ---------------------------------------------------------------------------
# Delivery callback — logs failures per message
# ---------------------------------------------------------------------------
def _delivery_callback(err, msg):
    if err:
        logger.error(
            "Message delivery FAILED | topic=%s partition=%d offset=%d error=%s",
            msg.topic(), msg.partition(), msg.offset(), err,
        )
    else:
        logger.debug(
            "Delivered | topic=%s partition=%d offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )


# ---------------------------------------------------------------------------
# CSV → dict converter with type coercion matching the Avro schema
# ---------------------------------------------------------------------------
TRANSACTION_TYPES = {"PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"}


def parse_row(row: dict, ingestion_ts_ms: int) -> Optional[dict]:
    """
    Parse and validate a CSV row into an Avro-compatible Python dict.
    Returns None on any parsing failure (malformed row).
    """
    try:
        tx_type = row["type"].strip().upper()
        if tx_type not in TRANSACTION_TYPES:
            logger.warning("Unknown transaction type '%s', skipping row.", tx_type)
            return None

        return {
            "step": int(row["step"]),
            "type": tx_type,
            "amount": float(row["amount"]),
            "nameOrig": row["nameOrig"].strip(),
            "oldbalanceOrg": float(row["oldbalanceOrg"]),
            "newbalanceOrig": float(row["newbalanceOrig"]),
            "nameDest": row["nameDest"].strip(),
            "oldbalanceDest": float(row["oldbalanceDest"]),
            "newbalanceDest": float(row["newbalanceDest"]),
            "isFraud": int(row["isFraud"]),
            "isFlaggedFraud": int(row["isFlaggedFraud"]),
            "ingestion_timestamp_ms": ingestion_ts_ms,
        }
    except (KeyError, ValueError, TypeError) as exc:
        logger.warning("Malformed row skipped — %s | row=%s", exc, row)
        return None


# ---------------------------------------------------------------------------
# Rate limiter — simple token-bucket approach
# ---------------------------------------------------------------------------
class RateLimiter:
    """Enforces a maximum number of operations per second."""

    def __init__(self, rate: float):
        self.rate = rate
        self._min_interval = 1.0 / rate if rate > 0 else 0.0
        self._last_call = 0.0

    def wait(self):
        if self._min_interval <= 0:
            return
        now = time.monotonic()
        elapsed = now - self._last_call
        sleep_time = self._min_interval - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)
        self._last_call = time.monotonic()


# ---------------------------------------------------------------------------
# Main producer loop
# ---------------------------------------------------------------------------
def run_producer():
    logger.info("CardShield Kafka Avro Producer starting…")
    logger.info(
        "Config: broker=%s topic=%s csv=%s rate_limit=%.0f msg/s batch=%d linger=%dms",
        KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, CSV_PATH,
        RATE_LIMIT, BATCH_SIZE, LINGER_MS,
    )

    # -- Schema Registry client + Avro serializer ---------------------------
    schema_str = load_avro_schema_string(AVRO_SCHEMA_PATH)
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=schema_str,
    )

    # -- Kafka producer config -----------------------------------------------
    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "batch.size": BATCH_SIZE * 1024,   # bytes; BATCH_SIZE is used as a msg count hint
        "linger.ms": LINGER_MS,
        "acks": "all",
        "retries": MAX_DELIVERY_RETRIES,
        "retry.backoff.ms": 500,
        "compression.type": "lz4",
        "enable.idempotence": True,
    }
    producer = Producer(producer_conf)

    rate_limiter = RateLimiter(RATE_LIMIT)

    total_sent = 0
    total_skipped = 0
    total_errors = 0
    start_time = time.monotonic()

    csv_file = open(CSV_PATH, "r", newline="", encoding="utf-8")
    reader = csv.DictReader(csv_file)

    try:
        for row in reader:
            if _shutdown_requested:
                break

            ingestion_ts_ms = int(time.time() * 1000)
            record = parse_row(row, ingestion_ts_ms)

            if record is None:
                total_skipped += 1
                continue

            # Rate limiting
            rate_limiter.wait()

            # Use nameOrig as the Kafka message key for consistent partition routing
            key = record["nameOrig"].encode("utf-8")

            try:
                serialized_value = avro_serializer(
                    record,
                    SerializationContext(KAFKA_TOPIC, MessageField.VALUE),
                )
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=key,
                    value=serialized_value,
                    callback=_delivery_callback,
                )
                total_sent += 1

                # Poll to trigger delivery callbacks without blocking
                producer.poll(0)

                if total_sent % 10_000 == 0:
                    elapsed = time.monotonic() - start_time
                    throughput = total_sent / elapsed if elapsed > 0 else 0
                    logger.info(
                        "Progress: sent=%d skipped=%d errors=%d throughput=%.0f msg/s",
                        total_sent, total_skipped, total_errors, throughput,
                    )

            except Exception as exc:  # noqa: BLE001
                total_errors += 1
                logger.error("Produce error: %s — row=%s", exc, row)

    finally:
        csv_file.close()
        logger.info("Flushing remaining messages…")
        producer.flush(timeout=30)

        elapsed = time.monotonic() - start_time
        throughput = total_sent / elapsed if elapsed > 0 else 0
        logger.info(
            "Producer finished | sent=%d skipped=%d errors=%d elapsed=%.1fs throughput=%.0f msg/s",
            total_sent, total_skipped, total_errors, elapsed, throughput,
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if not Path(CSV_PATH).is_file():
        logger.critical("CSV file not found: %s", CSV_PATH)
        sys.exit(1)
    run_producer()
