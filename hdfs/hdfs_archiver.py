#!/usr/bin/env python3
"""
CardShield — HDFS Avro Archiving Consumer
==========================================
Consumes Avro-encoded credit card transactions from the `card-transactions`
Kafka topic and archives them to HDFS in Avro container format, partitioned
by date and transaction hour.

HDFS path layout:
  /cardshield/transactions/
    year=YYYY/
      month=MM/
        day=DD/
          hour=HH/
            transactions_<partition>_<offset_start>.avro

Key design decisions:
  - Reads Avro binary from Kafka (Confluent wire format)
  - Writes standard Avro container files to HDFS via WebHDFS REST API
  - Batches records before writing (configurable batch size / flush interval)
  - Partition-aware: one output file per Kafka partition per flush cycle
  - Exactly-once-friendly: commits Kafka offsets AFTER successful HDFS write
  - On failure: logs error, increments retry counter, does NOT commit offset

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TRANSACTIONS, KAFKA_CONSUMER_GROUP,
  SCHEMA_REGISTRY_URL, HDFS_NAMENODE_URL, HDFS_ARCHIVE_BASE,
  HDFS_BATCH_SIZE, HDFS_FLUSH_INTERVAL_SEC, HDFS_WEBHDFS_PORT

Usage:
  python hdfs_archiver.py
"""

import io
import logging
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
import json

import fastavro
import requests
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("cardshield.hdfs_archiver")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_TRANSACTIONS", "card-transactions")
KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP_HDFS", "cardshield-hdfs-archiver")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE_URL", "hdfs://namenode:8020")
HDFS_WEBHDFS_HOST = os.environ.get("HDFS_WEBHDFS_HOST", "namenode")
HDFS_WEBHDFS_PORT = int(os.environ.get("HDFS_WEBHDFS_PORT", "9870"))
HDFS_ARCHIVE_BASE = os.environ.get("HDFS_ARCHIVE_BASE", "/cardshield/transactions")
HDFS_WEBHDFS_USER = os.environ.get("HDFS_WEBHDFS_USER", "root")

BATCH_SIZE = int(os.environ.get("HDFS_BATCH_SIZE", "5000"))
FLUSH_INTERVAL_SEC = int(os.environ.get("HDFS_FLUSH_INTERVAL_SEC", "60"))

# ---------------------------------------------------------------------------
# Avro schema (loaded from file)
# ---------------------------------------------------------------------------
AVRO_SCHEMA_PATH = str(Path(__file__).parent / "transaction.avsc")

def load_parsed_schema():
    with open(AVRO_SCHEMA_PATH, "r") as f:
        return fastavro.parse_schema(json.load(f))


# ---------------------------------------------------------------------------
# WebHDFS client (REST API — no Hadoop client libraries needed)
# ---------------------------------------------------------------------------
class WebHDFSClient:
    """Thin wrapper around the WebHDFS REST API for file creation."""

    def __init__(self, host: str, port: int, user: str = "root"):
        self.base = f"http://{host}:{port}/webhdfs/v1"
        self.user = user

    def _url(self, path: str) -> str:
        return f"{self.base}{path}?user.name={self.user}"

    def mkdirs(self, path: str) -> bool:
        """Create directory path recursively."""
        r = requests.put(
            self._url(path) + "&op=MKDIRS",
            allow_redirects=True,
            timeout=30,
        )
        return r.status_code == 200

    def create(self, path: str, data: bytes, overwrite: bool = False) -> bool:
        """
        Upload a file via WebHDFS two-step CREATE:
          1. PUT to NameNode → 307 redirect to DataNode
          2. PUT data to DataNode location
        """
        overwrite_str = "true" if overwrite else "false"
        url = self._url(path) + f"&op=CREATE&overwrite={overwrite_str}&replication=1"
        # Step 1: get redirect
        r1 = requests.put(url, allow_redirects=False, timeout=30)
        if r1.status_code != 307:
            logger.error("WebHDFS CREATE step 1 failed: %s %s", r1.status_code, r1.text)
            return False
        redirect_url = r1.headers["Location"]
        # Step 2: upload data
        r2 = requests.put(
            redirect_url,
            data=data,
            headers={"Content-Type": "application/octet-stream"},
            timeout=120,
        )
        if r2.status_code not in (200, 201):
            logger.error("WebHDFS CREATE step 2 failed: %s %s", r2.status_code, r2.text)
            return False
        return True

    def exists(self, path: str) -> bool:
        r = requests.get(self._url(path) + "&op=GETFILESTATUS", timeout=10)
        return r.status_code == 200


# ---------------------------------------------------------------------------
# Avro container writer helper
# ---------------------------------------------------------------------------
def serialize_avro_container(records: List[dict], schema) -> bytes:
    """Serialize a list of dicts into an Avro container file (bytes)."""
    buf = io.BytesIO()
    fastavro.writer(buf, schema, records, codec="snappy")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# HDFS path builder
# ---------------------------------------------------------------------------
def build_hdfs_path(base: str, ts: datetime, partition: int, offset_start: int) -> str:
    return (
        f"{base}"
        f"/year={ts.year:04d}"
        f"/month={ts.month:02d}"
        f"/day={ts.day:02d}"
        f"/hour={ts.hour:02d}"
        f"/transactions_p{partition:03d}_{offset_start}.avro"
    )


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
_shutdown = False

def _handle_signal(sig, _frame):
    global _shutdown
    logger.info("Shutdown signal received (%s). Draining buffer…", sig)
    _shutdown = True

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------
def run_archiver():
    logger.info("CardShield HDFS Avro Archiver starting…")
    schema = load_parsed_schema()
    webhdfs = WebHDFSClient(HDFS_WEBHDFS_HOST, HDFS_WEBHDFS_PORT, HDFS_WEBHDFS_USER)

    # Schema Registry deserializer
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(sr_client)

    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,   # manual commit after successful HDFS write
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 30000,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])

    # Buffer: keyed by Kafka partition
    buffers: Dict[int, List[dict]] = {}
    buffer_offsets: Dict[int, int] = {}   # partition → first offset in current buffer
    last_flush_time = time.monotonic()

    total_written = 0
    total_files = 0

    try:
        while not _shutdown:
            msg = consumer.poll(timeout=2.0)

            if msg is None:
                pass  # No message — check flush interval
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("End of partition %d offset %d", msg.partition(), msg.offset())
                else:
                    logger.error("Consumer error: %s", msg.error())
            else:
                partition = msg.partition()
                offset = msg.offset()

                # Deserialize Avro
                record = avro_deserializer(
                    msg.value(),
                    SerializationContext(KAFKA_TOPIC, MessageField.VALUE),
                )
                if record is None:
                    logger.warning("Null record at partition=%d offset=%d", partition, offset)
                    continue

                if partition not in buffers:
                    buffers[partition] = []
                    buffer_offsets[partition] = offset

                buffers[partition].append(record)

            # -- Flush condition: batch size OR time interval ----------------
            elapsed = time.monotonic() - last_flush_time
            should_flush = elapsed >= FLUSH_INTERVAL_SEC or any(
                len(b) >= BATCH_SIZE for b in buffers.values()
            )

            if should_flush and buffers:
                flush_time = datetime.now(timezone.utc)
                offsets_to_commit = []

                for part, records in list(buffers.items()):
                    if not records:
                        continue
                    offset_start = buffer_offsets[part]
                    hdfs_path = build_hdfs_path(
                        HDFS_ARCHIVE_BASE, flush_time, part, offset_start
                    )
                    dir_path = str(Path(hdfs_path).parent)

                    # Ensure directory exists
                    webhdfs.mkdirs(dir_path)

                    # Serialize to Avro container
                    avro_bytes = serialize_avro_container(records, schema)

                    # Write to HDFS
                    success = webhdfs.create(hdfs_path, avro_bytes)
                    if success:
                        total_written += len(records)
                        total_files += 1
                        logger.info(
                            "Flushed partition=%d records=%d → %s",
                            part, len(records), hdfs_path,
                        )
                        # Commit offset for this partition
                        last_msg_offset = offset_start + len(records) - 1
                        offsets_to_commit.append(
                            TopicPartition(KAFKA_TOPIC, part, last_msg_offset + 1)
                        )
                        del buffers[part]
                        del buffer_offsets[part]
                    else:
                        logger.error(
                            "HDFS write FAILED for partition=%d. Offsets NOT committed.",
                            part,
                        )

                if offsets_to_commit:
                    consumer.commit(offsets=offsets_to_commit, asynchronous=False)

                last_flush_time = time.monotonic()

    finally:
        # Final flush
        logger.info("Final flush before shutdown…")
        flush_time = datetime.now(timezone.utc)
        for part, records in buffers.items():
            if not records:
                continue
            offset_start = buffer_offsets[part]
            hdfs_path = build_hdfs_path(HDFS_ARCHIVE_BASE, flush_time, part, offset_start)
            webhdfs.mkdirs(str(Path(hdfs_path).parent))
            avro_bytes = serialize_avro_container(records, schema)
            if webhdfs.create(hdfs_path, avro_bytes):
                total_written += len(records)
                total_files += 1
                consumer.commit(
                    offsets=[TopicPartition(KAFKA_TOPIC, part, offset_start + len(records))],
                    asynchronous=False,
                )

        consumer.close()
        logger.info(
            "Archiver stopped | total_records=%d total_files=%d",
            total_written, total_files,
        )


if __name__ == "__main__":
    run_archiver()
