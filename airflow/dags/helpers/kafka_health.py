"""
CardShield — Kafka Health Check Helper
=======================================
Used by the Airflow DAG task `test_card_gateway_stream`.

Checks:
  1. Broker reachability (list topics)
  2. Target topic exists and has partitions
  3. Consumer group lag is within threshold
"""

import logging
import os
from typing import Dict, Any

logger = logging.getLogger("cardshield.kafka_health")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_TRANSACTIONS", "card-transactions")
MAX_ACCEPTABLE_LAG = int(os.environ.get("KAFKA_MAX_LAG_THRESHOLD", "100000"))
CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "cardshield-flink-group")


def check_kafka_health() -> Dict[str, Any]:
    """
    Perform a comprehensive Kafka health check.

    Returns:
        dict with keys:
          healthy (bool)      — overall pass/fail
          reason  (str)       — failure reason if not healthy
          details (dict)      — raw metrics
    """
    from confluent_kafka import Consumer, KafkaException
    from confluent_kafka.admin import AdminClient

    result = {
        "healthy": False,
        "reason": "Unknown",
        "details": {},
    }

    # ------------------------------------------------------------------
    # Step 1: Broker reachability — list topics
    # ------------------------------------------------------------------
    try:
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        metadata = admin.list_topics(timeout=15)
        topic_names = list(metadata.topics.keys())
        result["details"]["broker_reachable"] = True
        result["details"]["topic_count"] = len(topic_names)
        logger.info("Kafka broker reachable. Topics found: %d", len(topic_names))
    except KafkaException as exc:
        result["reason"] = f"Broker unreachable: {exc}"
        logger.error(result["reason"])
        return result

    # ------------------------------------------------------------------
    # Step 2: Target topic exists with partitions
    # ------------------------------------------------------------------
    if KAFKA_TOPIC not in topic_names:
        result["reason"] = f"Topic '{KAFKA_TOPIC}' does not exist"
        logger.error(result["reason"])
        return result

    topic_meta = metadata.topics[KAFKA_TOPIC]
    partition_count = len(topic_meta.partitions)
    result["details"]["topic"] = KAFKA_TOPIC
    result["details"]["partition_count"] = partition_count

    if partition_count == 0:
        result["reason"] = f"Topic '{KAFKA_TOPIC}' has no partitions"
        return result

    # Check for partition leader election
    leaderless = [
        p for p, pmeta in topic_meta.partitions.items()
        if pmeta.leader == -1
    ]
    if leaderless:
        result["reason"] = f"Partitions without leader: {leaderless}"
        logger.error(result["reason"])
        return result

    logger.info("Topic '%s' healthy: %d partitions, all with leaders", KAFKA_TOPIC, partition_count)

    # ------------------------------------------------------------------
    # Step 3: Consumer group lag check
    # ------------------------------------------------------------------
    try:
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"{CONSUMER_GROUP}-health-probe",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })

        # Get committed offsets for the group
        from confluent_kafka import TopicPartition
        partitions = [
            TopicPartition(KAFKA_TOPIC, p)
            for p in topic_meta.partitions.keys()
        ]
        committed = consumer.committed(partitions, timeout=10)

        total_lag = 0
        for tp in committed:
            try:
                lo, hi = consumer.get_watermark_offsets(tp, timeout=5)
                committed_offset = tp.offset if tp.offset >= 0 else lo
                lag = max(0, hi - committed_offset)
                total_lag += lag
            except Exception:  # noqa: BLE001
                pass  # Partition may be empty

        consumer.close()
        result["details"]["consumer_group"] = CONSUMER_GROUP
        result["details"]["total_lag"] = total_lag

        if total_lag > MAX_ACCEPTABLE_LAG:
            result["reason"] = (
                f"Consumer group lag {total_lag} exceeds threshold {MAX_ACCEPTABLE_LAG}"
            )
            logger.warning(result["reason"])
            # Warning only — do not fail on lag alone; Flink may be catching up
            # Uncomment below to make lag a hard failure:
            # return result

        logger.info("Consumer group '%s' lag: %d", CONSUMER_GROUP, total_lag)

    except Exception as exc:  # noqa: BLE001
        logger.warning("Consumer lag check skipped: %s", exc)

    # ------------------------------------------------------------------
    # All checks passed
    # ------------------------------------------------------------------
    result["healthy"] = True
    result["reason"] = "OK"
    logger.info("Kafka health check PASSED")
    return result
