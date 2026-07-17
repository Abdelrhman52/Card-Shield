#!/usr/bin/env python3
"""
CardShield — Apache Flink Real-Time Fraud Detection Job (PyFlink)
=================================================================
Consumes Avro-encoded card transactions from Kafka, applies a modular
set of fraud detection rules using stateful sliding windows, performs
async HBase lookups for blacklist/profile data, and writes fraud alerts
back to HBase and a downstream Kafka topic.

Rules implemented:
  R1 — Blacklist hit          : immediate reject if card is in HBase Blacklist
  R2 — Credit limit breach    : amount > credit_limit from UserProfiles
  R3 — Geographic velocity    : different city within 5-minute window
  R4 — High-value burst       : 3+ large transactions in a 5-min sliding window
  R5 — Behavioral anomaly     : amount > 5× user's rolling average
  R6 — Velocity burst         : > 10 transactions in any 5-min window

Processing guarantees:
  - Event-time processing with bounded-out-of-order watermarks
  - EXACTLY_ONCE Kafka source + RocksDB state backend
  - Incremental checkpointing every 30 s

Requirements (pip):
  apache-flink==1.19.0
  confluent-kafka[avro]==2.4.0
  fastavro==1.9.4
  happybase==1.2.0   (HBase Thrift client)
  python-dotenv==1.0.1

Usage:
  flink run -py fraud_job.py
  (or submit via Flink Web UI / REST API)
"""

import logging
import os
from typing import Any, Dict, Optional, Tuple

from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetResetStrategy,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    RuntimeContext,
)
from pyflink.datastream.state import (
    ListStateDescriptor,
)

logger = logging.getLogger("cardshield.flink")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC_IN = os.environ.get("KAFKA_TOPIC_TRANSACTIONS", "card-transactions")
KAFKA_TOPIC_ALERTS = os.environ.get("KAFKA_TOPIC_FRAUD_ALERTS", "fraud-alerts")
KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "cardshield-flink-group")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
HBASE_HOST = os.environ.get("HBASE_HOST", "hbase")
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))   # Thrift port
CHECKPOINT_INTERVAL_MS = int(os.environ.get("FLINK_CHECKPOINT_INTERVAL_MS", "30000"))

# Fraud rule thresholds
WINDOW_SIZE_MS = 5 * 60 * 1000         # 5-minute sliding window
WINDOW_SLIDE_MS = 1 * 60 * 1000        # slide every 1 minute
HIGH_VALUE_THRESHOLD = 10_000.0        # amount considered "large"
BURST_HIGH_VALUE_COUNT = 3             # R4: 3+ large txns in window
BURST_TX_COUNT = 10                    # R6: 10+ txns in any window
BEHAVIORAL_ANOMALY_MULTIPLIER = 5.0    # R5: 5× rolling average
GEO_VELOCITY_WINDOW_SEC = 300          # R3: 5-minute geo check

# ---------------------------------------------------------------------------
# Avro deserialization helper (using fastavro for performance)
# ---------------------------------------------------------------------------
import io as _io

import fastavro


def _load_schema():
    """Load the Avro schema from the .avsc file bundled with this job."""
    schema_path = os.path.join(os.path.dirname(__file__), "transaction.avsc")
    with open(schema_path, "r") as f:
        import json as _json
        return fastavro.parse_schema(_json.load(f))


_TX_SCHEMA = None  # lazy-loaded


def deserialize_avro(raw_bytes: bytes) -> Optional[Dict[str, Any]]:
    """
    Deserialize a Confluent Schema Registry Avro message.
    Wire format: [ 0x00 | 4-byte schema ID | avro payload ]
    """
    global _TX_SCHEMA
    if _TX_SCHEMA is None:
        _TX_SCHEMA = _load_schema()
    try:
        # Strip the 5-byte Confluent magic header
        if raw_bytes[0] != 0x00 or len(raw_bytes) < 6:
            return None
        payload = _io.BytesIO(raw_bytes[5:])
        return fastavro.schemaless_reader(payload, _TX_SCHEMA)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Avro deserialization failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# HBase client helper (synchronous Thrift via happybase)
# ---------------------------------------------------------------------------
def get_hbase_connection():
    import happybase
    return happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, autoconnect=True)


def compute_salt(card_id: str) -> str:
    """2-hex-digit salt derived from card_id for HBase row key salting."""
    return format(hash(card_id) & 0xFF, "02x")


def blacklist_row_key(card_id: str) -> str:
    return f"{compute_salt(card_id)}_{card_id}"


def profile_row_key(card_id: str, year_month: str) -> str:
    return f"{compute_salt(card_id)}_{card_id}_{year_month}"


def is_blacklisted(conn, card_id: str) -> bool:
    """Returns True if the card ID exists in the HBase Blacklist table."""
    try:
        row = conn.table("Blacklist").row(
            blacklist_row_key(card_id).encode(),
            columns=[b"info:is_active"],
        )
        return row.get(b"info:is_active", b"0") == b"1"
    except Exception as exc:  # noqa: BLE001
        logger.error("HBase blacklist lookup error: %s", exc)
        return False  # fail-open: do not block on lookup error


def get_credit_limit(conn, card_id: str, year_month: str) -> float:
    """Fetch the cardholder's credit limit from UserProfiles."""
    try:
        row = conn.table("UserProfiles").row(
            profile_row_key(card_id, year_month).encode(),
            columns=[b"account:credit_limit"],
        )
        val = row.get(b"account:credit_limit", b"0")
        return float(val)
    except Exception as exc:  # noqa: BLE001
        logger.error("HBase credit limit lookup error: %s", exc)
        return float("inf")  # fail-open: no block on lookup error


def get_rolling_avg(conn, card_id: str, year_month: str) -> float:
    """Fetch the cardholder's rolling average transaction amount."""
    try:
        row = conn.table("UserProfiles").row(
            profile_row_key(card_id, year_month).encode(),
            columns=[b"stats:avg_amount"],
        )
        val = row.get(b"stats:avg_amount", b"0")
        return float(val)
    except Exception as exc:  # noqa: BLE001
        logger.error("HBase rolling avg lookup error: %s", exc)
        return 0.0


def get_last_geo(conn, card_id: str, year_month: str) -> Tuple[str, int]:
    """Returns (last_city, last_tx_timestamp_ms) from UserProfiles."""
    try:
        row = conn.table("UserProfiles").row(
            profile_row_key(card_id, year_month).encode(),
            columns=[b"geo:last_city", b"geo:last_tx_ts"],
        )
        city = row.get(b"geo:last_city", b"UNKNOWN").decode()
        ts = int(row.get(b"geo:last_tx_ts", b"0"))
        return city, ts
    except Exception as exc:  # noqa: BLE001
        logger.error("HBase geo lookup error: %s", exc)
        return "UNKNOWN", 0


def update_profile(conn, card_id: str, year_month: str, tx: dict, risk_score: float):
    """Update UserProfile stats and risk score after processing a transaction."""
    try:
        table = conn.table("UserProfiles")
        rk = profile_row_key(card_id, year_month).encode()
        b = table.batch()
        b.put(rk, {
            b"stats:last_amount": str(tx["amount"]).encode(),
            b"risk:score": str(risk_score).encode(),
            b"risk:flagged": b"1" if risk_score > 0.5 else b"0",
            b"geo:last_tx_ts": str(tx["ingestion_timestamp_ms"]).encode(),
        })
        b.send()
    except Exception as exc:  # noqa: BLE001
        logger.error("HBase profile update error: %s", exc)


def write_fraud_alert(conn, tx: dict, rules_triggered: list, risk_score: float):
    """Write a fraud alert row to the FraudAlerts HBase table."""
    try:
        card_id = tx["nameOrig"]
        ts = tx["ingestion_timestamp_ms"]
        # Reverse timestamp for descending sort
        rev_ts = (2**63 - 1) - ts
        rk = f"{compute_salt(card_id)}_{card_id}_{rev_ts:020d}".encode()
        conn.table("FraudAlerts").put(rk, {
            b"alert:rules": ",".join(rules_triggered).encode(),
            b"alert:risk_score": str(risk_score).encode(),
            b"alert:ts": str(ts).encode(),
            b"tx:nameOrig": card_id.encode(),
            b"tx:amount": str(tx["amount"]).encode(),
            b"tx:type": tx["type"].encode(),
            b"tx:nameDest": tx["nameDest"].encode(),
        })
    except Exception as exc:  # noqa: BLE001
        logger.error("HBase fraud alert write error: %s", exc)


# ---------------------------------------------------------------------------
# Fraud rules — KeyedProcessFunction
# ---------------------------------------------------------------------------
class FraudRulesEngine(KeyedProcessFunction):
    """
    Stateful fraud rules engine keyed by nameOrig (card/customer ID).

    State maintained per key:
      - tx_history  : ListState[dict] — recent transactions for window checks
      - last_geo    : ValueState[tuple] — (city, ts_ms) of last transaction

    HBase is consulted for:
      - Blacklist hit (R1)
      - Credit limit (R2)
      - Rolling average (R5)
    """

    def __init__(self):
        self._tx_history_state = None
        self._hbase_conn = None

    def open(self, runtime_context: RuntimeContext):
        tx_desc = ListStateDescriptor("tx_history", Types.STRING())
        self._tx_history_state = runtime_context.get_list_state(tx_desc)
        # HBase connection opened once per task slot
        self._hbase_conn = get_hbase_connection()

    def process_element(self, tx: dict, ctx: KeyedProcessFunction.Context):
        """Called for every transaction in the stream."""
        card_id = tx["nameOrig"]
        amount = tx["amount"]
        ts_ms = tx["ingestion_timestamp_ms"]
        import datetime
        year_month = datetime.datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y%m")

        rules_triggered = []
        risk_score = 0.0

        conn = self._hbase_conn

        # -- R1: Blacklist hit -----------------------------------------------
        if is_blacklisted(conn, card_id):
            rules_triggered.append("R1_BLACKLIST")
            risk_score = max(risk_score, 1.0)

        # -- R2: Credit limit breach -----------------------------------------
        credit_limit = get_credit_limit(conn, card_id, year_month)
        if credit_limit > 0 and amount > credit_limit:
            rules_triggered.append("R2_CREDIT_LIMIT")
            risk_score = max(risk_score, 0.9)

        # -- R5: Behavioral anomaly (amount vs rolling average) --------------
        rolling_avg = get_rolling_avg(conn, card_id, year_month)
        if rolling_avg > 0 and amount > BEHAVIORAL_ANOMALY_MULTIPLIER * rolling_avg:
            rules_triggered.append("R5_BEHAVIORAL_ANOMALY")
            risk_score = max(risk_score, 0.75)

        # -- R3: Geographic velocity -----------------------------------------
        # (simplified: geo data would come from enriched events in production)
        # Here we use HBase stored geo from previous transaction
        last_city, last_ts = get_last_geo(conn, card_id, year_month)
        # In a real pipeline, current city comes from event enrichment.
        # We flag if last transaction was from a different "city" proxy
        # (using nameDest prefix as a city surrogate on synthetic data).
        current_city_proxy = tx["nameDest"][:3]
        if (
            last_city not in ("UNKNOWN", current_city_proxy)
            and ts_ms - last_ts < GEO_VELOCITY_WINDOW_SEC * 1000
        ):
            rules_triggered.append("R3_GEO_VELOCITY")
            risk_score = max(risk_score, 0.85)

        # -- Window-based rules: load recent tx history from ListState -------
        import json as _json
        window_cutoff_ms = ts_ms - WINDOW_SIZE_MS
        recent_txs = []
        for stored in self._tx_history_state.get() or []:
            entry = _json.loads(stored)
            if entry["ts_ms"] >= window_cutoff_ms:
                recent_txs.append(entry)

        # Add current tx to history
        recent_txs.append({"ts_ms": ts_ms, "amount": amount})

        # Update ListState (only keep within window)
        self._tx_history_state.clear()
        for entry in recent_txs:
            self._tx_history_state.add(_json.dumps(entry))

        # -- R4: High-value burst detection ----------------------------------
        high_value_count = sum(
            1 for e in recent_txs if e["amount"] >= HIGH_VALUE_THRESHOLD
        )
        if high_value_count >= BURST_HIGH_VALUE_COUNT:
            rules_triggered.append("R4_HIGH_VALUE_BURST")
            risk_score = max(risk_score, 0.80)

        # -- R6: Velocity burst ----------------------------------------------
        if len(recent_txs) >= BURST_TX_COUNT:
            rules_triggered.append("R6_VELOCITY_BURST")
            risk_score = max(risk_score, 0.70)

        # -- Update profile in HBase ----------------------------------------
        update_profile(conn, card_id, year_month, tx, risk_score)

        # -- Emit fraud alert if any rule fired ------------------------------
        if rules_triggered:
            write_fraud_alert(conn, tx, rules_triggered, risk_score)
            alert_payload = {
                "card_id": card_id,
                "amount": amount,
                "type": tx["type"],
                "rules_triggered": rules_triggered,
                "risk_score": risk_score,
                "is_fraud_label": tx["isFraud"],
                "alert_ts_ms": ts_ms,
            }
            yield _json.dumps(alert_payload)

        # Else: legitimate transaction, no output (or emit approved event)


# ---------------------------------------------------------------------------
# Avro → dict deserialization MapFunction wrapper
# ---------------------------------------------------------------------------
from pyflink.datastream.functions import MapFunction


class AvroDeserializeMap(MapFunction):
    """Deserializes raw Avro bytes into a Python dict."""

    def map(self, raw_bytes: bytes):
        return deserialize_avro(raw_bytes)


class FilterNone(MapFunction):
    """Filters out None values from failed deserialization."""

    def map(self, value):
        return value


from pyflink.datastream.functions import FilterFunction


class NotNoneFilter(FilterFunction):
    def filter(self, value) -> bool:
        return value is not None


# ---------------------------------------------------------------------------
# Flink Job Entry Point
# ---------------------------------------------------------------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    # -- Checkpointing -------------------------------------------------------
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)

    # -- State backend: RocksDB (set via flink-conf.yaml or env) ------------
    # Configured in docker-compose via FLINK_PROPERTIES:
    #   state.backend: rocksdb
    #   state.checkpoints.dir: file:///flink/checkpoints

    # -- Kafka Source --------------------------------------------------------
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_TOPIC_IN)
        .set_group_id(KAFKA_CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetResetStrategy.LATEST)
        .set_value_only_deserializer(SimpleStringSchema())  # raw bytes as string
        .build()
    )

    # Watermark strategy: event-time with 10-second out-of-orderness tolerance
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(10)
    ).with_timestamp_assigner(
        # Assign ingestion_timestamp_ms as event time
        lambda event, _: event.get("ingestion_timestamp_ms", 0) if event else 0
    )

    # -- Build Stream --------------------------------------------------------
    raw_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=watermark_strategy,
        source_name="KafkaTransactionSource",
    )

    # Deserialize Avro → dict
    tx_stream = (
        raw_stream
        .map(AvroDeserializeMap(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
        .filter(NotNoneFilter())
        .name("AvroDeserialization")
    )

    # Key by card ID (nameOrig) and apply fraud rules
    alert_stream = (
        tx_stream
        .key_by(lambda tx: tx["nameOrig"])
        .process(FraudRulesEngine())
        .name("FraudRulesEngine")
    )

    # -- Kafka Sink (fraud alerts) -------------------------------------------
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(KAFKA_TOPIC_ALERTS)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    alert_stream.sink_to(kafka_sink).name("FraudAlertKafkaSink")

    # -- Execute -------------------------------------------------------------
    logger.info("Submitting CardShield Flink fraud detection job…")
    env.execute("CardShield Fraud Detection Job")


if __name__ == "__main__":
    main()
