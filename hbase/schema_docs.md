# CardShield — HBase Schema Documentation

## Overview

HBase serves as the real-time fraud database in CardShield. Flink consults
it on every transaction for blacklist checks, credit limit lookups, and
behavioral profile reads. It also receives write-back updates from Flink
after each processed transaction.

---

## Row Key Design

### Salting strategy

All row keys in CardShield use a **2-hex-digit salt prefix** (`00`–`ff`)
derived from the card ID:

```
salt = format(hash(card_id) & 0xFF, "02x")
```

**Why salting is necessary:**

HBase stores rows in lexicographic order. Without salting, all rows for
cards starting with the same letter (e.g. `C1...`) would land in the same
region, creating a **write hotspot**. The salt distributes load across all
16 pre-split regions uniformly.

**Pre-split regions (16 splits):**

```
['10','20','30','40','50','60','70','80','90','a0','b0','c0','d0','e0','f0']
```

This gives 16 balanced regions from the start, avoiding costly region
splits under load.

---

## Tables

### 1. `Blacklist`

| Attribute         | Value                                         |
|-------------------|-----------------------------------------------|
| **Purpose**       | Store known-fraudulent and blocked card IDs   |
| **Row key format**| `{salt}_{cardId}`                             |
| **Example key**   | `a3_C1234567890`                              |
| **Access pattern**| Point lookup on every incoming transaction    |
| **Read/write**    | Read-heavy (every txn); write on new blocks   |

#### Column Families

| Family   | Columns                                    | TTL        | Versions | Notes                      |
|----------|--------------------------------------------|------------|----------|----------------------------|
| `info`   | `card_id`, `block_reason`, `is_active`     | Forever    | 1        | IN_MEMORY=true for hot cache|
| `audit`  | `blacklisted_at`, `source`, `analyst_id`   | 730 days   | 5        | Audit trail                |

#### Read pattern
```
Get 'Blacklist', 'a3_C1234567890', {COLUMN => 'info:is_active'}
```
Flink uses async I/O to pipeline multiple blacklist lookups concurrently,
avoiding blocking the main stream operator.

---

### 2. `UserProfiles`

| Attribute         | Value                                               |
|-------------------|-----------------------------------------------------|
| **Purpose**       | Per-card behavioral profiles for anomaly detection  |
| **Row key format**| `{salt}_{cardId}_{YYYYMM}`                          |
| **Example key**   | `5f_C9876543210_202607`                             |
| **Access pattern**| Point lookup + write-back per transaction           |
| **Read/write**    | Balanced read/write                                 |

The `YYYYMM` month bucket bounds row size. Flink always reads/writes the
current month bucket. Historical months can be used for long-term profiling.

#### Column Families

| Family    | Columns                                            | TTL        | Versions | Notes                       |
|-----------|----------------------------------------------------|------------|----------|-----------------------------|
| `account` | `card_id`, `credit_limit`, `currency`              | Forever    | 1        | Permanent account data       |
| `stats`   | `tx_count_5m`, `amount_sum_5m`, `avg_amount`       | 90 days    | 3        | Rolling window aggregates    |
| `geo`     | `last_country`, `last_city`, `last_tx_ts`          | 30 days    | 10       | Geographic velocity checks   |
| `risk`    | `score`, `flagged`, alert counts                   | 180 days   | 20       | Risk history                 |

---

### 3. `FraudAlerts`

| Attribute         | Value                                                           |
|-------------------|-----------------------------------------------------------------|
| **Purpose**       | Persist Flink-generated alerts for downstream consumers         |
| **Row key format**| `{salt}_{cardId}_{reversedTimestampMs}`                         |
| **Example key**   | `7b_C1234567890_09223372036854775807` (reversed for desc sort)  |
| **Access pattern**| Append-only writes; scan by card for history                    |
| **Read/write**    | Write-heavy                                                     |

Reversed timestamp (`Long.MAX_VALUE - ts_ms`) ensures the most recent
alerts appear first when scanning in lexicographic order.

#### Column Families

| Family  | Columns                                             | TTL        | Versions |
|---------|-----------------------------------------------------|------------|----------|
| `alert` | `rules`, `risk_score`, `ts`                         | 365 days   | 1        |
| `tx`    | `nameOrig`, `amount`, `type`, `nameDest`            | 365 days   | 1        |

---

## Performance Recommendations

| Concern                    | Recommendation                                              |
|----------------------------|-------------------------------------------------------------|
| Hotspotting                | 2-hex salt prefix + 16 pre-split regions at table creation  |
| Read latency               | Enable `IN_MEMORY=true` on `Blacklist:info` CF               |
| Compaction overhead        | Schedule major compactions during off-peak (2–4 AM)          |
| Region sizing              | Target 10–15 GB per region; auto-split at 20 GB              |
| Block cache                | Allocate 40% of HBase heap to block cache (hbase-site.xml)  |
| Write buffer               | Set `hbase.client.write.buffer = 8 MB` for batch writes      |

---

## Backup & Restore

- HBase tables are backed up daily by the Airflow `hbase_table_backup` task.
- Backup format: Avro container files written to HDFS under
  `/cardshield/hbase-backups/YYYY-MM-DD/<table>_backup.avro`.
- Restore: replay the Avro files using the `hbase_restore.py` script
  (see `hbase/hbase_restore.py`).
- Retention: backup files retained for 90 days in HDFS
  (enforced by HDFS trash TTL policy).

---

## Fields Stored in HBase vs Kafka/HDFS

| Field type                      | Location      | Reason                                    |
|---------------------------------|---------------|-------------------------------------------|
| Blacklist status, is_active     | **HBase**     | Sub-millisecond lookup per transaction    |
| Credit limit, account type      | **HBase**     | Static reference; fast random access      |
| Rolling stats, risk scores      | **HBase**     | Updated per transaction; Flink write-back |
| Raw transaction payloads        | **Kafka + HDFS** | Durability, replay, archival           |
| Historical Avro event log       | **HDFS**      | Compliance; not suited for random access  |
| Fraud alert records             | **HBase + MySQL** | HBase for live lookup; MySQL for BI   |
