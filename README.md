# CardShield — Real-Time Credit Card Fraud Detection

A production-oriented, distributed fraud detection pipeline built on
Apache Kafka, Apache Flink, Apache HBase, Hadoop HDFS, and Apache Airflow.
Serialization format: **Apache Avro** (via Confluent Schema Registry).
Input dataset: **Fraud.csv** (6.3 M synthetic credit card transactions).


---

## Architecture

```
Fraud.csv
    │
    ▼
[Docker Producer] ── Avro ──► [Kafka: card-transactions]
                                     │                   │
                               ┌─────▼─────┐    ┌────────▼────────┐
                               │ Flink Job │    │ HDFS Archiver   │
                               │ (rules)   │    │ (Avro → HDFS)   │
                               └─────┬─────┘    └────────┬────────┘
                                     │                   │
                          ┌──────────▼──────────┐        │
                          │  HBase              │        │
                          │  Blacklist          │        │
                          │  UserProfiles       │        │
                          │  FraudAlerts        │        │
                          └──────────┬──────────┘        │
                                     │                   │
                               [MySQL Staging] ◄─────────┘
                               (KPI / Power BI)
                                     │
                          ┌──────────▼──────────┐
                          │  Airflow DAG         │
                          │  (orchestration)     │
                          └─────────────────────┘
```

---

## Repository Structure

```
cardshield/
├── docker-compose.yml          # Full infrastructure stack
├── .env                        # Environment variable template 
├── README.md                   # This file
│
├── producer/                   # Kafka Avro transaction producer
│   ├── producer.py
│   ├── transaction.avsc        # Avro schema (source of truth)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── README.md
│
├── flink/                      # Flink fraud detection job
│   ├── fraud_job.py            # PyFlink job (all 6 fraud rules)
│   ├── transaction.avsc        # Avro schema (copy)
│   ├── flink-conf.yaml         # Flink configuration
│   └── requirements.txt
│
├── hdfs/                       # HDFS Avro archiving consumer
│   ├── hdfs_archiver.py
│   ├── transaction.avsc        # Avro schema (copy)
│   ├── Dockerfile
│   └── requirements.txt
│
├── hbase/                      # HBase DDL + documentation
│   ├── create_tables.hbase     # HBase shell script
│   └── schema_docs.md          # Row key design, column families, performance notes
│
├── airflow/                    # Airflow DAG
│   ├── requirements.txt
│   └── dags/
│       ├── cardshield_dag.py   # Main DAG: 3 tasks, daily 12:30 AM
│       └── helpers/
│           ├── kafka_health.py
│           ├── flink_manager.py
│           └── hbase_backup.py
│
└── mysql/                      # MySQL staging schema + KPI queries
    ├── airflow_db_init.sql     # Creates Airflow metadata DB (auto-run by Docker)
    ├── schema.sql              # Fact/dim tables, views
    └── kpi_queries.sql         # 12 Power BI-ready KPI queries
```

---

## Fraud.csv - Credit Card Transaction Data

This dataset contains **6,362,620 synthetic credit card transactions** collected over **743 hours (approximately 30 days)**. It is designed for **binary classification** tasks to detect fraudulent credit card transactions.

### Column Summary

| Column | Type | Description |
|--------|------|-------------|
| `step` | Integer | Hour of the transaction (1–743) |
| `type` | Categorical | Transaction type (`PAYMENT`, `TRANSFER`, `CASH_OUT`, `DEBIT`, `CASH_IN`) |
| `amount` | Float | Transaction amount in local currency |
| `nameOrig` | String | Customer ID initiating the transaction |
| `oldbalanceOrg` | Float | Origin account balance before the transaction |
| `newbalanceOrig` | Float | Origin account balance after the transaction |
| `nameDest` | String | Recipient customer ID |
| `oldbalanceDest` | Float | Recipient account balance before the transaction |
| `newbalanceDest` | Float | Recipient account balance after the transaction |
| `isFraud` | Binary | Target variable (`1` = Fraud, `0` = Legitimate) |
| `isFlaggedFraud` | Binary | System flag for suspicious high-value transfers (> 200,000) |

### Dataset Characteristics

- **Format:** CSV
- **Number of Columns:** 11
- **Number of Rows:** 6,362,620
- **File Size:** 493.53 MB
- **Fraud Cases:** 8,213 (0.13%)
- **Legitimate Cases:** 6,354,407 (99.87%)
- **Missing Values:** None
- **Data Type:** Fully synthetic (research and educational purposes)

### Dataset Source

This project uses the **Credit Card Fraud Dataset** available on Kaggle.

🔗 **Dataset:**  
https://www.kaggle.com/datasets/dylanmoraes/credit-card-fraud-dataset

Or click here:

**[Credit Card Fraud Dataset (Kaggle)](https://www.kaggle.com/datasets/dylanmoraes/credit-card-fraud-dataset)**

---

## Quick Start

### Prerequisites

- Docker ≥ 24 and Docker Compose ≥ 2.24
- Minimum: **8 CPU cores, 16 GB RAM** for the full stack
- `Fraud.csv` downloaded from Kaggle (place it anywhere; mount path configured in `.env`)

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env — set passwords, Fernet key, Slack webhook, CSV path
```

Generate the Airflow Fernet key:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 2. Start the stack

```bash
docker-compose up -d
```

Services start in dependency order. Allow ~2–3 minutes for HBase and
Hadoop to fully initialize before running the producer.

### 3. Create HBase tables

```bash
docker exec -it cardshield-hbase hbase shell /hbase/create_tables.hbase
```

To mount the script automatically, add this volume to the `hbase` service:
```yaml
volumes:
  - ./hbase/create_tables.hbase:/hbase/create_tables.hbase
```

### 4. Run the Kafka Avro producer

```bash
docker build -t cardshield-producer ./producer

docker run --rm \
  --network cardshield_cardshield-net \
  -v /path/to/Fraud.csv:/data/Fraud.csv \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  -e PRODUCER_RATE_LIMIT=500 \
  cardshield-producer
```

### 5. Run the HDFS archiver

```bash
docker build -t cardshield-hdfs ./hdfs

docker run --rm \
  --network cardshield_cardshield-net \
  cardshield-hdfs
```

### 6. Submit the Flink job

```bash
# Install PyFlink in the Flink container and submit
docker exec cardshield-flink-jm bash -c "
  pip install apache-flink==1.19.0 fastavro==1.9.4 confluent-kafka[avro]==2.4.0 happybase==1.2.0 &&
  flink run -py /opt/flink/usrlib/fraud_job.py"
```

Or copy the job into the container and submit via the Flink Web UI at
`http://localhost:8082`.

### 7. Access UIs

| Service             | URL                          |
|---------------------|------------------------------|
| Airflow             | http://localhost:8083        |
| Flink Web UI        | http://localhost:8082        |
| HDFS NameNode UI    | http://localhost:9870        |
| HBase Master UI     | http://localhost:16010       |
| Schema Registry     | http://localhost:8081        |
| MySQL               | localhost:3306               |

---

## Stopping the Stack

```bash
docker-compose down           # Stop containers, keep volumes
docker-compose down -v        # Stop containers + delete all volumes (DESTRUCTIVE)
```

---

## Inspecting Logs

```bash
docker-compose logs -f kafka
docker-compose logs -f flink-jobmanager
docker-compose logs -f airflow-scheduler
docker-compose logs -f cardshield-hbase
```

---

## Validating Health

### Kafka
```bash
# List topics
docker exec cardshield-kafka kafka-topics \
  --bootstrap-server localhost:9092 --list

# Consume a few messages
docker exec cardshield-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic card-transactions \
  --from-beginning \
  --max-messages 5
```

### HDFS
```bash
# List archived Avro files
curl -s "http://localhost:9870/webhdfs/v1/cardshield/transactions?op=LISTSTATUS" | python -m json.tool
```

### HBase
```bash
docker exec -it cardshield-hbase hbase shell
# Inside shell:
# list
# count 'Blacklist'
# count 'UserProfiles'
```

### Flink
```bash
curl -s http://localhost:8082/jobs | python -m json.tool
```

### Airflow
```bash
curl -s -u admin:changeme_admin http://localhost:8083/api/v1/dags \
  | python -m json.tool
```

---

## Airflow DAG: `fraud_protection_pipeline`

| Property        | Value                          |
|-----------------|-------------------------------|
| Schedule        | Daily at 12:30 AM UTC          |
| Tasks           | 3 (sequential)                 |
| Retries         | 4 per task, 5-min delay        |
| Alerting        | Slack webhook on failure       |
| Safe Mode       | Auto-activated if Flink fails  |

**Task order:**
```
test_card_gateway_stream → run_rules_guard_job → hbase_table_backup
```

---

## Fraud Detection Rules

| Rule | Name                   | Threshold                       | Risk Score |
|------|------------------------|---------------------------------|------------|
| R1   | Blacklist Hit          | HBase lookup                    | 1.00       |
| R2   | Credit Limit Breach    | amount > credit_limit           | 0.90       |
| R3   | Geographic Velocity    | Different city within 5 min     | 0.85       |
| R4   | High-Value Burst       | 3+ txns ≥ 10,000 in 5 min       | 0.80       |
| R5   | Behavioral Anomaly     | amount > 5× rolling average     | 0.75       |
| R6   | Velocity Burst         | 10+ txns in any 5-min window    | 0.70       |

---

## Avro Schema

Location: `producer/transaction.avsc` (canonical copy — shared across all services)

Fields derived from `Fraud.csv`:
`step`, `type` (enum), `amount`, `nameOrig`, `oldbalanceOrg`, `newbalanceOrig`,
`nameDest`, `oldbalanceDest`, `newbalanceDest`, `isFraud`, `isFlaggedFraud`,
`ingestion_timestamp_ms`

---

## Common Failure Points

| Symptom                            | Fix                                                        |
|------------------------------------|------------------------------------------------------------|
| Schema Registry not ready          | Wait 30 s after Kafka starts; check port 8081              |
| HBase shell script fails           | Ensure HBase is fully started (check port 16010 Web UI)    |
| Flink job won't connect to HBase   | Check `HBASE_HOST` env var; HBase Thrift port is 9090      |
| Producer CSV not found             | Check volume mount path in docker run / docker-compose      |
| Airflow DB init fails              | Check MySQL is healthy before airflow-init runs             |
| HDFS WebHDFS 307 redirect fails    | Ensure DataNode is running and reachable from client         |

---

## Security Notes

- **Never expose** ZooKeeper (2181), HBase Thrift (9090), or internal Kafka (29092) outside the Docker network.
- Rotate all `.env` passwords before deploying to any shared environment.
- The MySQL root password, Airflow Fernet key, and Slack webhook URL must be treated as secrets.
- For production, enable Kafka SSL/SASL and HBase Kerberos authentication.

