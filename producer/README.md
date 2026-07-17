# CardShield — Kafka Avro Producer

Reads `Fraud.csv` sequentially and publishes each row as an **Avro binary**
message to the `card-transactions` Kafka topic via Confluent Schema Registry.

## Rate Limiting

`PRODUCER_RATE_LIMIT` (msgs/second) uses a simple token-bucket approach:
each `produce()` call sleeps for `1/rate` seconds if needed.

- Default: `500` msg/s
- Set to `0` for unlimited throughput (stress test mode)
- For stress testing: set `PRODUCER_RATE_LIMIT=0`, `PRODUCER_BATCH_SIZE=500`, `PRODUCER_LINGER_MS=5`

## Verifying Delivery

```bash
# Tail the fraud-alerts topic (produced by Flink)
docker exec cardshield-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic card-transactions \
  --from-beginning \
  --max-messages 10

# Check Schema Registry for the registered schema
curl http://localhost:8081/subjects/card-transactions-value/versions/latest
```

## Test Plan

| Test                  | How to validate                                          |
|-----------------------|----------------------------------------------------------|
| Throughput            | Monitor producer log for `throughput=XXX msg/s` lines   |
| Malformed row         | Inject a row with missing columns → check `skipped` count |
| Connectivity          | Stop Kafka → producer logs delivery errors + retries     |
| Rate limit            | Set `PRODUCER_RATE_LIMIT=10` and confirm slow output      |
