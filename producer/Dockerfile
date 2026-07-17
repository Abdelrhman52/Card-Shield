##############################################################################
# CardShield — Kafka Avro Producer Dockerfile
##############################################################################

FROM python:3.11-slim

LABEL maintainer="CardShield Team"
LABEL description="Reads Fraud.csv and publishes Avro-encoded events to Kafka"

# System deps needed by confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .
COPY transaction.avsc .

# Run as non-root for defense-in-depth
RUN useradd --create-home --shell /bin/bash appuser
USER appuser

# Mount your Fraud.csv at /data/Fraud.csv via a volume or bind mount:
#   docker run -v /path/to/Fraud.csv:/data/Fraud.csv cardshield-producer
VOLUME ["/data"]

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV SCHEMA_REGISTRY_URL=http://schema-registry:8081
ENV KAFKA_TOPIC_TRANSACTIONS=card-transactions
ENV PRODUCER_CSV_PATH=/data/Fraud.csv
ENV PRODUCER_RATE_LIMIT=500
ENV PRODUCER_BATCH_SIZE=100
ENV PRODUCER_LINGER_MS=10

CMD ["python", "producer.py"]
