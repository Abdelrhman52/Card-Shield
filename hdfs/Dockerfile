##############################################################################
# CardShield — HDFS Avro Archiver Dockerfile
##############################################################################

FROM python:3.11-slim

LABEL maintainer="CardShield Team"
LABEL description="Consumes Avro events from Kafka and archives them to HDFS"

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY hdfs_archiver.py .
COPY transaction.avsc .

# Run as non-root for defense-in-depth
RUN useradd --create-home --shell /bin/bash appuser
USER appuser

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV KAFKA_TOPIC_TRANSACTIONS=card-transactions
ENV KAFKA_CONSUMER_GROUP_HDFS=cardshield-hdfs-archiver
ENV SCHEMA_REGISTRY_URL=http://schema-registry:8081
ENV HDFS_WEBHDFS_HOST=namenode
ENV HDFS_WEBHDFS_PORT=9870
ENV HDFS_ARCHIVE_BASE=/cardshield/transactions
ENV HDFS_WEBHDFS_USER=root
ENV HDFS_BATCH_SIZE=5000
ENV HDFS_FLUSH_INTERVAL_SEC=60

CMD ["python", "hdfs_archiver.py"]
