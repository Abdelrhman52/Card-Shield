FROM apache/airflow:2.9.2-python3.11

# librdkafka is required for confluent-kafka's C extension to build/install
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
