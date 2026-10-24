# =============================================================================
# tableflow — Kafka-native CDC mirror to Iceberg
#
# Base  : python:3.11-slim
# Deps  : pyiceberg[s3] confluent-kafka pyarrow
#
# Built as:
#   192.168.1.50:30500/tableflow:1.0.4
# =============================================================================
FROM python:3.11-slim

WORKDIR /app

# System deps needed by confluent-kafka (librdkafka) and pyarrow
RUN apt-get update && apt-get install -y --no-install-recommends \
        librdkafka-dev \
        gcc \
        g++ \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application
COPY tableflow.py .

# Non-root user
RUN useradd -m -u 1000 tableflow
USER tableflow

ENTRYPOINT ["python3", "-u", "tableflow.py"]
