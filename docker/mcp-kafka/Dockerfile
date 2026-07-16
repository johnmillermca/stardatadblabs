# =============================================================================
# Kafka MCP Server
# Exposes Apache Kafka topic management, produce/consume, consumer group
# management, and cluster health as MCP tools for AI agents.
# Base : python:3.11-slim
# Port : 3104
# Supports both bitnami/kafka (SASL/PLAIN) and Strimzi (SCRAM-SHA-512)
# =============================================================================
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "mcp>=1.0.0" \
    "confluent-kafka>=2.5.0" \
    httpx \
    pydantic

WORKDIR /app
COPY server.py /app/server.py

EXPOSE 3104

CMD ["python", "/app/server.py"]
