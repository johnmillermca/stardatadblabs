# =============================================================================
# Spark/Gluten/Velox MCP Server
# Exposes Apache Spark job submission, status, and Gluten/Velox configuration
# as MCP tools for AI agents.
# Base : python:3.11-slim
# Port : 3103
# =============================================================================
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN pip install --no-cache-dir \
    "mcp>=1.0.0" \
    "pyspark==3.5.1" \
    httpx \
    pydantic

WORKDIR /app
COPY server.py /app/server.py

EXPOSE 3103

CMD ["python", "/app/server.py"]
