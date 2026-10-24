# =============================================================================
# ksqlDB MCP Server
# Exposes ksqlDB REST API operations (DESCRIBE EXTENDED, LIST STREAMS/TABLES,
# EXPLAIN, etc.) as MCP tools for AI agents.
# Base : python:3.11-slim
# Port : 3105
# =============================================================================
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "mcp>=1.0.0" \
    httpx \
    pydantic

WORKDIR /app
COPY server.py /app/server.py

EXPOSE 3105

CMD ["python", "/app/server.py"]
