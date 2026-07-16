# =============================================================================
# OpenSearch MCP Server
# Exposes OpenSearch index management, search, and cluster operations as MCP tools.
# Base : python:3.11-slim
# Port : 3102
# =============================================================================
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "mcp>=1.0.0" \
    "opensearch-py>=2.6.0" \
    httpx \
    pydantic

WORKDIR /app
COPY server.py /app/server.py

EXPOSE 3102

CMD ["python", "/app/server.py"]
