# =============================================================================
# Doris MCP Server
# Exposes Apache Doris query, schema introspection, and admin operations
# as MCP tools for AI agents.
# Base : python:3.11-slim
# Port : 3101
# =============================================================================
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl default-mysql-client && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "mcp>=1.0.0" \
    mysql-connector-python \
    pymysql \
    httpx \
    pydantic

WORKDIR /app
COPY server.py /app/server.py

EXPOSE 3101

CMD ["python", "/app/server.py"]
