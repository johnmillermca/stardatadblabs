# =============================================================================
# Kestra MCP Server
# Exposes Kestra workflow orchestration operations as MCP tools for AI agents.
# Base : python:3.11-slim
# Port : 3105 (MCP SSE transport)
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
