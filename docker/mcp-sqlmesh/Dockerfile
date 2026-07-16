# =============================================================================
# SQLMesh MCP Server
# A Model Context Protocol server that exposes SQLMesh APIs as MCP tools,
# allowing LLMs/AI agents to run SQLMesh commands (plan, apply, audit, etc.)
# Base : python:3.11-slim
# Port : 3100 (MCP stdio-over-HTTP / SSE transport)
# =============================================================================
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "sqlmesh[spark]==0.99.0" \
    "mcp>=1.0.0" \
    psycopg2-binary \
    httpx \
    pydantic

WORKDIR /app

COPY server.py /app/server.py
COPY config_template.yaml /app/config_template.yaml

EXPOSE 3100

CMD ["python", "/app/server.py"]
