# =============================================================================
# Prometheus MCP Server
# Image: 192.168.1.50:30500/mcp-prometheus:1.0.0
# Port:  3200
# =============================================================================
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir aiohttp==3.9.5

COPY server.py .

EXPOSE 3200
CMD ["python", "server.py"]
