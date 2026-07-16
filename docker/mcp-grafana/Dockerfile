# =============================================================================
# Grafana MCP Server
# Image: 192.168.1.50:30500/mcp-grafana:1.0.0
# Port:  3201
# =============================================================================
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir aiohttp==3.9.5

COPY server.py .

EXPOSE 3201
CMD ["python", "server.py"]
