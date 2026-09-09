# =============================================================================
# Doris Write Proxy
# Transparent MySQL protocol proxy — intercepts DML against managed Iceberg
# catalogs and pushes them to Spark, returning OK/ERR to the MySQL client.
#
# Pure stdlib — no pip dependencies.  Python 3.11-slim base.
# =============================================================================
FROM python:3.11-slim

WORKDIR /app

# Copy proxy script and the Spark write-pushdown job (shared with cache manager)
COPY doris_write_proxy.py  /app/doris_write_proxy.py
COPY spark_iceberg_write.py /app/spark_iceberg_write.py

# No pip install needed — only stdlib (asyncio, json, re, struct, urllib)

EXPOSE 9040

CMD ["python", "/app/doris_write_proxy.py"]
