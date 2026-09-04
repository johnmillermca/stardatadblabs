FROM 192.168.1.50:30500/python:3.12-slim

WORKDIR /app

# PyMySQL is the only runtime dependency for the daemon itself.
# pyspark is NOT installed here — spark_iceberg_write.py is submitted
# to the Spark cluster via the REST API and runs on Spark workers,
# not inside this container.
RUN pip install --no-cache-dir pymysql==1.1.1

COPY doris_cache_manager.py /app/doris_cache_manager.py
# Write-pushdown Spark job — uploaded to the Spark cluster at submission time.
# The Spark worker fetches this file from the path specified in appResource.
# Bake it here so the daemon can reference /app/spark_iceberg_write.py as
# the canonical local path when building the REST submission payload.
COPY spark_iceberg_write.py /app/spark_iceberg_write.py

CMD ["python", "/app/doris_cache_manager.py"]
