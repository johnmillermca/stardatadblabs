# Apache Gluten + Velox (Spark Acceleration)

## Overview
Apache Gluten 1.2.0 with the Velox native execution backend provides columnar, vectorized query execution for Apache Spark, offloading SQL operators to native C++ code for 2–10x performance improvement on analytical queries.

| Component | Value |
|---|---|
| Gluten version | `1.2.0` |
| Velox backend | bundled in Gluten 1.2.0 |
| Spark version | `3.5.1` |
| Custom image | `192.168.1.50:30500/spark-gluten-velox:3.5.1` |
| Dockerfile | [`docker/spark-gluten-velox/Dockerfile`](../docker/spark-gluten-velox/Dockerfile) |
| Build script | [`docker/spark-gluten-velox/build-and-push.sh`](../docker/spark-gluten-velox/build-and-push.sh) |

## Build
```bash
bash docker/spark-gluten-velox/build-and-push.sh
# Builds on Ubuntu 22.04 base, compiles Velox from source (~60 min)
```

## Enable Per Session
```python
spark = SparkSession.builder \
    .config("spark.plugins", "io.glutenproject.GlutenPlugin") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .config("spark.gluten.sql.columnar.backend.lib", "velox") \
    .getOrCreate()
```

## Verify Gluten is Active
```python
# Run a query — look for "GlutenColumnarToRow" in the query plan
df = spark.range(1000000).selectExpr("id * 2 as val")
df.explain()
```

## Supported Operators
Velox accelerates: Filter, Project, Aggregate, Sort, HashJoin, BroadcastJoin, Exchange, TableScan (Parquet/ORC/Iceberg).

## Production Notes
- Requires `AVX-512` or `AVX2` CPU instruction set on worker nodes
- Not compatible with UDFs that use Java serialization
- Use `spark.gluten.sql.columnar.fallback.ignoreRowToColumnar=true` to gracefully fallback
