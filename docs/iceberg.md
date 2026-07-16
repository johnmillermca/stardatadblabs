# Apache Iceberg — k8s-platform

## Overview

Apache Iceberg is an open table format for huge analytic datasets. In this platform Iceberg tables are created and queried via **Apache Spark** (using the Spark Catalog API) against the **Apache Polaris** REST catalog.

| Field | Value |
|---|---|
| **Iceberg runtime JAR** | `iceberg-spark-runtime-3.5_2.12-1.9.2.jar` |
| **JAR version** | 1.9.2 (latest LTS stable) |
| **Spark version** | 3.5.1 |
| **Scala version** | 2.12 |
| **Gluten version** | 1.2.0 + Velox backend |
| **Catalog type** | REST (Apache Polaris) |
| **JAR SHA-256** | `c40ae0a8e2673bb5c01951b7d15aae2224a534f8331f4274ce2082776116a044` |
| **Maven coordinate** | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2` |
| **Downloaded to** | `jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar` |

---

## Version Compatibility Matrix

| Iceberg | Spark | Scala | Gluten 1.2.0 / Velox | Notes |
|---|---|---|---|---|
| **1.9.2** ✅ | 3.5.x | 2.12 | ✅ Fully compatible | **Used in this platform** |
| 1.10.x | 3.5.4+ | 2.12 | ⚠️ Requires Spark 3.5.4 | Gluten 1.2.0 not tested |
| 1.11.x | 3.5.5+ | 2.12 | ❌ API changes | Not compatible with Gluten 1.2.0 |
| 1.5.2 | 3.5.x | 2.12 | ✅ | Previous version (replaced) |

> **Why 1.9.2?**  
> Iceberg 1.9.2 is the latest release in the 1.9.x LTS series and is the highest version fully validated with Gluten 1.2.0's Velox-based physical plan rewriting. Versions 1.10+ introduced breaking changes in the `TableScan` API that Gluten 1.2.0 has not yet absorbed.

---

## Spark Configuration

The Iceberg runtime is pre-installed in the Spark-Gluten-Velox image at:
```
/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar
```

The following settings are baked into `spark-defaults.conf` inside the container:

```properties
# Iceberg SQL extensions
spark.sql.extensions  org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
                      io.delta.sql.DeltaSparkSessionExtension

# Polaris REST catalog
spark.sql.catalog.polaris      org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.polaris.type rest
spark.sql.catalog.polaris.uri  http://polaris-rest.catalog.svc.cluster.local:8181/api/catalog
```

---

## Creating an Iceberg Table

### PySpark example
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("iceberg-example") \
    .getOrCreate()

# Create an Iceberg namespace (database) via Polaris catalog
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.analytics")

# Create an Iceberg table
spark.sql("""
    CREATE TABLE IF NOT EXISTS polaris.analytics.sales (
        id         BIGINT,
        product    STRING,
        amount     DOUBLE,
        sale_date  DATE
    )
    USING iceberg
    PARTITIONED BY (sale_date)
""")

# Insert data
spark.sql("""
    INSERT INTO polaris.analytics.sales VALUES
        (1, 'Widget A', 99.99, DATE '2025-01-01'),
        (2, 'Widget B', 49.50, DATE '2025-01-02')
""")

# Query
spark.sql("SELECT * FROM polaris.analytics.sales").show()
```

### Spark SQL example
```sql
-- Create table
CREATE TABLE polaris.analytics.events (
    event_id   BIGINT,
    event_type STRING,
    payload    STRING,
    ts         TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ts));

-- Time-travel query (snapshot ID)
SELECT * FROM polaris.analytics.events VERSION AS OF 1234567890;

-- Time-travel query (timestamp)
SELECT * FROM polaris.analytics.events TIMESTAMP AS OF '2025-01-01 00:00:00';
```

---

## SparkSession with Iceberg (manual configuration)

If not using the bundled image, configure Iceberg manually:

```python
spark = SparkSession.builder \
    .appName("iceberg") \
    .config("spark.jars", "/path/to/iceberg-spark-runtime-3.5_2.12-1.9.2.jar") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.polaris",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.polaris.type", "rest") \
    .config("spark.sql.catalog.polaris.uri",
            "http://polaris-rest.catalog.svc.cluster.local:8181/api/catalog") \
    .getOrCreate()
```

---

## Downloading the JAR

The JAR is already downloaded to `jars/` in this repository:

```bash
# Already downloaded — just copy to Spark jars directory
cp jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar /opt/spark/jars/

# Or re-download from Maven Central
curl -fsSL \
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/iceberg-spark-runtime-3.5_2.12-1.9.2.jar" \
  -o jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar

# Verify checksum
sha256sum jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar
# Expected: c40ae0a8e2673bb5c01951b7d15aae2224a534f8331f4274ce2082776116a044
```

---

## Rebuilding the Spark Image

After any JAR update, rebuild the Docker image:

```bash
bash docker/spark-gluten-velox/build-and-push.sh
```

This pulls `apache/spark:3.5.1-scala2.12-java17-python3-ubuntu`, installs Gluten 1.2.0 + Velox, Iceberg 1.9.2, and Delta Lake 3.2.0, then pushes to the internal registry at `192.168.1.50:30500/spark-gluten-velox:3.5.1`.

---

## Iceberg Metadata Operations

```sql
-- List all snapshots
SELECT * FROM polaris.analytics.events.snapshots;

-- List data files
SELECT * FROM polaris.analytics.events.files;

-- List all partitions
SELECT * FROM polaris.analytics.events.partitions;

-- Expire old snapshots (keep last 7 days)
CALL polaris.system.expire_snapshots(
    table => 'analytics.events',
    older_than => TIMESTAMP '2025-01-01 00:00:00',
    retain_last => 5
);

-- Remove orphan files
CALL polaris.system.remove_orphan_files(table => 'analytics.events');

-- Rewrite small files
CALL polaris.system.rewrite_data_files(table => 'analytics.events');
```

---

## Related Components

| Component | Role |
|---|---|
| `spark-gluten-velox` | Spark executor with Iceberg JAR pre-loaded |
| `Apache Polaris` | REST Iceberg catalog (`catalog` namespace) |
| `JupyterHub` | Interactive PySpark notebook environment |
| `SQLMesh` | Transformation pipelines reading Iceberg tables |
