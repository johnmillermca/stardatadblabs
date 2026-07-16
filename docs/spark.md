# Apache Spark + Gluten + Velox

## Overview
Apache Spark 3.5.1 deployed via `bitnami/spark 10.0.3` using a custom image that bundles **Gluten 1.2.0** and the **Velox** native execution backend for columnar, vectorized query acceleration.

| Property | Value |
|---|---|
| Chart | `bitnami/spark 10.0.3` |
| Namespace | `analytics` |
| Master UI | `http://192.168.1.50:30707` |
| Master RPC | `spark://192.168.1.50:30777` |
| Image | `192.168.1.50:30500/spark-gluten-velox:3.5.1` |
| Workers | 3 replicas (2 CPU, 2 GB each) |

## Build Custom Image
```bash
bash docker/spark-gluten-velox/build-and-push.sh
```
The Dockerfile is at `docker/spark-gluten-velox/Dockerfile`.

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-spark.yaml`

## Manual Helm Deploy
```bash
helm upgrade --install spark bitnami/spark \
  --version 10.0.3 \
  --namespace analytics \
  -f helm/spark/values.yaml
```

## Submit a Job
```bash
# From inside cluster
kubectl run spark-submit --rm -it --restart=Never \
  --image=192.168.1.50:30500/spark-gluten-velox:3.5.1 \
  -- spark-submit \
    --master spark://spark-master-svc.analytics.svc.cluster.local:7077 \
    --class org.apache.spark.examples.SparkPi \
    /opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar 100
```

## Gluten / Velox Usage
Gluten/Velox is enabled per-query with:
```python
spark.conf.set("spark.plugins", "io.glutenproject.GlutenPlugin")
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "2g")
spark.conf.set("spark.gluten.sql.columnar.backend.lib", "velox")
```

## Iceberg Integration
```python
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.polaris.type", "rest") \
    .config("spark.sql.catalog.polaris.uri", "http://polaris-rest.catalog.svc.cluster.local:8181/api/catalog") \
    .getOrCreate()
```
