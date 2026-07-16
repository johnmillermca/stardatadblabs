# Apache Polaris

## Overview
Apache Polaris — open-source Iceberg REST catalog. Provides a standards-compliant Iceberg REST catalog API, enabling Spark, Flink, Trino, and other engines to discover and access Iceberg tables through a single endpoint.

| Property | Value |
|---|---|
| Namespace | `catalog` |
| REST API | `http://192.168.1.50:30181` |
| Internal | `http://polaris-rest.catalog.svc.cluster.local:8181` |
| Image | `192.168.1.50:30500/apache-polaris:latest` (must be built) |
| Depends on | PostgreSQL (`polaris` database) |
| Secret | `polaris-db-credentials` |
| Manifest | `manifests/polaris/polaris-deployment.yaml` |

## Prerequisites
1. PostgreSQL deployed and `polaris` database created
2. Build and push image:
```bash
git clone https://github.com/apache/polaris.git
cd polaris
./gradlew :polaris-quarkus-server:build -Dquarkus.package.type=uber-jar
docker build -t 192.168.1.50:30500/apache-polaris:latest .
docker push 192.168.1.50:30500/apache-polaris:latest
```
3. Seed secrets:
```bash
sudo bash scripts/master/12-seed-openbao-secrets.sh
```

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-polaris.yaml`

## Verify
```bash
curl http://192.168.1.50:30181/api/catalog/v1/config
```

## Create a Catalog
```bash
curl -X POST http://192.168.1.50:30181/api/catalog/v1/catalogs \
  -H "Content-Type: application/json" \
  -d '{"catalog": {"name": "main", "type": "INTERNAL", "properties": {"default-base-location": "s3://warehouse/"}}}'
```

## Spark Integration
```python
spark = SparkSession.builder \
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.polaris.type", "rest") \
    .config("spark.sql.catalog.polaris.uri", "http://polaris-rest.catalog.svc.cluster.local:8181/api/catalog") \
    .getOrCreate()
```

## Secrets
| Key | Description |
|---|---|
| `db-user` | PostgreSQL user (`polaris`) |
| `db-password` | PostgreSQL password |

OpenBao path: `secret/data/polaris/credentials`
