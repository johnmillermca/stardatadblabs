# JupyterHub

## Overview
JupyterHub 5.5.0 (chart 4.4.0) — multi-user Jupyter notebook server. Spawns per-user notebook pods using a custom image with PySpark and Gluten/Velox support.

| Property | Value |
|---|---|
| Chart | `jupyterhub/jupyterhub 4.4.0` |
| Namespace | `analytics` |
| UI URL | `http://192.168.1.50:30888` |
| Auth | DummyAuthenticator (lab) |
| Admin user | `admin` |
| Single-user image | `192.168.1.50:30500/jupyter-spark:latest` |
| Secret | `jupyterhub-credentials` |

## Build Single-User Image
```bash
# Dockerfile at docker/jupyter-spark/Dockerfile (create as needed)
docker build -t 192.168.1.50:30500/jupyter-spark:latest docker/jupyter-spark/
docker push 192.168.1.50:30500/jupyter-spark:latest
```

## Deployment (ArgoCD)
ArgoCD application: [`argocd-apps/app-jupyterhub.yaml`](../argocd-apps/app-jupyterhub.yaml)

## Login
1. Open `http://192.168.1.50:30888`
2. Username: `admin`, Password: from `jupyterhub-credentials` secret
```bash
kubectl get secret jupyterhub-credentials -n analytics \
  -o jsonpath='{.data.admin-password}' | base64 -d
```

## PySpark in Notebook
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("spark://spark-master-svc.analytics.svc.cluster.local:7077") \
    .appName("JupyterNotebook") \
    .getOrCreate()
df = spark.createDataFrame([(1, "hello"), (2, "world")], ["id", "msg"])
df.show()
```

## Secrets
| Key | Description |
|---|---|
| `admin-password` | JupyterHub admin password |
| `crypt-key` | Hub cookie encryption key |

OpenBao path: `secret/data/jupyterhub/credentials`
