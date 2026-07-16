# Oracle XE 21c

## Overview
Oracle Database XE 21c running on Kubernetes using the `gvenzl/oracle-xe:21-slim` image. Used as a CDC source for Debezium LogMiner connector.

| Property | Value |
|---|---|
| Namespace | `databases` |
| Node | `master.local` (pinned) |
| JDBC URL | `jdbc:oracle:thin:@192.168.1.50:30521/XEPDB1` |
| Image | `gvenzl/oracle-xe:21-slim` |
| Storage | 30 Gi PVC |
| Secret | `oracle-credentials` |
| Manifest | `manifests/oracle/oracle-deployment.yaml` |

> **Note:** `oracle-xe:19` has no public image. `gvenzl/oracle-xe:21-slim` is the closest free Oracle XE available and fully supports LogMiner / Debezium CDC.

## Prerequisites
```bash
sudo bash scripts/master/12-seed-openbao-secrets.sh
```

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-oracle.yaml`

## Manual Deploy
```bash
kubectl apply -f manifests/oracle/oracle-deployment.yaml
# Oracle XE takes 2–5 minutes to initialize
kubectl rollout status deployment/oracle-xe -n databases --timeout=600s
```

## Connect
```bash
# Wait for readiness (initialDelaySeconds: 120)
kubectl port-forward svc/oracle-xe -n databases 1521:1521 &
sqlplus system/<password>@//localhost:1521/XEPDB1
```

## Enable LogMiner for Debezium
```sql
-- Run as SYSDBA
ALTER SYSTEM SET db_recovery_file_dest_size = 5G;
ALTER SYSTEM SET db_recovery_file_dest = '/opt/oracle/oradata/recovery_area';
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER SYSTEM SWITCH LOGFILE;
-- Create Debezium user
CREATE USER c##dbzuser IDENTIFIED BY <password> CONTAINER=ALL;
GRANT CREATE SESSION, SET CONTAINER TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$DATABASE TO c##dbzuser CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION TO c##dbzuser CONTAINER=ALL;
GRANT LOGMINING TO c##dbzuser CONTAINER=ALL;
```

## Secrets
| Key | Description |
|---|---|
| `oracle-password` | SYS/SYSTEM password |

OpenBao path: `secret/data/oracle/credentials`
