# Oracle XE 21c

## Overview
Oracle Database XE 21c running on Kubernetes using the `gvenzl/oracle-xe:21-slim` image. Used as a CDC source for the Debezium LogMiner connector.

| Property | Value |
|---|---|
| Image | `gvenzl/oracle-xe:21-slim` |
| Namespace | `prod` |
| Node | `master.local` (pinned via nodeSelector) |
| JDBC URL | `jdbc:oracle:thin:@192.168.1.50:30521/XEPDB1` |
| Service (NodePort) | `192.168.1.50:30521` |
| Storage | `oracle-data` PVC — 200Gi on `local-path` |
| Secret | `oracle-credentials` (key: `oracle-password`) |
| Manifest | [`manifests/oracle/oracle-deployment.yaml`](../manifests/oracle/oracle-deployment.yaml) |

> **Note:** `oracle-xe:19` has no public image. `gvenzl/oracle-xe:21-slim` is the closest free Oracle XE available and fully supports LogMiner / Debezium CDC.

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-oracle.yaml` (sync-wave 0)

## Manual Deploy
```bash
kubectl apply -f manifests/oracle/oracle-deployment.yaml
# Oracle XE takes 3–5 minutes to initialize on first boot
kubectl rollout status deployment/oracle-xe -n prod --timeout=600s
```

## Connect
```bash
# Port-forward for local SQL access
kubectl port-forward svc/oracle-xe -n prod 1521:1521 &

# SQLPlus
sqlplus system/<password>@//localhost:1521/XEPDB1

# Or via NodePort
sqlplus system/<password>@//192.168.1.50:30521/XEPDB1
```

## Post-Reboot Behaviour — ORA-01081 Prevention

Oracle XE is configured with **two init containers** that run before the main container on every pod start. They clean all stale lock/SGA files that Oracle leaves behind when the pod is killed without a graceful shutdown (e.g. node reboot):

**Init container 1 — `fix-permissions`** (busybox): cleans PVC-resident lock files:
```
/opt/oracle/oradata/XE/lk*
/opt/oracle/oradata/XE/sgadef.dbf
/opt/oracle/oradata/XE/.oracle_ipc_lock
/opt/oracle/oradata/dbconfig/XE/lk*
/opt/oracle/oradata/dbconfig/XE/sgadef.dbf
```

**Init container 2 — `oracle-cleanup`** (oracle-xe image): cleans Oracle Home lock files:
```
$ORACLE_BASE_HOME/dbs/sgadef.dbf
$ORACLE_BASE_HOME/dbs/lk*
$ORACLE_BASE_HOME/dbs/hc_*.dat
/tmp/.oracle/s*
```

**`terminationGracePeriodSeconds: 120`** gives Oracle 2 minutes to checkpoint and shut down cleanly — this is the primary prevention that stops stale files from being created in the first place.

> If you see `ORA-01081: cannot start already-running ORACLE` — the init containers did not clean a lock file. Check which file was missed:
> ```bash
> kubectl logs -n prod -l app=oracle-xe -c fix-permissions
> kubectl logs -n prod -l app=oracle-xe -c oracle-cleanup
> ```
> See [Runbook 09 §10](runbooks/runbook-09-incident-postmortem.md#10-oracle-xe--crashloopbackoff-ora-01081-stale-lock-files) for full details.

## Enable LogMiner for Debezium
```sql
-- Connect as SYSDBA
-- Enable archive logging (required for LogMiner)
ALTER SYSTEM SET db_recovery_file_dest_size = 5G;
ALTER SYSTEM SET db_recovery_file_dest = '/opt/oracle/oradata/recovery_area';
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER SYSTEM SWITCH LOGFILE;

-- Create Debezium CDC user
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
| `oracle-password` | SYSTEM/SYS password |

OpenBao path: `secret/data/oracle/credentials`

## Troubleshooting

### CrashLoopBackOff — Exit Code 57 (ORA-01081)
```bash
kubectl logs -n prod -l app=oracle-xe --previous | grep "ORA-01081"
```
**Cause:** Stale Oracle lock files not cleaned by init containers.  
**Fix:** The init containers should handle this automatically. If still failing:
```bash
# Force a new pod (init containers will re-run)
kubectl rollout restart deployment/oracle-xe -n prod
# Watch init container logs
kubectl logs -n prod -l app=oracle-xe -c fix-permissions -f
kubectl logs -n prod -l app=oracle-xe -c oracle-cleanup -f
```

### Pod slow to become Ready
Oracle XE takes 3–5 minutes on first boot and 1–2 minutes on subsequent boots. The readiness probe waits 120 seconds before checking — this is intentional.

### Check Oracle is listening
```bash
kubectl exec -n prod -l app=oracle-xe -- \
  bash -c "echo 'SELECT 1 FROM DUAL;' | sqlplus -S system/\$ORACLE_PASSWORD@localhost:1521/XEPDB1"
```
