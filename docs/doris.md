# Apache Doris

## Overview
Apache Doris 4.0.7 — MPP analytical database for real-time analytics. Deployed with one FE (Frontend, StatefulSet) and one BE (Backend, Deployment) in the `prod` namespace.

| Property | Value |
|---|---|
| FE image | `192.168.1.50:30500/apache/doris:fe-4.0.7` |
| BE image | `192.168.1.50:30500/apache/doris:be-4.0.7` |
| Namespace | `prod` |
| FE Web UI | `http://192.168.1.50:30030` |
| FE MySQL protocol | `192.168.1.50:30090` (port 30090) |
| FE node | `worker1.local` (pinned via nodeSelector) |
| BE node | `worker1.local` (pinned via nodeSelector) |
| FE storage | `doris-fe-meta` PVC — 100Gi |
| BE storage | `doris-be-storage` PVC — 400Gi |
| Manifests | [`manifests/doris/`](../manifests/doris/) |

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-doris.yaml` (sync-wave 0)

## Manual Deploy
```bash
kubectl apply -f manifests/doris/doris-services.yaml
kubectl apply -f manifests/doris/doris-fe-deployment.yaml
kubectl rollout status statefulset/doris-fe -n prod
kubectl apply -f manifests/doris/doris-be-deployment.yaml
kubectl rollout status deployment/doris-be -n prod
```

## Connect
```bash
mysql -h 192.168.1.50 -P 30090 -u root
# No password on first boot. Set one immediately:
# SET PASSWORD FOR 'root'@'%' = PASSWORD('<your-password>');
```

## Node-Level Requirements — worker1.local

The Doris BE requires `vm.max_map_count ≥ 2,000,000` on the host node. This is set permanently on all nodes via `/etc/sysctl.d/99-doris.conf`:

```bash
# Verify on worker1.local
ssh root@192.168.1.51 cat /proc/sys/vm/max_map_count
# Must show: 2000000

# If missing (e.g. after OS reinstall), re-apply:
sysctl -w vm.max_map_count=2000000 && \
echo 'vm.max_map_count=2000000' > /etc/sysctl.d/99-doris.conf && \
sysctl --system
```

> ⚠️ If `vm.max_map_count` is below 2,000,000, the BE will exit immediately (Exit Code 0) with the message:
> ```
> Set kernel parameter 'vm.max_map_count' to a value greater than 2000000
> ```
> See [Runbook 09 §9](runbooks/runbook-09-incident-postmortem.md#9-doris-be--crashloopbackoff-vmmax_map_count-too-low).

## FQDN Mode (FE)

The FE runs in **FQDN mode** (`enable_fqdn_mode=true`) as a `StatefulSet` with a headless service. This ensures BdbJE (the FE metadata store) records a stable DNS name rather than an ephemeral pod IP. After a reboot, the FE always rejoins its own quorum using the same FQDN:

```
doris-fe-0.doris-fe-headless.prod.svc.cluster.local
```

> ⚠️ **Do not convert the FE back to a `Deployment`.** A Deployment assigns a new pod IP on every restart — BdbJE will store the old IP and the FE will remain in `feType:UNKNOWN` after each reboot. See [Runbook 09 §5](runbooks/runbook-09-incident-postmortem.md#5-apache-doris-fe--degraded-bdbje-peer-address-stale).

## Strategy: Recreate (BE)

The BE uses `strategy: Recreate` because it mounts a `ReadWriteOnce` PVC (`doris-be-storage`). `RollingUpdate` would start a new pod before the old terminates, causing a `Multi-Attach` error on the PVC.

## Check Cluster Health
```sql
-- FE status
SHOW FRONTENDS\G
-- Expected: feType=MASTER, Alive=true, Host contains FQDN

-- BE status
SHOW BACKENDS\G
-- Expected: Alive=true, HeartbeatAddress using pod FQDN
```

```bash
# Quick health check via kubectl
kubectl get pod -n prod -l app=doris-fe
kubectl get pod -n prod -l app=doris-be
```

## Secrets
| Key | Description |
|---|---|
| `admin-password` | Doris root password |

OpenBao path: `secret/data/doris/credentials`

## Troubleshooting

### BE CrashLoopBackOff — vm.max_map_count
```bash
kubectl logs -n prod -l app=doris-be --tail=5 | grep "max_map_count"
```
If present: see [node-level requirements](#node-level-requirements--worker1local) above.

### FE feType:UNKNOWN
```bash
kubectl logs -n prod -l app=doris-fe | grep -i "bdbje\|peer\|UNKNOWN"
```
Cause is usually a leftover BdbJE metadata entry with a stale IP. Because the FE now runs in FQDN mode with a StatefulSet, this should not occur. If it does:
```bash
# Delete FE metadata and let it re-initialize (data loss of FE metadata — BEs re-register)
kubectl exec -n prod doris-fe-0 -- \
  rm -rf /opt/apache-doris/fe/doris-meta/bdb/
kubectl rollout restart statefulset/doris-fe -n prod
```

### BE not registering with FE
```bash
kubectl logs -n prod -l app=doris-be | grep -i "heartbeat\|FE\|register"
# Should show: successful heartbeat to FE at doris-fe-headless.prod.svc.cluster.local
```

If the BE is running but logs show `waiting to receive first heartbeat from frontend`:

1. **BE was running before FE restarted** — the BE's entrypoint only self-registers on pod
   start. Restart the BE so it re-runs registration:
   ```bash
   kubectl rollout restart deployment/doris-be -n prod
   kubectl rollout status deployment/doris-be -n prod
   ```

2. **Stale hostname registration** — when `enable_fqdn_mode=true` is set in `fe.conf`, the
   BE pod may self-register using its pod hostname (e.g. `doris-be-574d976cb9-nq76s`) instead
   of its IP. The FE cannot resolve the pod hostname and reports
   `java.net.UnknownHostException`. Drop the stale entry and re-add by IP:
   ```bash
   DORIS_ADMIN_PASS=$(kubectl get secret doris-credentials -n prod \
     -o jsonpath='{.data.admin-password}' | base64 -d)
   BE_IP=$(kubectl get pod -n prod -l app=doris-be -o jsonpath='{.items[0].status.podIP}')

   kubectl exec -n prod statefulset/doris-fe -c doris-fe -- \
     mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_ADMIN_PASS}" \
     -e "ALTER SYSTEM DROPP BACKEND '<stale-hostname>:9050';"

   kubectl exec -n prod statefulset/doris-fe -c doris-fe -- \
     mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_ADMIN_PASS}" \
     -e "ALTER SYSTEM ADD BACKEND '${BE_IP}:9050';"
   ```

3. **Cluster ID mismatch** — if the FE metadata PVC was wiped or the FE reinitialised, the
   stored cluster ID in the BE's storage directory will not match FE's. The BE logs show:
   ```
   invalid cluster id. ignore. Record cluster id=<BE_ID>, Invalid cluster_id=<FE_ID>
   ```
   Fix by updating the BE's stored cluster ID to match FE:
   ```bash
   # Get FE cluster ID
   kubectl exec -n prod statefulset/doris-fe -c doris-fe -- \
     cat /opt/apache-doris/fe/doris-meta/image/VERSION
   # Note the clusterId= value

   # Overwrite BE's stored cluster ID
   BE_POD=$(kubectl get pod -n prod -l app=doris-be -o jsonpath='{.items[0].metadata.name}')
   kubectl exec -n prod ${BE_POD} -c doris-be -- \
     bash -c "echo '<FE_CLUSTER_ID>' > /opt/apache-doris/be/storage/cluster_id"

   # Restart BE to reload
   kubectl rollout restart deployment/doris-be -n prod
   ```
   Then drop and re-add the BE backend entry (see step 2 above).

### `kubectl exec` defaults to wrong container

The FE pod has two containers: `doris-fe` and `krb-doris-guard`. `kubectl exec` without
`-c doris-fe` defaults to `krb-doris-guard` which has no `mysql` binary:

```
error: exec: "mysql": executable file not found in $PATH
```

Always specify the container explicitly:
```bash
kubectl exec -n prod statefulset/doris-fe -c doris-fe -- mysql ...
```
