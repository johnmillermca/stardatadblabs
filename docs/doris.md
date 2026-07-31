# Apache Doris

## Overview
Apache Doris 4.0.7 — MPP analytical database for real-time analytics. Deployed with one FE (Frontend, StatefulSet) and one BE (Backend, Deployment) in the `prod` namespace.

| Property | Value |
|---|---|
| FE image | `192.168.1.50:30500/apache/doris:fe-4.0.7-ranger` (Ranger-enabled) |
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

---

## Ranger RBAC Integration

Doris FE enforces fine-grained access control via Apache Ranger 2.7.0. The
built-in `RangerDorisAccessControllerFactory` (inside `doris-fe.jar`) uses
the Ranger Hive service type to evaluate policies on every SQL query.

### Architecture

```
SQL query
   |
   v
Doris FE  --(access_controller_type=ranger-doris)-->  RangerDorisPlugin
                                                             |
                                         polls every 30s    |
                                                             v
                                              Ranger Admin :6080
                                              service name: doris
                                              service type: hive
```

Policies are cached locally in `/opt/apache-doris/fe/ranger-cache/` so **no
network call is made per query** — only a background poll every 30 seconds.

### Custom FE Image

The Ranger-enabled FE image is built from [`docker/doris-ranger/Dockerfile`](../docker/doris-ranger/Dockerfile).
It adds upstream Ranger 2.7.0 JARs on top of the base `fe-4.0.7` image:

```bash
# Build and push (run from repo root on master node)
podman build --platform linux/amd64 \
  -t 192.168.1.50:30500/apache/doris:fe-4.0.7-ranger \
  docker/doris-ranger/
podman push 192.168.1.50:30500/apache/doris:fe-4.0.7-ranger
```

> **Note:** `ranger-plugins-audit` 2.7.0 has no JAR on Maven Central — the
> Dockerfile uses `ranger-audit-core` instead.

### How the Plugin Loads

Doris FE `start_fe.sh` adds `${DORIS_HOME}/conf` to the JVM classpath.
The Ranger plugin reads its config from two files **on the classpath**:

| File | Purpose |
|---|---|
| `ranger-doris-security.xml` | Ranger Admin URL, service name, poll interval, cache dir |
| `ranger-doris-audit.xml` | Audit destination settings |

Both files are mounted directly into `/opt/apache-doris/fe/conf/` via the
`doris-ranger-config` ConfigMap using individual `subPath` mounts — **not**
into a subdirectory (subdirectories are not on the classpath).

### Key Config Values

**`fe.conf`** (in `doris-fe-config` ConfigMap):
```
access_controller_type = ranger-doris
```

> The factory identifier is `ranger-doris` — confirmed from
> `RangerDorisAccessControllerFactory.factoryIdentifier()` in `doris-fe.jar`.
> Using `ranger` or `ranger-hive` will throw `No authorization plugin factory found`.

**`ranger-doris-security.xml`**:
```xml
<property>
  <name>ranger.plugin.doris.service.name</name>
  <value>doris</value>           <!-- must match service name in Ranger Admin -->
</property>
<property>
  <name>ranger.plugin.doris.policy.rest.url</name>
  <value>http://ranger-admin.prod.svc.cluster.local:6080</value>
</property>
<property>
  <name>ranger.plugin.doris.policy.pollIntervalMs</name>
  <value>30000</value>
</property>
<property>
  <name>ranger.plugin.doris.policy.cache.dir</name>
  <value>/opt/apache-doris/fe/ranger-cache</value>
</property>
```

> All properties use `ranger.plugin.doris.*` prefix — **not** `ranger.plugin.hive.*`.

### Registering the Doris Service in Ranger Admin

Doris uses the Ranger **Hive** service type (no native `doris` service type
exists in Ranger 2.7.0). The service must be named exactly `doris` to match
`ranger.plugin.doris.service.name` in the security XML.

**Via REST API (recommended — idempotent, scriptable):**
```bash
RANGER_PASS=$(kubectl get secret ranger-db-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
DORIS_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)

curl -s -u "admin:${RANGER_PASS}" \
  -X POST http://192.168.1.50:30680/service/public/v2/api/service \
  -H "Content-Type: application/json" \
  -d "{
    \"type\": \"hive\",
    \"name\": \"doris\",
    \"displayName\": \"Apache Doris\",
    \"description\": \"Apache Doris 4.0.7 RBAC via Ranger Hive plugin\",
    \"isEnabled\": true,
    \"configs\": {
      \"username\": \"root\",
      \"password\": \"${DORIS_PASS}\",
      \"jdbc.driverClassName\": \"com.mysql.jdbc.Driver\",
      \"jdbc.url\": \"jdbc:mysql://doris-fe.prod.svc.cluster.local:9030\",
      \"policy.download.auth.users\": \"root\",
      \"tag.download.auth.users\": \"root\",
      \"ranger.plugin.policy.pollIntervalMs\": \"30000\"
    }
  }"
```

**Via Ranger Admin UI:**
1. Open `http://192.168.1.50:30680` → login as `admin`
2. **Access Manager → Service Manager → HIVE → +**
3. Fill in:
   - Service Name: `doris`
   - Username: `root` / Password: from `doris-credentials` secret
   - jdbc.driverClassName: `com.mysql.jdbc.Driver`
   - jdbc.url: `jdbc:mysql://doris-fe.prod.svc.cluster.local:9030`
4. **Save**

### Creating Policies

After the service is registered, policies are created under **Access Manager → doris**:

| Resource | Grant | Example |
|---|---|---|
| database=`*`, table=`*`, column=`*` | SELECT to `analyst` group | Full read access |
| database=`pii`, table=`customers`, column=`ssn` | MASK to `analyst` | Data masking |
| database=`prod`, table=`orders` | SELECT, INSERT to `etl_user` | ETL access |

### Verifying Ranger RBAC is Active

```bash
# 1. Check FE loaded the plugin
kubectl logs -n prod doris-fe-0 | grep -i "ranger-doris\|PolicyRefresher"
# Expected:
#   Found Authentication Plugin Factories: ranger-doris from class path.
#   PolicyRefresher(serviceName=doris): found updated version. ... newVersion=11
#   This policy engine contains N policy evaluators

# 2. Check XMLs are on classpath
kubectl exec -n prod doris-fe-0 -- ls /opt/apache-doris/fe/conf/ | grep ranger
# Expected:
#   ranger-doris-audit.xml
#   ranger-doris-security.xml

# 3. Check fe.conf has Ranger enabled
kubectl exec -n prod doris-fe-0 -- grep access_controller /opt/apache-doris/fe/conf/fe.conf
# Expected:
#   access_controller_type = ranger-doris

# 4. Verify service exists in Ranger
RANGER_PASS=$(kubectl get secret ranger-db-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
curl -s -u "admin:${RANGER_PASS}" \
  http://192.168.1.50:30680/service/public/v2/api/service/doris | python3 -m json.tool
```

### Re-deploying from Scratch

If the Doris StatefulSet is deleted and recreated (e.g. after cluster wipe),
follow this sequence due to StatefulSet immutability:

```bash
# 1. Delete StatefulSet preserving the PVC (cascade=orphan)
kubectl delete statefulset doris-fe -n prod --cascade=orphan

# 2. Recreate from manifest
kubectl apply -f manifests/doris/doris-fe-deployment.yaml

# 3. If PVC was also deleted, recreate it first:
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: doris-fe-meta
  namespace: prod
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: local-path
  resources:
    requests:
      storage: 100Gi
EOF

# 4. Force pod restart to re-run init container with latest ConfigMap
kubectl delete pod doris-fe-0 -n prod
kubectl rollout status statefulset/doris-fe -n prod --timeout=300s
```

> ⚠️ **StatefulSet volume changes require full delete+recreate.**
> `kubectl apply` cannot update the `volumes` list of an existing StatefulSet.
> Always use `--cascade=orphan` to preserve the PVC, then re-apply.

> ⚠️ **ArgoCD manages the ConfigMap.** Manual `kubectl patch` on `doris-fe-config`
> will be reverted on the next ArgoCD sync. Always commit to git and push first,
> then force a refresh:
> ```bash
> kubectl annotate application doris -n argocd argocd.argoproj.io/refresh=hard --overwrite
> ```

### Troubleshooting Ranger

| Symptom | Cause | Fix |
|---|---|---|
| `No authorization plugin factory found for ranger` | Wrong `access_controller_type` | Must be `ranger-doris`, not `ranger` or `hive` |
| `bound must be positive` at `RangerRESTClient` | Security XML not on classpath | Mount XMLs into `fe/conf/` not a subdirectory |
| `ranger.plugin.hive.*` properties ignored | Wrong property prefix | Use `ranger.plugin.doris.*` |
| `404 RANGER_ERROR_SERVICE_NOT_FOUND` | Service not registered in Ranger Admin | Run the REST API registration command above |
| Policies not applying after creation | Poll interval not elapsed | Wait 30s or restart FE pod |

---

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
