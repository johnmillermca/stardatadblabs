# Runbook 35 — Upgrading Doris (Stardata FE) with Snowflake JDBC Patches

> **Applies to:** `192.168.1.50` cluster · namespace `prod` · Stardata FE StatefulSet  
> **Current version:** `doris-4.1.4.1-rc02` · **Custom image tag:** `fe-4.2`  
> **Snowflake account:** `oqihhtj-ta50603` · **Catalog:** `snowflake` (db: `SNOWFLAKE_AUGMENTATION`)

---

## Background

The Stardata FE image is **not** a plain upstream Doris image. It carries four Java patches
that add Snowflake JDBC catalog support, which does not exist in upstream Doris 4.1:

| Patched file | What it does |
|---|---|
| `stardata/doris-patch/JdbcResource.java` | Adds `JDBC_SNOWFLAKE` / `SNOWFLAKE` constants and a `parseDbType()` branch for `jdbc:snowflake://` URLs |
| `stardata/doris-patch/JdbcClient.java` | Adds a `SNOWFLAKE` case in `createJdbcClient()` returning a `JdbcSnowflakeClient`; removes Lombok (not on FE classpath) |
| `stardata/doris-patch/JdbcSnowflakeClient.java` | New class — Snowflake type mapping on top of `JdbcClient` base |
| `stardata/doris-patch/JdbcTable.java` | Maps `"snowflake"` → `TOdbcTableType.POSTGRESQL`; adds explicit getters/setters (no Lombok) |

These patches are compiled at Docker build time against the `doris-fe.jar` that ships inside
the base image.  If the upstream Doris version changes those class signatures, `javac` will
**fail at build time** (not silently at runtime).

The busybox `httpd` sidecar in the FE StatefulSet serves `snowflake-jdbc-4.0.2.jar` on
`:8181` so the Doris BE can download it.  This sidecar is defined in git and survives
upgrades automatically.

---

## What Survives a Doris Upgrade Automatically

| Component | Storage | Survives? |
|---|---|---|
| `snowflake` catalog definition | BdbJE metadata on the 100 Gi Retain PVC (`/opt/apache-doris/fe/doris-meta`) | ✅ Yes — no action needed |
| `snowflake-credentials` K8s Secret | Cluster namespace `prod` | ✅ Yes |
| `busybox httpd` sidecar + port 8181 | `manifests/stardata/stardata-fe-deployment.yaml` in git | ✅ Yes — ArgoCD reapplies |
| `driver_url` = `http://stardata-fe.prod.svc.cluster.local:8181/...` | `manifests/stardata/setup/10_create_snowflake_catalog.sql` in git | ✅ Yes |
| Java patches | Compiled against the **base image's** `doris-fe.jar` | ⚠️ Must rebuild — may need re-patching |
| `snowflake-jdbc-4.0.2.jar` in image | `stardata/jars/snowflake-jdbc-4.0.2.jar` COPY'd in `Dockerfile.fe` | ✅ Yes — stays in build context |

---

## Upgrade Checklist

### Step 1 — Find the new Doris image tag

```bash
# Browse available tags on Docker Hub
curl -s "https://registry.hub.docker.com/v2/repositories/apache/doris/tags/?page_size=20&name=fe-" \
  | python3 -m json.tool | grep '"name"' | head -20

# Example — upgrading to 5.5.0
NEW_TAG="fe-5.5.0"
```

> ⚠️ Always use a **specific version tag** (e.g. `fe-5.5.0`), never `fe-latest`.
> The base image is currently pinned in `stardata/Dockerfile.fe` — change both
> the builder stage and the runtime stage.

---

### Step 2 — Update `Dockerfile.fe` with the new tag

Edit [`stardata/Dockerfile.fe`](../../stardata/Dockerfile.fe) — change the two `FROM` lines:

```dockerfile
# Stage 1 — builder
FROM docker.io/apache/doris:fe-5.5.0 AS builder   # ← was fe-4.1.4.1-rc02

# Stage 2 — runtime
FROM docker.io/apache/doris:fe-5.5.0              # ← was fe-4.1.4.1-rc02
```

Also update the `LABEL` to reflect the new version:

```dockerfile
LABEL org.stardata.version="5.5"
LABEL org.stardata.base="apache/doris:branch-5.5"
```

---

### Step 3 — Attempt the build (patch compile check)

```bash
bash scripts/master/build-and-push-custom-images.sh stardata-fe 2>&1 | tee /tmp/fe-build.log
```

**Two outcomes:**

#### A) Build succeeds — no patch changes needed
```
[2/2] STEP 6/6: COPY jars/snowflake-jdbc-4.0.2.jar ...
Successfully tagged 192.168.1.50:30500/apache/stardata:fe-4.2
✓ stardata-fe → 192.168.1.50:30500/apache/stardata:fe-4.2
```
→ Skip to **Step 5**.

#### B) Build fails with `javac` errors
```
error: cannot find symbol
  method getJdbcDbType()
...
1 error
```
→ Continue to **Step 4**.

---

### Step 4 — Re-patch the Java files (only if Step 3 failed)

The upstream Doris source for the affected classes is at:
```
https://github.com/apache/doris/tree/<commit>/fe/fe-core/src/main/java/org/apache/doris/
  catalog/JdbcResource.java
  catalog/JdbcTable.java
  datasource/jdbc/client/JdbcClient.java
```

#### 4a — Pull the new upstream source

```bash
# Find the commit hash for the new tag
git clone --depth 1 --branch <new-doris-tag> https://github.com/apache/doris /tmp/doris-src

# Copy the files you need to diff
cp /tmp/doris-src/fe/fe-core/src/main/java/org/apache/doris/catalog/JdbcResource.java  /tmp/JdbcResource.upstream.java
cp /tmp/doris-src/fe/fe-core/src/main/java/org/apache/doris/catalog/JdbcTable.java     /tmp/JdbcTable.upstream.java
cp /tmp/doris-src/fe/fe-core/src/main/java/org/apache/doris/datasource/jdbc/client/JdbcClient.java /tmp/JdbcClient.upstream.java
```

#### 4b — Diff each patch against the new upstream

```bash
diff /tmp/JdbcResource.upstream.java stardata/doris-patch/JdbcResource.java
diff /tmp/JdbcTable.upstream.java    stardata/doris-patch/JdbcTable.java
diff /tmp/JdbcClient.upstream.java   stardata/doris-patch/JdbcClient.java
```

**What to look for in each file:**

| File | Key things our patch adds — check these still apply cleanly |
|---|---|
| `JdbcResource.java` | Constants `JDBC_SNOWFLAKE = "SNOWFLAKE"` and `SNOWFLAKE = "snowflake"`; `parseDbType()` branch `case "snowflake"` |
| `JdbcClient.java` | `case SNOWFLAKE:` in `createJdbcClient()` returning `new JdbcSnowflakeClient(...)`; no Lombok imports; explicit `getJdbcDbType()` getter |
| `JdbcTable.java` | `TABLE_TYPE_MAP.put("snowflake", TOdbcTableType.POSTGRESQL)` in the static initializer; no `@Getter`/`@Setter`; explicit getter/setter methods |
| `JdbcSnowflakeClient.java` | Entirely new file — just needs to compile against the new `JdbcClient` base class; check constructor signature has not changed |

#### 4c — Apply fixes and rebuild

After updating the patch files, retry:

```bash
bash scripts/master/build-and-push-custom-images.sh stardata-fe 2>&1 | tee /tmp/fe-build.log
grep -E "error:|successfully tagged|FAILED" /tmp/fe-build.log
```

Repeat until the build is clean.

---

### Step 5 — Verify the jar is in the new image

```bash
# Pull and inspect — jar must be present
podman run --rm --tls-verify=false \
  192.168.1.50:30500/apache/stardata:fe-4.2 \
  ls -lh /opt/apache-doris/fe/jdbc_drivers/
# Expected: snowflake-jdbc-4.0.2.jar  ~95 MB

# Verify patches compiled correctly (check Snowflake class is present)
podman run --rm --tls-verify=false \
  192.168.1.50:30500/apache/stardata:fe-4.2 \
  bash -c "jar tf /opt/apache-doris/fe/lib/doris-fe.jar | grep -i snowflake"
# Expected:
#   org/apache/doris/catalog/JdbcResource.class
#   org/apache/doris/datasource/jdbc/client/JdbcSnowflakeClient.class
```

---

### Step 6 — Commit and push to git

```bash
cd /home/star_master/k8s-platform
git add stardata/Dockerfile.fe stardata/doris-patch/   # include any re-patched files
git commit -m "chore: upgrade Doris base image to fe-5.5.0, re-patch Snowflake JDBC classes"
git push origin main
```

---

### Step 7 — Evict old cached image from worker nodes

Because `imagePullPolicy: IfNotPresent` and the image tag does not change (`fe-4.2`),
worker nodes will not pull the new image unless the old one is deleted first.

```bash
for NODE in worker2.local worker3.local worker4.local; do
  echo "=== Evicting from ${NODE} ==="
  kubectl debug node/${NODE} -it --image=192.168.1.50:30500/busybox:1.36 \
    -- sh -c "chroot /host crictl rmi 192.168.1.50:30500/apache/stardata:fe-4.2 2>&1"
done
```

Expected output per node: `Deleted: 192.168.1.50:30500/apache/stardata:fe-4.2`

---

### Step 8 — Roll the FE pods

```bash
kubectl rollout restart statefulset/stardata-fe -n prod
kubectl rollout status  statefulset/stardata-fe -n prod --timeout=300s
```

Wait until all 3 pods show `3/3 Running`:

```bash
kubectl get pods -n prod -l app=stardata-fe
# NAME            READY   STATUS    RESTARTS   AGE
# stardata-fe-0   3/3     Running   0          2m
# stardata-fe-1   3/3     Running   0          4m
# stardata-fe-2   3/3     Running   0          6m
```

> The 3 containers per pod are: `stardata-fe`, `ddl-mirror`, `jdbc-httpd`.

---

### Step 9 — Verify the sidecar is serving the jar

```bash
kubectl exec -n prod stardata-fe-2 -c jdbc-httpd -- \
  wget -q -O /dev/null --server-response \
  http://localhost:8181/snowflake-jdbc-4.0.2.jar 2>&1 | head -3
# Expected: HTTP/1.1 200 OK
```

---

### Step 10 — Verify the Snowflake catalog still works

The catalog is persisted in BdbJE and should survive automatically.  Confirm:

```bash
STARDATA_PASS=$(kubectl get secret stardata-credentials \
  -n prod -o jsonpath='{.data.admin-password}' | base64 -d)

kubectl exec -n prod stardata-fe-2 -c stardata-fe -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${STARDATA_PASS}" --connect-timeout=10 \
  -e "SHOW CATALOGS;" 2>&1 | grep -v Warning
# Expected: snowflake  jdbc  ...

kubectl exec -n prod stardata-fe-2 -c stardata-fe -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${STARDATA_PASS}" --connect-timeout=10 \
  -e "SHOW DATABASES FROM snowflake;" 2>&1 | grep -v Warning
# Expected: AUG_TEST, PUBLIC, information_schema, mysql

kubectl exec -n prod stardata-fe-2 -c stardata-fe -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${STARDATA_PASS}" --connect-timeout=10 \
  -e "SELECT COUNT(*) FROM snowflake.AUG_TEST.CUSTOMER;" 2>&1 | grep -v Warning
# Expected: a row count (not an error)
```

---

### Step 11 — If the catalog is broken after upgrade

If `SHOW DATABASES FROM snowflake` returns an error after the upgrade, recreate the catalog:

```bash
SNOWFLAKE_PASS=$(kubectl get secret snowflake-credentials \
  -n prod -o jsonpath='{.data.password}' | base64 -d)
STARDATA_PASS=$(kubectl get secret stardata-credentials \
  -n prod -o jsonpath='{.data.admin-password}' | base64 -d)

kubectl exec -n prod stardata-fe-2 -c stardata-fe -- bash -c "
mysql -h 127.0.0.1 -P 9030 -u root -p'${STARDATA_PASS}' --connect-timeout=10 -e \"
  DROP CATALOG IF EXISTS snowflake;
  CREATE CATALOG snowflake PROPERTIES (
    'type'         = 'jdbc',
    'user'         = 'testsnowflake',
    'password'     = '${SNOWFLAKE_PASS}',
    'jdbc_url'     = 'jdbc:snowflake://oqihhtj-ta50603.snowflakecomputing.com/?warehouse=COMPUTE_WH&loginTimeout=30&user=testsnowflake&password=${SNOWFLAKE_PASS}&db=SNOWFLAKE_AUGMENTATION',
    'driver_url'   = 'http://stardata-fe.prod.svc.cluster.local:8181/snowflake-jdbc-4.0.2.jar',
    'driver_class' = 'net.snowflake.client.jdbc.SnowflakeDriver'
  );
  REFRESH CATALOG snowflake;
\" 2>&1
" 2>&1 | grep -v Warning
```

---

## Rollback

If the new image causes FE instability:

```bash
# 1. Revert Dockerfile.fe to the old tag in git and push
git revert HEAD   # or manually edit and push

# 2. Rebuild the old image
bash scripts/master/build-and-push-custom-images.sh stardata-fe

# 3. Evict new image, re-roll
for NODE in worker2.local worker3.local worker4.local; do
  kubectl debug node/${NODE} -it --image=192.168.1.50:30500/busybox:1.36 \
    -- sh -c "chroot /host crictl rmi 192.168.1.50:30500/apache/stardata:fe-4.2 2>&1"
done
kubectl rollout restart statefulset/stardata-fe -n prod
```

The catalog data in BdbJE on the PVC is unaffected by rollback.

---

## Reference

| File | Purpose |
|---|---|
| [`stardata/Dockerfile.fe`](../../stardata/Dockerfile.fe) | Base image tag, patch compilation, jar COPY |
| [`stardata/doris-patch/`](../../stardata/doris-patch/) | 4 patched Java source files |
| [`stardata/jars/snowflake-jdbc-4.0.2.jar`](../../stardata/jars/snowflake-jdbc-4.0.2.jar) | JDBC driver baked into image |
| [`manifests/stardata/stardata-fe-deployment.yaml`](../../manifests/stardata/stardata-fe-deployment.yaml) | FE StatefulSet — sidecar, initContainer, volumes |
| [`manifests/stardata/stardata-services.yaml`](../../manifests/stardata/stardata-services.yaml) | FE Service — port 8181 |
| [`manifests/stardata/setup/10_create_snowflake_catalog.sql`](../../manifests/stardata/setup/10_create_snowflake_catalog.sql) | Catalog DDL with correct `driver_url` and `jdbc_url` |
| [`manifests/stardata/setup/snowflake-credentials-secret.yaml`](../../manifests/stardata/setup/snowflake-credentials-secret.yaml) | K8s Secret for Snowflake password |
