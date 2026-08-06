# Runbook 12 — RBAC Control Plane: New User Setup & Verification

> **RBAC API:** `http://192.168.1.50:30850` · **Swagger UI:** `http://192.168.1.50:30850/docs`  
> **Realm:** `STARDATADBLABS.LOCAL` · **KDC:** `kerberos-kdc.prod.svc.cluster.local:88`  
> **Related runbooks:** [08 — Security & Access](runbook-08-security-access.md) · [11 — Kerberos Client Auth](runbook-11-kerberos-integration.md)

---

## Overview

This runbook covers the full lifecycle of adding a new user to the platform and verifying their access across all four services:

| Service | Access mechanism | What we verify |
|---|---|---|
| **Apache Doris** | Native SQL GRANT/REVOKE via krb-doris-guard proxy | `mysql` CLI query succeeds |
| **Apache Kafka** | Strimzi `KafkaUser` CR + SCRAM ACLs | Producer/consumer test |
| **Apache OpenSearch** | Security plugin internal user + role mapping | `curl` search request |
| **Apache Spark** | `spark-rbac-allowlist` ConfigMap | Job submission |

**End-to-end flow for every new user:**

```
1. Create KDC principal  (Kerberos KDC)
2. Register user         (RBAC Control Plane)
3. Bind role             (RBAC Control Plane)
4. Sync to services      (RBAC Control Plane → Doris / Kafka / OpenSearch / Spark)
5. Verify access         (test commands per service)
```

---

## Prerequisites

### Shell environment (run all commands from master.local)

```bash
# Install CLI dependencies (once)
pip3 install --quiet httpx typer rich

# Set up rbacctl alias
alias rbacctl="python3 /root/k8s-platform/rbac-plane/cli/rbacctl.py"

# Set RBAC API endpoint and master token
# (master token was printed by seed-rbac-credentials.sh — retrieve from OpenBao if lost)
export RBAC_URL="http://192.168.1.50:30850"
export RBAC_TOKEN=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.MASTER_TOKEN}' | base64 -d)

# Quick sanity check
rbacctl services
# Expected output: table showing doris, kafka, opensearch, spark
```

### Verify the RBAC control plane is running

```bash
kubectl get pods -n prod -l app=rbac-plane
# Expected: 2 pods  STATUS=Running

curl -s http://192.168.1.50:30850/health | python3 -m json.tool
# Expected: {"status": "ok", "version": "1.0.0"}

kubectl get pods -n prod -l app=redis
# Expected: 1 pod  STATUS=Running
```

---

## Section 1 — Adding a Standard Analyst User (`alice`)

This is the most common pattern: a new data analyst who needs read-only access to Doris and OpenSearch, can consume Kafka topics, and can view the Spark UI.

### Step 1 — Create the Kerberos principal

```bash
# Create the principal (will prompt for password — use a strong password)
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc alice@STARDATADBLABS.LOCAL"

# Verify it was created
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "getprinc alice@STARDATADBLABS.LOCAL"
# Expected line: "Principal: alice@STARDATADBLABS.LOCAL"
```

### Step 2 — Export keytab (for service-account / batch access)

```bash
# Export keytab inside the KDC pod
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "ktadd -k /tmp/alice.keytab alice@STARDATADBLABS.LOCAL"

# Copy to master
KDC_POD=$(kubectl get pod -n prod -l app=kerberos-kdc \
  -o jsonpath='{.items[0].metadata.name}')
kubectl cp prod/${KDC_POD}:/tmp/alice.keytab /tmp/alice.keytab

# Verify keytab contents
klist -ekt /tmp/alice.keytab
# Expected: shows alice@STARDATADBLABS.LOCAL entry

# Store as K8s secret
kubectl create secret generic alice-keytab \
  --from-file=keytab=/tmp/alice.keytab \
  -n prod \
  --dry-run=client -o yaml | kubectl apply -f -

# Clean up temp files
kubectl exec -n prod deploy/kerberos-kdc -- rm /tmp/alice.keytab
rm /tmp/alice.keytab
```

### Step 3 — Register user in the RBAC control plane

```bash
rbacctl user create alice \
  --name "Alice Smith" \
  --email "alice@example.com"

# Confirm
rbacctl user get alice
# Expected: table row with username=alice, enabled=true
```

### Step 4 — Bind the `analyst` role

```bash
rbacctl user bind alice analyst

# Confirm binding
rbacctl user bindings alice
# Expected: row with role_name=analyst, service_name=(empty = all services)
```

### Step 5 — Sync to all services

```bash
# Dry-run first — preview what will be applied
rbacctl sync run --user alice --dry-run

# Apply
rbacctl sync run --user alice

# Expected output (one row per service):
#   alice  doris        synced   1 permissions applied
#   alice  kafka        synced   2 permissions applied
#   alice  opensearch   synced   2 permissions applied
#   alice  spark        synced   1 permissions applied
```

### Step 6 — Verify effective permissions

```bash
rbacctl user roles alice
# Expected:
#   Roles: analyst
#   Permissions table:
#     doris       SELECT        {}
#     kafka       CONSUME       {}
#     kafka       DESCRIBE      {}
#     opensearch  INDEX_READ    {}
#     opensearch  CLUSTER_READ  {}
#     spark       VIEW_UI       {}
```

---

## Section 2 — Verifying Access Per Service

### 2.1 Doris — SQL read access

```bash
# Obtain a Kerberos TGT for alice (on a node with krb5 client)
kinit alice@STARDATADBLABS.LOCAL
klist  # verify ticket is present

# Connect via MySQL client through the krb-doris-guard proxy
# (the guard validates alice's KDC principal before passing to Doris)
mysql -h 192.168.1.50 -P 30090 -u alice -p
# Enter alice's Doris password when prompted

# Inside the mysql session:
# ── Should work (analyst has SELECT) ──
SHOW DATABASES;
USE information_schema;
SELECT TABLE_NAME FROM TABLES LIMIT 5;

# ── Should be denied (no INSERT) ──
CREATE DATABASE alice_test;
# Expected: ERROR 1045 (28000): Access denied; you need CREATE privilege

exit
```

**Verify the GRANT was applied directly:**

```bash
# Connect as root and check alice's privileges
DORIS_ROOT_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)

kubectl exec -n prod statefulset/doris-fe -c doris-fe -- \
  mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_ROOT_PASS}" \
  -e "SHOW GRANTS FOR 'alice'@'%';"
# Expected: GRANT SELECT_PRIV ON *.* TO 'alice'@'%'
```

### 2.2 Kafka — CONSUME access

```bash
# Verify the KafkaUser CR was created
kubectl get kafkauser alice -n prod
# Expected: STATUS=Ready

# Check the ACL rules in the CR
kubectl get kafkauser alice -n prod -o jsonpath='{.spec.authorization}' \
  | python3 -m json.tool
# Expected: acls list with Read (topic), Describe (topic), Read (group)

# Get alice's SCRAM password (created by Strimzi when KafkaUser is Ready)
ALICE_KAFKA_PASS=$(kubectl get secret alice -n prod \
  -o jsonpath='{.data.password}' | base64 -d)
echo "Alice Kafka SCRAM password: ${ALICE_KAFKA_PASS}"

# Test consume from a topic (using the standard image)
kubectl run kafka-test-alice --rm -it --restart=Never \
  --image 192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0 \
  -n prod \
  -- kafka-console-consumer.sh \
    --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092 \
    --topic debezium-offsets \
    --from-beginning \
    --max-messages 3 \
    --consumer-property security.protocol=SASL_PLAINTEXT \
    --consumer-property sasl.mechanism=SCRAM-SHA-512 \
    --consumer-property "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=alice password=${ALICE_KAFKA_PASS};"
# Expected: reads up to 3 messages and exits (no auth errors)

# Verify alice CANNOT produce (no PRODUCE permission in analyst role)
kubectl run kafka-deny-alice --rm -it --restart=Never \
  --image 192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0 \
  -n prod \
  -- kafka-console-producer.sh \
    --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092 \
    --topic debezium-offsets \
    --producer-property security.protocol=SASL_PLAINTEXT \
    --producer-property sasl.mechanism=SCRAM-SHA-512 \
    --producer-property "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=alice password=${ALICE_KAFKA_PASS};"
# Expected: LEADER_NOT_AVAILABLE or AUTHORIZATION_EXCEPTION — not allowed to write
# (Ctrl+C after a few seconds)
```

### 2.3 OpenSearch — INDEX_READ access

```bash
OPENSEARCH_PASS=$(kubectl get secret opensearch-credentials -n prod \
  -o jsonpath='{.data.opensearch-password}' | base64 -d)

# Verify alice's internal user was created
curl -sk -u "admin:${OPENSEARCH_PASS}" \
  "https://192.168.1.50:30920/_plugins/_security/api/internalusers/alice" \
  | python3 -m json.tool
# Expected: {"alice": {...}} — user exists

# Verify alice's role mapping
curl -sk -u "admin:${OPENSEARCH_PASS}" \
  "https://192.168.1.50:30920/_plugins/_security/api/rolesmapping" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
alice_roles = [r for r, v in d.items() if 'alice' in v.get('users', [])]
print('Alice is mapped to roles:', alice_roles)
"
# Expected: roles beginning with rbac_index_read_ and rbac_cluster_read_

# Test that alice can read cluster health (CLUSTER_READ)
ALICE_OS_PASS="alice_opensearch_password"  # set to alice's OpenSearch password
curl -sk -u "alice:${ALICE_OS_PASS}" \
  "https://192.168.1.50:30920/_cluster/health" | python3 -m json.tool
# Expected: {"status":"green",...} — read access works

# Test alice can search (INDEX_READ)
curl -sk -u "alice:${ALICE_OS_PASS}" \
  "https://192.168.1.50:30920/_cat/indices?v"
# Expected: list of indices — read access works

# Test alice CANNOT write (no INDEX_WRITE)
curl -sk -u "alice:${ALICE_OS_PASS}" \
  -X PUT "https://192.168.1.50:30920/alice-test-index" \
  -H "Content-Type: application/json" \
  -d '{"settings":{"number_of_shards":1}}'
# Expected: {"status":403,...} — permission denied
```

### 2.4 Spark — VIEW_UI access (allowlist ConfigMap)

```bash
# Verify alice appears in the spark-rbac-allowlist ConfigMap
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | python3 -m json.tool
# Expected: entry for "alice": {"can_submit": false, "can_kill_any": false, "view_ui": true}
# (analyst role only grants VIEW_UI, not SUBMIT_JOB)

# Verify alice cannot submit a job (guard will check allowlist)
# If krb-spark-guard is active on port 30777, a submission attempt without
# can_submit=true will be rejected at the RPC proxy with an auth error.
```

---

## Section 3 — Adding an ETL Writer User (`etl_bob`)

An ETL user who needs write access to Doris and can both produce and consume Kafka.

```bash
# Step 1: Create KDC principal
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc etl_bob@STARDATADBLABS.LOCAL"

# Step 2: Register in RBAC plane
rbacctl user create etl_bob --name "Bob ETL" --email "bob@example.com"

# Step 3: Bind etl_writer role
rbacctl user bind etl_bob etl_writer

# Step 4: Also give bob Spark job submission
rbacctl user bind etl_bob spark_user --service spark

# Step 5: Sync
rbacctl sync run --user etl_bob

# Step 6: Verify effective permissions
rbacctl user roles etl_bob
# Expected permissions:
#   doris       SELECT        {}
#   doris       INSERT        {}
#   doris       UPDATE        {}
#   doris       LOAD          {}
#   kafka       PRODUCE       {}
#   kafka       CONSUME       {}
#   kafka       DESCRIBE      {}
#   spark       SUBMIT_JOB    {}
#   spark       KILL_OWN_JOB  {}
#   spark       VIEW_UI       {}
```

### Verify Doris write access for etl_bob

```bash
mysql -h 192.168.1.50 -P 30090 -u etl_bob -p

# Inside session:
CREATE DATABASE IF NOT EXISTS bob_test;
USE bob_test;
CREATE TABLE test_writes (id INT, val VARCHAR(100)) 
  DISTRIBUTED BY HASH(id) BUCKETS 1;
INSERT INTO test_writes VALUES (1, 'hello from etl_bob');
SELECT * FROM test_writes;
# Expected: row returned

DROP TABLE test_writes;
DROP DATABASE bob_test;
exit
```

### Verify Kafka produce access for etl_bob

```bash
ETLBOB_KAFKA_PASS=$(kubectl get secret etl-bob -n prod \
  -o jsonpath='{.data.password}' | base64 -d 2>/dev/null || \
  kubectl get secret etl_bob -n prod \
  -o jsonpath='{.data.password}' | base64 -d)

kubectl run kafka-prod-bob --rm -it --restart=Never \
  --image 192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0 \
  -n prod \
  -- kafka-console-producer.sh \
    --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092 \
    --topic schema-registry-schemas \
    --producer-property security.protocol=SASL_PLAINTEXT \
    --producer-property sasl.mechanism=SCRAM-SHA-512 \
    --producer-property "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=etl_bob password=${ETLBOB_KAFKA_PASS};"
# Type a test message, press Enter, then Ctrl+C
# Expected: message written without error

# Verify Spark allowlist has can_submit=true
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | python3 -m json.tool
# Expected: "etl_bob": {"can_submit": true, "can_kill_any": false, "view_ui": true}
```

---

## Section 4 — Service-Scoped Role Binding

Give `contractor1` Kafka-only analyst access (CONSUME only), expiring in 30 days.

```bash
# Create KDC principal
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc contractor1@STARDATADBLABS.LOCAL"

# Register user
rbacctl user create contractor1 --name "Contractor One"

# Bind analyst role scoped to Kafka only, expires in 30 days
rbacctl user bind contractor1 kafka_consumer \
  --service kafka \
  --expires-days 30

# Sync
rbacctl sync run --user contractor1

# Verify: only Kafka permissions, no Doris/OpenSearch/Spark
rbacctl user roles contractor1
# Expected: only kafka CONSUME and DESCRIBE

# Verify contractor1 has NO Doris user
DORIS_ROOT_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
kubectl exec -n prod statefulset/doris-fe -c doris-fe -- \
  mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_ROOT_PASS}" \
  -e "SHOW GRANTS FOR 'contractor1'@'%';" 2>&1
# Expected: ERROR or empty — no Doris user created

# Verify contractor1 KafkaUser CR was created
kubectl get kafkauser contractor1 -n prod
# Expected: STATUS=Ready
```

---

## Section 5 — Revoking Access (User Offboarding)

When a user leaves, their access must be removed from all services.

```bash
# Example: offboarding alice

# Step 1: Disable immediately (evicts cache, blocks new sessions within cache TTL)
rbacctl user disable alice
# Expected: User alice disabled

# Step 2: Sync with empty permission set (removes from all services)
rbacctl sync run --user alice
# Expected:
#   alice  doris        synced   0 permissions applied   (REVOKE ALL executed)
#   alice  kafka        synced   0 permissions applied   (KafkaUser CR updated, ACLs removed)
#   alice  opensearch   synced   0 permissions applied   (removed from all rbac_ roles)
#   alice  spark        synced   0 permissions applied   (removed from allowlist)

# Step 3: Remove bindings
rbacctl user bindings alice  # note the binding IDs
# For each binding ID shown:
rbacctl user unbind alice <binding_id>

# Step 4: Delete the KDC principal (prevents new ticket acquisition)
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "delprinc -force alice@STARDATADBLABS.LOCAL"

# Step 5: Delete K8s keytab secret
kubectl delete secret alice-keytab -n prod 2>/dev/null || echo "not found"

# Step 6: Delete the user from the RBAC plane
rbacctl user delete alice --yes

# Step 7: Verify removal from each service
echo "--- Doris ---"
kubectl exec -n prod statefulset/doris-fe -c doris-fe -- \
  mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_ROOT_PASS}" \
  -e "SELECT User, Host FROM mysql.user WHERE User='alice';" 2>&1
# Expected: Empty set

echo "--- Kafka ---"
kubectl get kafkauser alice -n prod 2>&1
# Expected: Error from server (NotFound)

echo "--- OpenSearch ---"
curl -sk -u "admin:${OPENSEARCH_PASS}" \
  "https://192.168.1.50:30920/_plugins/_security/api/internalusers/alice" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('STILL EXISTS' if 'alice' in d else 'REMOVED')"
# Expected: REMOVED

echo "--- Spark allowlist ---"
kubectl get configmap spark-rbac-allowlist -n prod \
  -o jsonpath='{.data.allowlist\.json}' | python3 -m json.tool | grep alice
# Expected: no output (alice not in allowlist)

echo "--- KDC ---"
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "listprincs" | grep "alice@" && \
  echo "WARNING: alice still in KDC" || echo "OK — KDC clean"
```

---

## Section 6 — Checking the Audit Log

Every operation is recorded in the audit log.

```bash
# Show last 20 audit events
rbacctl audit log --limit 20

# Filter by actor (who performed actions)
rbacctl audit log --actor master --limit 10

# Filter by action type
rbacctl audit log --action BIND_ROLE
rbacctl audit log --action SYNC

# Query via REST API (more filtering options)
RBAC_JWT=$(curl -s -X POST \
  "http://192.168.1.50:30850/api/v1/auth/token?raw_token=${RBAC_TOKEN}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl -s \
  -H "Authorization: Bearer ${RBAC_JWT}" \
  "http://192.168.1.50:30850/api/v1/audit?limit=50" \
  | python3 -m json.tool
```

---

## Section 7 — Creating a Custom Role

Create a role that gives write access to a specific Doris database only.

```bash
# Step 1: List Doris permissions to find the right permission IDs
rbacctl role perms --service doris
# Note the IDs for SELECT, INSERT, UPDATE, LOAD

# Step 2: Create the role via Swagger UI or API
# Use Swagger UI at http://192.168.1.50:30850/docs for interactive form
# POST /api/v1/roles

RBAC_JWT=$(curl -s -X POST \
  "http://192.168.1.50:30850/api/v1/auth/token?raw_token=${RBAC_TOKEN}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# Create a role: sales_analyst — SELECT on sales database only
curl -s -X POST \
  -H "Authorization: Bearer ${RBAC_JWT}" \
  -H "Content-Type: application/json" \
  "http://192.168.1.50:30850/api/v1/roles" \
  -d '{
    "name": "sales_analyst",
    "display_name": "Sales Analyst",
    "description": "Read-only access to the sales database in Doris",
    "permissions": [
      {"permission_id": 1, "resource_scope": {"database": "sales", "table": "*"}}
    ]
  }' | python3 -m json.tool
# Note the returned role ID

# Step 3: Bind the custom role to a user
rbacctl user bind alice sales_analyst
rbacctl sync run --user alice --service doris

# Verify the scoped GRANT
kubectl exec -n prod statefulset/doris-fe -c doris-fe -- \
  mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_ROOT_PASS}" \
  -e "SHOW GRANTS FOR 'alice'@'%';"
# Expected: GRANT SELECT_PRIV ON sales.* TO 'alice'@'%'
```

---

## Section 8 — Using the Swagger UI (user-friendly alternative to CLI)

For users who prefer a browser interface:

1. Open `http://192.168.1.50:30850/docs`
2. Click **Authorize** (top right) → enter `Bearer <your-RBAC_TOKEN>`
3. Use the interactive forms for every operation:
   - **Users** section → `POST /api/v1/users` → create user
   - **Users** section → `POST /api/v1/users/{username}/bindings` → bind role
   - **Sync** section → `POST /api/v1/sync` → push to services
   - **Audit** section → `GET /api/v1/audit` → view log

The `GET /api/v1/users/{username}/roles` endpoint is also useful for quick verification — it returns from the cache (sub-millisecond) and shows the full resolved permission set.

---

## Section 9 — Creating a Service-Specific API Token

For CI/CD pipelines that should only be able to bind roles (not delete them).

```bash
# Create a write-scoped token (no admin scope = cannot delete or sync)
rbacctl token create ci-pipeline --scopes read,write
# Output: shows the raw token — save it immediately

# Test the token has the right scope
export CI_TOKEN="<raw-token-from-above>"
curl -s -X POST \
  "http://192.168.1.50:30850/api/v1/auth/token?raw_token=${CI_TOKEN}" \
  | python3 -m json.tool
# Expected: JWT with scopes: ["read", "write"]

# Verify the CI token CANNOT delete users (needs admin scope)
CI_JWT=$(curl -s -X POST \
  "http://192.168.1.50:30850/api/v1/auth/token?raw_token=${CI_TOKEN}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl -s -X DELETE \
  -H "Authorization: Bearer ${CI_JWT}" \
  "http://192.168.1.50:30850/api/v1/users/alice"
# Expected: {"detail":"Scope(s) required: admin"} — 403
```

---

## Section 10 — Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `rbacctl services` → connection refused | RBAC plane not running | `kubectl get pods -n prod -l app=rbac-plane` — check logs |
| Sync status `error` for doris | Doris FE not reachable or wrong password | `kubectl logs -n prod -l app=rbac-plane` — check "Doris" adapter logs |
| `KafkaUser` CR stuck `NotReady` | Strimzi User Operator issue | `kubectl get kafkausers -n prod` + check operator logs |
| OpenSearch user exists but denied | Role mapping not applied | Re-run `rbacctl sync run --user alice --service opensearch` |
| Spark allowlist ConfigMap not updated | rbac-plane ServiceAccount missing ConfigMap permission | `kubectl auth can-i update configmaps -n prod --as=system:serviceaccount:prod:rbac-plane` |
| `RBAC_TOKEN: not set` | Env var missing | `export RBAC_TOKEN=$(kubectl get secret rbac-plane-credentials -n prod -o jsonpath='{.data.MASTER_TOKEN}' \| base64 -d)` |
| Cache returning stale permissions | Cache TTL not elapsed | Wait 30s or `rbacctl sync run --user alice` which invalidates cache |
| Doris: `Access denied — principal not in KDC` | KDC principal missing | Run Step 1 (Create KDC principal) first |
| Sync `skipped: no change` | State hasn't changed since last sync | Add/modify a binding then sync, or check `sync_state` table in PostgreSQL |

### Useful diagnostic commands

```bash
# Check RBAC plane logs
kubectl logs -n prod -l app=rbac-plane --tail=50 -f

# Check Redis is responding
kubectl exec -n prod deploy/redis -- redis-cli ping
# Expected: PONG

# Inspect the PostgreSQL rbac database
PG_PASS=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.PG_PASSWORD}' | base64 -d)
kubectl exec -n prod statefulset/postgresql -- \
  env PGPASSWORD="${PG_PASS}" psql -U rbac -d rbac \
  -c "SELECT u.username, r.name as role, s.name as service, b.expires_at
      FROM role_bindings b
      JOIN users u ON u.id = b.user_id
      JOIN roles r ON r.id = b.role_id
      LEFT JOIN services s ON s.id = b.service_id
      ORDER BY u.username, r.name;"

# Check sync state
kubectl exec -n prod statefulset/postgresql -- \
  env PGPASSWORD="${PG_PASS}" psql -U rbac -d rbac \
  -c "SELECT u.username, s.name, ss.synced_at
      FROM sync_state ss
      JOIN users u ON u.id = ss.user_id
      JOIN services s ON s.id = ss.service_id
      ORDER BY ss.synced_at DESC LIMIT 20;"

# Manually flush the Redis cache (forces DB lookup on next role check)
kubectl exec -n prod deploy/redis -- redis-cli FLUSHDB
```

---

## Quick Reference

```bash
# ── New user (full setup, one block) ──────────────────────
USERNAME="newuser"
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc ${USERNAME}@STARDATADBLABS.LOCAL"
rbacctl user create ${USERNAME}
rbacctl user bind   ${USERNAME} analyst
rbacctl sync run    --user ${USERNAME}
rbacctl user roles  ${USERNAME}

# ── Revoke all access ─────────────────────────────────────
USERNAME="olduser"
rbacctl user disable ${USERNAME}
rbacctl sync run    --user ${USERNAME}
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "delprinc -force ${USERNAME}@STARDATADBLABS.LOCAL"
rbacctl user delete ${USERNAME} --yes

# ── Full platform sync (run after bulk changes) ───────────
rbacctl sync run

# ── View all bindings across all users ───────────────────
curl -s -H "Authorization: Bearer ${RBAC_TOKEN}" \
  http://192.168.1.50:30850/api/v1/users | python3 -m json.tool
```
