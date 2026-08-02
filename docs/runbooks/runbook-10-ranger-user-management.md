# Runbook 10 — Apache Ranger: User Management, Policy Workflows & Testing

> **Ranger Admin UI:** `http://192.168.1.50:30680` · **Credentials:** `admin / Priya1982`  
> **Namespace:** `prod` · **Version:** Apache Ranger 2.7.0  
> **Related runbooks:** [08 — Security & Access](runbook-08-security-access.md)

---

## 1. Architecture Overview

Ranger acts as the **central policy engine** for all services in the platform.
Each service runs a Ranger plugin that polls Ranger Admin every 30 seconds and
caches policies locally. Authorization decisions are made **in-process** — no
network call per request.

```
Client Request
    │
    ▼
Service (Kafka / Doris / OpenSearch)
    │
    ▼
Ranger Plugin (in-process, on the service JVM)
    │
    ▼
Policy Cache (local — refreshed every 30s from Ranger Admin)
    │
    ▼
Allow / Deny  (sub-millisecond, no I/O on the hot path)
```

### Key Concepts

| Concept | Description |
|---|---|
| **Service** | A named plugin registration — one per data system (`kafka`, `doris`, `opensearch`) |
| **Policy** | Rule: resource + users/groups + allowed operations. Policies are *additive* — any matching allow wins |
| **Resource** | What is protected — Kafka topic/consumergroup/cluster, Doris database/table/column, OpenSearch index |
| **User** | Identity that must match exactly how the service reports it (SCRAM username for Kafka, SQL login for Doris) |
| **Group** | Collection of users. Adding a user to a group grants all the group's policy permissions automatically |
| **`public` group** | Special group — every Ranger user is implicitly a member. Policies on `public` apply to *all* users |

---

## 2. Registered Services

| Service ID | Name | Type | Plugin Active | Notes |
|---|---|---|---|---|
| 18 | `kafka` | kafka | ✅ Enforcing | Strimzi Kafka 4.2.0 broker |
| 13 | `doris` | hive | ✅ Enforcing | Apache Doris FE |
| 15 | `opensearch` | elasticsearch | ⏳ Pending | Service registered; plugin JAR not yet installed |

---

## 3. Current Policies

### Kafka (service id=18)

| Policy ID | Resource | Operations | Users / Groups |
|---|---|---|---|
| 112 | consumergroup: `*` | consume, describe, delete | 7 principals + `public` group |
| 113 | topic: `*` | publish, consume, configure, describe, create, delete, alter, alter_configs, describe_configs | 7 principals + `public` group |
| 114 | transactionalid: `*` | publish, describe | 7 principals + `public` group |
| 115 | cluster: `*` | configure, describe, kafka_admin, create, idempotent_write, alter, cluster_action, alter_configs, describe_configs | 7 principals + `public` group |
| 116 | delegationtoken: `*` | describe | 7 principals + `public` group |

> The 7 principals: `kafka-app-user`, `debezium-user`, `schema-registry-user`, and the four
> Strimzi TLS principals (`CN=strimzi-kafka-kafka,O=io.strimzi`, `CN=cluster-operator,O=io.strimzi`,
> `CN=strimzi-kafka-entity-topic-operator,O=io.strimzi`, `CN=strimzi-kafka-entity-user-operator,O=io.strimzi`).

> **Note:** Current policies are wide-open (`*` on all resources). This is the lab baseline.
> For production, replace `*` with specific topic names and create per-user scoped policies.

### Doris (service id=13)

| Policy ID | Resource | Operations | Users / Groups |
|---|---|---|---|
| 87 | global: `*` | all | `root` |
| 88 | database/table/column: `*` | all | `root`, `{OWNER}` |
| 89 | database/table: `*` | all | `root`, `{OWNER}` |
| 90 | database: `*` | all / create | `root` (all), `public` group (create) |
| 94 | default db, all tables/cols | create | `public` group |
| 95 | information_schema, all tables/cols | select | `public` group |

### OpenSearch (service id=15)

| Policy ID | Resource | Operations | Users / Groups |
|---|---|---|---|
| 97 | index: `*` | all, monitor, manage, read, write, create, delete | `admin` |

> OpenSearch Ranger plugin JAR is not installed — policies are defined but not enforced.
> OpenSearch access is currently controlled by the built-in security plugin (`internal_users.yml`).

---

## 4. Groups Reference

| Group Name | Intended Use |
|---|---|
| `public` | All users — base access level |
| `account_admin` | Platform administrators |
| `streaming_admin` | Kafka admin — full topic/consumer group management |
| `streaming_dev` | Kafka developers — produce/consume on assigned topics |
| `processing_admin` | Doris/Spark admin users |
| `processing_dev` | Doris/Spark developers — SELECT on assigned databases |
| `caching_admin` | Schema Registry admin |
| `caching_dev` | Schema Registry developers |

---

## 5. Adding a New User

Adding a user requires three steps: (a) create the service-level identity,
(b) register the user in Ranger, (c) add them to a policy or group.

### Step A — Register user in Ranger user store

**Via UI:**
1. Open `http://192.168.1.50:30680` → login as `admin / Priya1982`
2. Top menu: **Settings → Users / Groups / Roles**
3. Click **Add New User**
4. Fill: **Username** (must exactly match what the service reports), **Password** (Ranger UI only — not the service credential), **Role** = `ROLE_USER`
5. Optionally assign a Group
6. Click **Save**

**Via REST API:**
```bash
curl -su admin:Priya1982 \
  -X POST http://192.168.1.50:30680/service/xusers/secure/users \
  -H "Content-Type: application/json" \
  -d '{
    "name":          "alice",
    "password":      "RangerUIPass1!",
    "firstName":     "Alice",
    "lastName":      "Smith",
    "emailAddress":  "",
    "userRoleList":  ["ROLE_USER"],
    "groupNameList": ["streaming_dev"]
  }'
```

### Step B — Assign to a group (via UI)

1. **Settings → Users / Groups / Roles → Users**
2. Open the user → click the **Groups** tab → search for the group → Add

---

## 6. Kafka — User Workflow & Testing

### 6.1 Create the Kafka credential (KafkaUser CR)

Kafka uses SCRAM-SHA-512. Strimzi manages the credential — you declare the user
as a `KafkaUser` resource and Strimzi generates a random password stored in a K8s secret.

Add to `manifests/strimzi/kafka-cluster.yaml` (or a separate file):

```yaml
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaUser
metadata:
  name: alice
  namespace: prod
  labels:
    strimzi.io/cluster: strimzi-kafka
spec:
  authentication:
    type: scram-sha-512
```

```bash
# Apply and wait for Ready
kubectl apply -f manifests/strimzi/kafka-cluster.yaml
kubectl get kafkauser alice -n prod
# NAME    CLUSTER         AUTHENTICATION   READY
# alice   strimzi-kafka   scram-sha-512    True

# Get the generated password
ALICE_PASS=$(kubectl get secret alice -n prod -o jsonpath='{.data.password}' | base64 -d)
echo "Alice's Kafka password: $ALICE_PASS"
```

### 6.2 Register alice in Ranger

```bash
curl -su admin:Priya1982 \
  -X POST http://192.168.1.50:30680/service/xusers/secure/users \
  -H "Content-Type: application/json" \
  -d '{
    "name":         "alice",
    "password":     "RangerUIPass1!",
    "firstName":    "Alice",
    "userRoleList": ["ROLE_USER"],
    "groupNameList":["streaming_dev"]
  }'
```

### 6.3 Add alice to a Kafka policy

**Option A — Add to existing wildcard policies 112–116 (via UI):**
1. Ranger Admin → **Access Manager → kafka**
2. Click policy **"all - topic"** (ID 113)
3. In **Allow Conditions** → Users field → type `alice` → select → **Save**
4. Repeat for policies 112, 114, 115, 116

**Option B — Create a scoped topic policy (recommended for production):**
```bash
curl -su admin:Priya1982 \
  -X POST http://192.168.1.50:30680/service/plugins/policies \
  -H "Content-Type: application/json" \
  -d '{
    "service":   "kafka",
    "name":      "alice-orders-topic",
    "isEnabled": true,
    "resources": {
      "topic": {"values":["orders","orders.*"],"isExcludes":false,"isRecursive":false}
    },
    "policyItems": [{
      "users":  ["alice"],
      "groups": [],
      "accesses": [
        {"type":"publish",  "isAllowed":true},
        {"type":"consume",  "isAllowed":true},
        {"type":"describe", "isAllowed":true}
      ],
      "conditions":    [],
      "delegateAdmin": false
    }]
  }'
```

### 6.4 Test: produce messages

```bash
ALICE_PASS=$(kubectl get secret alice -n prod -o jsonpath='{.data.password}' | base64 -d)

kubectl run kafka-test -n prod --rm -it --restart=Never \
  --image=192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0-ranger-v6 \
  -- bash -c "
cat > /tmp/client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username=\"alice\" password=\"${ALICE_PASS}\";
EOF

echo 'hello-from-alice' | /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092 \
  --topic orders \
  --producer.config /tmp/client.properties
echo 'Exit code: '$?
"
# Expected: exit code 0 (allowed) or error if not in policy
```

### 6.5 Test: consume messages

```bash
ALICE_PASS=$(kubectl get secret alice -n prod -o jsonpath='{.data.password}' | base64 -d)

kubectl run kafka-consume -n prod --rm -it --restart=Never \
  --image=192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0-ranger-v6 \
  -- bash -c "
cat > /tmp/client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username=\"alice\" password=\"${ALICE_PASS}\";
EOF

/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092 \
  --topic orders \
  --consumer.config /tmp/client.properties \
  --from-beginning \
  --timeout-ms 5000
"
# Expected: messages printed (allowed) or
# WARN: Not authorized to read from topic orders (denied)
```

### 6.6 Test: external access (NodePort 30093)

```bash
ALICE_PASS=$(kubectl get secret alice -n prod -o jsonpath='{.data.password}' | base64 -d)

cat > /tmp/client-ext.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="alice" password="${ALICE_PASS}";
EOF

# From any host that can reach 192.168.1.54:
kafka-console-producer.sh \
  --bootstrap-server 192.168.1.54:30093 \
  --topic orders \
  --producer.config /tmp/client-ext.properties
```

### 6.7 Check Ranger audit logs

1. Ranger Admin → **Audit → Access**
2. Filter: **Service Name** = `kafka`, **User** = `alice`
3. Each row shows: timestamp, user, resource, operation, result (Allow / Deny)

---

## 7. Doris — User Workflow & Testing

### 7.1 Create the Doris SQL user

```bash
DORIS_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)

kubectl exec -n prod deploy/doris-fe -it -- \
  mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_PASS}" \
  -e "CREATE USER 'alice'@'%' IDENTIFIED BY 'AliceDoris1!';"
```

> In Ranger-managed Doris, you still need `CREATE USER` in Doris, but permissions
> are controlled by Ranger policies. The Doris-native privilege check is overridden
> by the Ranger plugin — so no additional `GRANT` in SQL is required.

### 7.2 Register alice in Ranger

```bash
curl -su admin:Priya1982 \
  -X POST http://192.168.1.50:30680/service/xusers/secure/users \
  -H "Content-Type: application/json" \
  -d '{
    "name":         "alice",
    "password":     "RangerUIPass1!",
    "firstName":    "Alice",
    "userRoleList": ["ROLE_USER"],
    "groupNameList":["processing_dev"]
  }'
```

### 7.3 Create a scoped Doris policy

```bash
curl -su admin:Priya1982 \
  -X POST http://192.168.1.50:30680/service/plugins/policies \
  -H "Content-Type: application/json" \
  -d '{
    "service":   "doris",
    "name":      "alice-analytics-select",
    "isEnabled": true,
    "resources": {
      "database": {"values":["analytics"],  "isExcludes":false,"isRecursive":false},
      "table":    {"values":["*"],           "isExcludes":false,"isRecursive":false},
      "column":   {"values":["*"],           "isExcludes":false,"isRecursive":false}
    },
    "policyItems": [{
      "users":  ["alice"],
      "groups": [],
      "accesses": [
        {"type":"select","isAllowed":true}
      ],
      "conditions":    [],
      "delegateAdmin": false
    }]
  }'
```

### 7.4 Test: SELECT access (should be allowed)

```bash
kubectl run doris-test -n prod --rm -it --restart=Never \
  --image=mysql:8.0 \
  -- mysql -h doris-fe.prod.svc.cluster.local -P9030 \
     -u alice -p'AliceDoris1!' \
     -e "SELECT * FROM analytics.orders LIMIT 5;"
# Expected: rows returned
# Denied:   ERROR 1105 (HY000): Access denied
```

### 7.5 Test: INSERT access (should be denied)

```bash
kubectl run doris-test -n prod --rm -it --restart=Never \
  --image=mysql:8.0 \
  -- mysql -h doris-fe.prod.svc.cluster.local -P9030 \
     -u alice -p'AliceDoris1!' \
     -e "INSERT INTO analytics.orders VALUES (999, 'test');"
# Expected: ERROR 1105 (HY000): Access denied
```

### 7.6 Add column-level data masking (optional)

1. Ranger Admin → **Access Manager → doris**
2. Click the **Masking** tab → **Add New Policy**
3. Resource: `database=analytics`, `table=customers`, `column=email`
4. User: `alice` → Masking Option: **MASK** (replaces value with `xxxx`)
5. **Save** → takes effect within 30 seconds

---

## 8. OpenSearch — Current State & Pending Work

OpenSearch Ranger authorization requires the `ranger-opensearch-plugin` JAR
installed inside the OpenSearch image and configured in the security plugin's
`config.yml` as an `authz` backend. The `plugins.security.authorization.ranger.*`
settings seen in some guides are **not** native OpenSearch settings — they cause a
`SettingsException` at startup if placed in `opensearch.yml`.

**What is active today:**
- OpenSearch service registered in Ranger (id=15)
- Policy 97: all-index → `admin` user, all operations (defined but not enforced)
- Access controlled by OpenSearch built-in security plugin (`internal_users.yml`)

**Current OpenSearch credentials:**
- Username: `admin` / Password: `admin` (stored in K8s secret `opensearch-credentials`)

**To add a user to OpenSearch today** (without Ranger):
```bash
# Use securityadmin.sh inside the pod — update internal_users.yml
# or use the OpenSearch Security REST API:
curl -su admin:admin \
  -X PUT "http://192.168.1.50:30920/_plugins/_security/api/internalusers/alice" \
  -H "Content-Type: application/json" \
  -d '{
    "password":        "AliceOS1!",
    "backend_roles":   ["readall"],
    "attributes":      {}
  }'
```

**Ranger integration for OpenSearch** — planned future task:
1. Build custom OpenSearch image with `ranger-opensearch-plugin-2.7.0.jar`
2. Configure `config.yml` authz backend to point at Ranger Admin
3. Add users/policies in Ranger for index-level access control

---

## 9. Deleting a User — Full Cleanup

> **Order matters:** remove from policies first, then delete the service identity,
> then delete from Ranger. Reversing this order leaves dangling policy references.

### Step 1 — Find all policies referencing the user

```bash
curl -su admin:Priya1982 \
  "http://192.168.1.50:30680/service/plugins/policies?pageSize=500" | \
  python3 -c "
import sys, json
d = json.load(sys.stdin)
for p in d.get('policies', []):
    for item in p.get('policyItems', []) + p.get('denyPolicyItems', []):
        if 'alice' in item.get('users', []):
            print(f'Policy ID={p[\"id\"]}  name={p[\"name\"]}  service={p[\"service\"]}')
"
```

### Step 2 — Remove user from each policy

**Via UI:** Open each policy → Allow Conditions → click ✕ next to `alice` → Save

**Via REST API (patch a policy):**
```bash
# GET the policy first, edit the users array, then PUT it back
POLICY_ID=113
curl -su admin:Priya1982 \
  "http://192.168.1.50:30680/service/plugins/policies/${POLICY_ID}" | \
  python3 -c "
import sys, json
p = json.load(sys.stdin)
for item in p.get('policyItems', []):
    item['users'] = [u for u in item.get('users', []) if u != 'alice']
print(json.dumps(p))
" > /tmp/policy-patched.json

curl -su admin:Priya1982 \
  -X PUT "http://192.168.1.50:30680/service/plugins/policies/${POLICY_ID}" \
  -H "Content-Type: application/json" \
  -d @/tmp/policy-patched.json
```

### Step 3 — Delete the service-level identity

**Kafka:**
```bash
# Deletes the KafkaUser CR and the K8s secret holding the SCRAM credential
kubectl delete kafkauser alice -n prod
kubectl get secret alice -n prod   # should return: NotFound
```

**Doris:**
```bash
DORIS_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
kubectl exec -n prod deploy/doris-fe -it -- \
  mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_PASS}" \
  -e "DROP USER 'alice'@'%';"
```

**OpenSearch (built-in security):**
```bash
curl -su admin:admin \
  -X DELETE "http://192.168.1.50:30920/_plugins/_security/api/internalusers/alice"
```

### Step 4 — Delete the Ranger user

```bash
# Find the Ranger user ID
RANGER_USER_ID=$(curl -su admin:Priya1982 \
  "http://192.168.1.50:30680/service/xusers/users?pageSize=100" | \
  python3 -c "
import sys, json
for u in json.load(sys.stdin)['vXUsers']:
    if u['name'] == 'alice':
        print(u['id'])
")

echo "Ranger user ID: ${RANGER_USER_ID}"

# Delete the user
curl -su admin:Priya1982 \
  -X DELETE \
  "http://192.168.1.50:30680/service/xusers/users/${RANGER_USER_ID}?forceDelete=true"
```

### Step 5 — Verify cleanup

```bash
curl -su admin:Priya1982 \
  "http://192.168.1.50:30680/service/plugins/policies?pageSize=500" | \
  python3 -c "
import sys, json
raw = sys.stdin.read()
if 'alice' in raw:
    print('WARNING: alice still referenced in policies — check manually')
else:
    print('OK — alice fully removed from all policies')
"
```

---

## 10. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| User denied even though policy exists | Policy not yet synced — plugin polls every 30s | Wait 30 seconds. Check broker logs: `kubectl logs -n prod strimzi-kafka-combined-0 --tail=100 \| grep "policy version"` |
| Policy version stays at `-1` | Plugin can't download policies — wrong auth credentials or user not in `policy.download.auth.users` | Check `ranger-kafka-security.xml` credentials in ConfigMap `kafka-ranger-config`. Verify the plugin auth user (`kafka-app-user`) exists in Ranger |
| Kafka: everyone allowed without a policy | `allow.everyone.if.no.acl.found=true` in broker config | Expected for lab. For production set to `false` in `kafka-cluster.yaml` and commit |
| KafkaUser stuck `NotReady` | AclCache error (old image) | Ensure image is `ranger-v6` or later. Check user-operator logs: `kubectl logs -n prod deploy/strimzi-kafka-entity-operator -c user-operator --tail=50` |
| Doris: Ranger policy exists but still denied | Doris Ranger plugin cache stale | Restart Doris FE pod: `kubectl rollout restart deploy/doris-fe -n prod` |
| Ranger Admin pod not starting | PostgreSQL connection failed | `kubectl logs -n prod deploy/ranger-admin --tail=50`. Check secret `ranger-db-credentials` |
| Audit logs not visible in UI | Audit endpoint not configured | Ranger → Audit → Access. Logs use `internal_opensearch` backend. If OpenSearch is down, audit writes may fail silently |

### Useful debugging commands

```bash
# Check all Ranger services
curl -su admin:Priya1982 \
  "http://192.168.1.50:30680/service/plugins/services?pageSize=100" | \
  python3 -c "import sys,json; [print(s['id'],s['name'],s['type'],s['isEnabled']) \
    for s in json.load(sys.stdin)['services']]"

# Check all users in Ranger
curl -su admin:Priya1982 \
  "http://192.168.1.50:30680/service/xusers/users?pageSize=100" | \
  python3 -c "import sys,json; [print(u['id'],u['name'],u.get('groupNameList',[])) \
    for u in json.load(sys.stdin)['vXUsers']]"

# Check Kafka policy sync on broker
kubectl logs -n prod strimzi-kafka-combined-0 --tail=200 | \
  grep -E "policy version|PolicyRefresher|Ranger"

# Check Ranger pod health
kubectl get pods -n prod -l app=ranger-admin
kubectl logs -n prod deploy/ranger-admin --tail=50 | grep -E "ERROR|WARN|Started"

# Force Kafka broker to reload policies (last resort)
kubectl rollout restart statefulset/strimzi-kafka-combined -n prod
```

---

## 11. Quick Reference

### Ranger Admin REST base URL
```
http://192.168.1.50:30680
```

### Internal service URL (from within the cluster)
```
http://ranger-admin.prod.svc.cluster.local:6080
```

### Plugin poll interval
All plugins poll every **30 seconds**. Policy changes take effect within 30s.

### Service IDs
| Service | ID |
|---|---|
| `kafka` | 18 |
| `doris` | 13 |
| `opensearch` | 15 |

### Bootstrap credentials stored in K8s secrets

| Secret | Namespace | Key |
|---|---|---|
| `ranger-db-credentials` | `prod` | `admin-password` |
| `kafka-app-user` | `prod` | `password` (Kafka plugin auth) |
| `opensearch-credentials` | `prod` | `opensearch-password` |
| `doris-credentials` | `prod` | `admin-password` |
