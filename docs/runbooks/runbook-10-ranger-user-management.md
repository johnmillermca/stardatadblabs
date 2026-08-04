# Runbook 10 — Ranger User Management, Kerberos Toggle & Per-Service Testing

> **Ranger Admin UI:** `http://192.168.1.50:30680` · **Credentials:** `admin / Priya1982`
> **Namespace:** `prod` · **Ranger version:** 2.7.0 · **Realm:** `STARDATADBLABS.LOCAL`
> **Related runbooks:** [01 — OpenBao](runbook-01-openbao.md) · [11 — Kerberos Integration](runbook-11-kerberos-integration.md)

---

## 1. Architecture Overview

Every request passes through three independent security layers:

```
┌─────────────────────────────────────────────────────────────────────┐
│  Layer 1 — Kerberos KDC  (Authentication — who are you?)            │
│  Realm: STARDATADBLABS.LOCAL                                        │
│  KDC:   kerberos-kdc.prod.svc.cluster.local:88                      │
│  Used for: Spark GSSAPI, OpenSearch SPNEGO                          │
│  NOT used for: Kafka SCRAM, Doris SQL password                      │
├─────────────────────────────────────────────────────────────────────┤
│  Layer 2 — OpenBao  (Secret Manager)                                │
│  Stores all credentials: Ranger admin, SCRAM passwords, Doris       │
│  passwords, Kerberos keytab metadata.                               │
│  UI: http://192.168.1.50:30820                                      │
├─────────────────────────────────────────────────────────────────────┤
│  Layer 3 — Apache Ranger  (Authorization — what can you do?)        │
│  Answers every access decision for Kafka, Doris, OpenSearch, Spark. │
│  Plugin polls every 30s — decisions made in-process, zero I/O.     │
│  Admin: http://192.168.1.50:30680                                   │
└─────────────────────────────────────────────────────────────────────┘
```

**Identity namespace — the golden rule:**
Ranger username = KDC principal name (realm stripped) = Doris SQL username = Kafka SCRAM username.
All four are the same string: `alice`. One name governs access everywhere.

```
KDC principal:      alice@STARDATADBLABS.LOCAL
Ranger username:    alice          ← strip_realm_from_principal=true
Kafka SCRAM user:   alice          ← KafkaUser CR name
Doris SQL user:     alice          ← CREATE USER 'alice'@'%'
OpenSearch user:    alice          ← internal_users or SPNEGO identity
```

---

## 2. Kerberos Platform Toggle

Kerberos client authentication is controlled by a **single ConfigMap toggle**.
Changing it requires restarting the affected services — it is not a hot-switch.

### 2.1 Check current state

```bash
kubectl get cm kerberos-integration-config -n prod \
  -o jsonpath='{.data.kerberos\.enabled}' && echo
# "true"  = Kerberos authentication active on OpenSearch (SPNEGO) and Spark
# "false" = Kerberos disabled; services use SCRAM / SQL password / basic auth
```

### 2.2 Enable Kerberos (platform-wide)

**Current state: `kerberos.enabled = "true"` — already enabled.**

If it has been disabled and you want to re-enable:

```bash
# Step 1 — flip the ConfigMap toggle
kubectl patch cm kerberos-integration-config -n prod \
  --type merge -p '{"data":{"kerberos.enabled":"true"}}'

# Step 2 — restart Spark (krb5.conf + keytab already mounted)
kubectl rollout restart deploy/spark-master deploy/spark-worker -n prod
kubectl rollout status deploy/spark-master -n prod --timeout=120s

# Step 3 — restart Doris FE (krb5.conf + doris-keytab pre-staged)
kubectl rollout restart statefulset/doris-fe -n prod
kubectl rollout status statefulset/doris-fe -n prod --timeout=120s

# Step 4 — re-apply OpenSearch SPNEGO config via securityadmin.sh
#   (see §2.4 below for full commands)

# Step 5 — enable Ranger SPNEGO
#   In manifests/ranger/ranger-deployment.yaml set:
#     ranger.spnego.kerberos.enabled = 'true'
#   Commit, push → ArgoCD reconciles → Ranger restarts

# Step 6 — enable Kafka GSSAPI listener (optional — see §2.5)
```

### 2.3 Disable Kerberos (platform-wide)

```bash
# Step 1 — flip the toggle
kubectl patch cm kerberos-integration-config -n prod \
  --type merge -p '{"data":{"kerberos.enabled":"false"}}'

# Step 2 — restart Spark and Doris (krb5.conf still mounted but harmless)
kubectl rollout restart deploy/spark-master deploy/spark-worker -n prod
kubectl rollout restart statefulset/doris-fe -n prod

# Step 3 — disable OpenSearch SPNEGO via securityadmin.sh
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  mkdir -p /tmp/backup-secconfig
  /usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh \
    -backup /tmp/backup-secconfig -icl -nhnv \
    -cacert /usr/share/opensearch/config/tls/root-ca.pem \
    -cert   /usr/share/opensearch/config/tls/admin.pem \
    -key    /usr/share/opensearch/config/tls/admin-key.pem \
    -h localhost -p 9200 2>/dev/null

  python3 - <<'PYEOF'
import yaml
with open('/tmp/backup-secconfig/config.yml') as f:
    doc = yaml.safe_load(f)
doc['config']['dynamic']['authc']['kerberos_auth_domain']['http_enabled'] = False
with open('/tmp/backup-secconfig/config.yml', 'w') as f:
    yaml.dump(doc, f, default_flow_style=False)
print('Kerberos disabled in config')
PYEOF

  /usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh \
    -f /tmp/backup-secconfig/config.yml -t config -icl -nhnv \
    -cacert /usr/share/opensearch/config/tls/root-ca.pem \
    -cert   /usr/share/opensearch/config/tls/admin.pem \
    -key    /usr/share/opensearch/config/tls/admin-key.pem \
    -h localhost -p 9200 2>&1 | tail -5
"

# Step 4 — disable Ranger SPNEGO (optional)
#   In manifests/ranger/ranger-deployment.yaml set:
#     ranger.spnego.kerberos.enabled = 'false'
#   Commit, push → ArgoCD reconciles

# Step 5 — disable Kafka GSSAPI listener
#   Comment out the KRB listener block in kafka-cluster.yaml (see §2.5)
```

### 2.4 OpenSearch — apply Kerberos config change

OpenSearch security config lives in a distributed index, not a file.
Changes require `securityadmin.sh` — a pod restart alone is not enough.

```bash
# Enable SPNEGO
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  mkdir -p /tmp/backup-secconfig
  /usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh \
    -backup /tmp/backup-secconfig -icl -nhnv \
    -cacert /usr/share/opensearch/config/tls/root-ca.pem \
    -cert   /usr/share/opensearch/config/tls/admin.pem \
    -key    /usr/share/opensearch/config/tls/admin-key.pem \
    -h localhost -p 9200 2>/dev/null

  python3 - <<'PYEOF'
import yaml
with open('/tmp/backup-secconfig/config.yml') as f:
    doc = yaml.safe_load(f)
krb = doc['config']['dynamic']['authc']['kerberos_auth_domain']
krb['http_enabled'] = True
krb['order'] = 1
if 'config' not in krb['http_authenticator']:
    krb['http_authenticator']['config'] = {}
krb['http_authenticator']['config']['krb_debug'] = False
krb['http_authenticator']['config']['strip_realm_from_principal'] = True
krb['http_authenticator']['config']['krb_service_principal'] = 'svc/opensearch@STARDATADBLABS.LOCAL'
krb['http_authenticator']['config']['krb_keytab_path'] = '/etc/security/keytabs/opensearch.service.keytab'
with open('/tmp/backup-secconfig/config.yml', 'w') as f:
    yaml.dump(doc, f, default_flow_style=False)
print('Kerberos SPNEGO enabled')
PYEOF

  /usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh \
    -f /tmp/backup-secconfig/config.yml -t config -icl -nhnv \
    -cacert /usr/share/opensearch/config/tls/root-ca.pem \
    -cert   /usr/share/opensearch/config/tls/admin.pem \
    -key    /usr/share/opensearch/config/tls/admin-key.pem \
    -h localhost -p 9200 2>&1 | tail -5
"
# Expected last line: Done with success
```

### 2.5 Kafka — enable GSSAPI listener (port 9093)

The GSSAPI listener is **commented out** in [`manifests/strimzi/kafka-cluster.yaml`](../../manifests/strimzi/kafka-cluster.yaml).
SCRAM listeners on ports 9092/9094 are always active and unaffected.

**To enable — uncomment three blocks in `kafka-cluster.yaml`:**

1. The `#- name: krb` listener block in `spec.kafka.listeners`
2. The `#- name: kafka-keytab` and `#- name: kafka-jaas` volume entries in `KafkaNodePool.template.pod.volumes`
3. The matching `#- name: kafka-keytab` and `#- name: kafka-jaas` entries in `kafkaContainer.volumeMounts`
4. Add `-Djava.security.auth.login.config=/mnt/kafka-jaas/jaas.conf` to the `KAFKA_OPTS` env var

Then commit, push → ArgoCD syncs → Strimzi performs a rolling restart (~3 min).

**To disable — comment those same blocks back out, commit, push.**

Verify the listener is up after restart:
```bash
kubectl get kafka strimzi-kafka -n prod \
  -o jsonpath='{.status.listeners[*].name}' && echo
# Expected when enabled: plain external krb
kubectl logs -n prod strimzi-kafka-combined-0 | grep -i "9093\|KRB\|GSSAPI" | tail -5
```

---

## 3. Registered Services & Current Policies

### Services

| Service ID | Name | Type | Enforcing | Auth mechanism |
|---|---|---|---|---|
| 18 | `kafka` | kafka | ✅ Yes | SCRAM-SHA-512 (+ GSSAPI when KRB listener enabled) |
| 13 | `doris` | hive | ✅ Yes | SQL password (Kerberos principal maps to same username) |
| 15 | `opensearch` | elasticsearch | ⚠️ Partial | SPNEGO active; Ranger authz plugin JAR not yet installed |

### Kafka policies (service id=18)

| ID | Resource | Operations | Who |
|---|---|---|---|
| 112 | consumergroup: `*` | consume, describe, delete | 7 principals + `public` |
| 113 | topic: `*` | publish, consume, describe, create, delete, alter … | 7 principals + `public` |
| 114 | transactionalid: `*` | publish, describe | 7 principals + `public` |
| 115 | cluster: `*` | configure, describe, kafka_admin, create … | 7 principals + `public` |
| 116 | delegationtoken: `*` | describe | 7 principals + `public` |

> Current policies are wide-open (`*`). For production scope to specific topic names.

### Doris policies (service id=13)

| ID | Resource | Operations | Who |
|---|---|---|---|
| 87 | global: `*` | all | `root` |
| 88–89 | database/table/column: `*` | all | `root`, `{OWNER}` |
| 90 | database: `*` | all / create | `root`, `public` |
| 94 | default db, all tables | create | `public` |
| 95 | information_schema | select | `public` |

### OpenSearch (service id=15)

| ID | Resource | Operations | Who |
|---|---|---|---|
| 97 | index: `*` | all | `admin` |

> Ranger authz plugin JAR not installed — policy evaluations use the OpenSearch built-in security engine. Adding a user to Ranger creates the identity record but does not yet enforce index-level policy via Ranger.

---

## 4. Groups Reference

| Group | Services | Access level |
|---|---|---|
| `public` | All | Base level — every Ranger user is a member |
| `streaming_admin` | Kafka | Full topic + consumer group management |
| `streaming_dev` | Kafka | Produce/consume on assigned topics |
| `processing_admin` | Doris, Spark | Full database management |
| `processing_dev` | Doris, Spark | SELECT on assigned databases |
| `caching_admin` | Schema Registry | Admin |
| `caching_dev` | Schema Registry | Read schemas |
| `account_admin` | All | Platform administrators |

---

## 5. Adding a New User — Complete End-to-End Walkthrough

This section walks through creating `alice` and verifying her access in every service.
Replace `alice` / `AliceKrb1!` / `AliceDoris1!` with the real username and passwords.

> **OpenBao note:** The `bao` CLI is inside the `openbao-0` pod only.
> All operations from `master.local` use `curl`. KV v2 paths need `/data/` prefix.

### Step 1 — Create the Kerberos principal

Every user starts here. This creates the single identity that flows through all services.

```bash
# Create the principal (prompts for password interactively)
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc alice@STARDATADBLABS.LOCAL"

# OR non-interactive with a temporary password
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc -pw AliceKrb1! alice@STARDATADBLABS.LOCAL"

# Verify it was created
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "getprinc alice@STARDATADBLABS.LOCAL" 2>/dev/null | \
  grep -E "Principal|Expiration|Last pwd"
```

### Step 2 — Export keytab and store as K8s secret

Keytabs are required for non-interactive Spark jobs and OpenSearch SPNEGO.

```bash
# Export keytab inside the KDC pod
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "ktadd -k /tmp/alice.keytab alice@STARDATADBLABS.LOCAL"

# Copy to master
KDC_POD=$(kubectl get pod -n prod -l app=kerberos-kdc \
  -o jsonpath='{.items[0].metadata.name}')
kubectl cp prod/${KDC_POD}:/tmp/alice.keytab /tmp/alice.keytab

# Verify keytab is valid (check for header bytes and realm)
kubectl get secret -n prod alice-keytab 2>/dev/null && echo "secret already exists" || \
  kubectl create secret generic alice-keytab \
    --from-file=keytab=/tmp/alice.keytab -n prod

# Clean up temp files
kubectl exec -n prod deploy/kerberos-kdc -- rm -f /tmp/alice.keytab
rm -f /tmp/alice.keytab
```

### Step 3 — Create the Kafka SCRAM user

Kafka uses SCRAM-SHA-512 — Strimzi generates and manages the credential.

```bash
kubectl apply -f - <<'EOF'
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
EOF

# Wait for Strimzi to provision the SCRAM credential
kubectl wait kafkauser alice -n prod --for=condition=Ready --timeout=60s

# Retrieve the generated password (needed for testing)
ALICE_KAFKA_PASS=$(kubectl get secret alice -n prod \
  -o jsonpath='{.data.password}' | base64 -d)
echo "Kafka SCRAM password: ${ALICE_KAFKA_PASS}"
```

### Step 4 — Create the Doris SQL user

Doris uses SQL authentication. The username must match the Kerberos short name (`alice`).

```bash
BAO_ADDR="http://192.168.1.50:30820"
KEYS_FILE="${HOME}/openbao-init-keys.json"
[ -f "${KEYS_FILE}" ] || KEYS_FILE="/root/openbao-init-keys.json"
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${KEYS_FILE}'))['root_token'])")

DORIS_ROOT_PASS=$(curl -sf \
  -H "X-Vault-Token: ${ROOT_TOKEN}" \
  "${BAO_ADDR}/v1/secret/data/doris/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

kubectl exec -n prod statefulset/doris-fe -it -- \
  mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_ROOT_PASS}" \
  -e "CREATE USER 'alice'@'%' IDENTIFIED BY 'AliceDoris1!';"

# Verify
kubectl exec -n prod statefulset/doris-fe -it -- \
  mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_ROOT_PASS}" \
  -e "SELECT user, host FROM mysql.user WHERE user='alice';"
```

### Step 5 — Create the OpenSearch user

```bash
OPENSEARCH_PASS=$(kubectl get secret opensearch-credentials -n prod \
  -o jsonpath='{.data.opensearch-password}' | base64 -d)

curl -sk -u "admin:${OPENSEARCH_PASS}" \
  -X PUT "https://192.168.1.53:30920/_plugins/_security/api/internalusers/alice" \
  -H "Content-Type: application/json" \
  -d '{
    "password":      "AliceOS1!",
    "backend_roles": ["readall"],
    "attributes":    {}
  }'
# Expected: {"status":"CREATED","message":"User alice has been created."}
```

### Step 6 — Store all credentials in OpenBao

Single source of truth for all of alice's credentials across every service.

```bash
BAO_ADDR="http://192.168.1.50:30820"
KEYS_FILE="${HOME}/openbao-init-keys.json"
[ -f "${KEYS_FILE}" ] || KEYS_FILE="/root/openbao-init-keys.json"
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${KEYS_FILE}'))['root_token'])")

# Kafka SCRAM password (mirror from K8s secret)
ALICE_KAFKA_PASS=$(kubectl get secret alice -n prod \
  -o jsonpath='{.data.password}' | base64 -d)
curl -sf -X POST \
  -H "X-Vault-Token: ${ROOT_TOKEN}" -H "Content-Type: application/json" \
  -d "{\"data\":{\"username\":\"alice\",\"password\":\"${ALICE_KAFKA_PASS}\",\"service\":\"kafka\",\"created_by\":\"admin\"}}" \
  "${BAO_ADDR}/v1/secret/data/kafka/users/alice" && echo "Kafka stored"

# Doris SQL password
curl -sf -X POST \
  -H "X-Vault-Token: ${ROOT_TOKEN}" -H "Content-Type: application/json" \
  -d '{"data":{"username":"alice","password":"AliceDoris1!","service":"doris","created_by":"admin"}}' \
  "${BAO_ADDR}/v1/secret/data/doris/users/alice" && echo "Doris stored"

# OpenSearch password
curl -sf -X POST \
  -H "X-Vault-Token: ${ROOT_TOKEN}" -H "Content-Type: application/json" \
  -d '{"data":{"username":"alice","password":"AliceOS1!","service":"opensearch","created_by":"admin"}}' \
  "${BAO_ADDR}/v1/secret/data/opensearch/users/alice" && echo "OpenSearch stored"

# Kerberos principal metadata
curl -sf -X POST \
  -H "X-Vault-Token: ${ROOT_TOKEN}" -H "Content-Type: application/json" \
  -d '{"data":{"principal":"alice@STARDATADBLABS.LOCAL","keytab_secret":"alice-keytab","created_by":"admin"}}' \
  "${BAO_ADDR}/v1/secret/data/kerberos/users/alice" && echo "Kerberos stored"
```

### Step 7 — Register in Ranger and assign groups

One Ranger user record covers all services. The username `alice` must match exactly.

```bash
BAO_ADDR="http://192.168.1.50:30820"
KEYS_FILE="${HOME}/openbao-init-keys.json"
[ -f "${KEYS_FILE}" ] || KEYS_FILE="/root/openbao-init-keys.json"
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${KEYS_FILE}'))['root_token'])")
RANGER_PASS=$(curl -sf \
  -H "X-Vault-Token: ${ROOT_TOKEN}" \
  "${BAO_ADDR}/v1/secret/data/ranger/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

# Register alice in Ranger
# groupNameList controls which pre-existing group policies apply immediately:
#   streaming_dev  → inherits Kafka produce/consume policies
#   processing_dev → inherits Doris SELECT policies
curl -su "admin:${RANGER_PASS}" \
  -X POST http://192.168.1.50:30680/service/xusers/secure/users \
  -H "Content-Type: application/json" \
  -d '{
    "name":          "alice",
    "password":      "RangerUIPass1!",
    "firstName":     "Alice",
    "lastName":      "Smith",
    "userRoleList":  ["ROLE_USER"],
    "groupNameList": ["streaming_dev","processing_dev"]
  }'

# Verify alice appears in Ranger
curl -su "admin:${RANGER_PASS}" \
  "http://192.168.1.50:30680/service/xusers/users?pageSize=100" | \
  python3 -c "
import sys, json
for u in json.load(sys.stdin)['vXUsers']:
    if u['name'] == 'alice':
        print('Found:', u['id'], u['name'], u.get('groupNameList',[]))
"
```

**What alice can do after group assignment (before any scoped policies):**

| Service | Access from group membership |
|---|---|
| Kafka | `streaming_dev` → produce/consume on topics covered by group policies |
| Doris | `processing_dev` → SELECT on databases covered by group policies |
| OpenSearch | No Ranger authz yet — access via `readall` backend role in internal users |
| Spark | No cluster-level auth — job-level Kerberos keytab governs data source access |

---

## 6. Kafka — Testing Alice's Access

### 6.1 Verify Ranger recognises alice for Kafka

```bash
RANGER_PASS=$(curl -sf \
  -H "X-Vault-Token: $(python3 -c "import json; print(json.load(open('${HOME}/openbao-init-keys.json'))['root_token'])")" \
  "http://192.168.1.50:30820/v1/secret/data/ranger/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

# List all kafka policies that include alice (directly or via group)
curl -su "admin:${RANGER_PASS}" \
  "http://192.168.1.50:30680/service/plugins/policies?serviceName=kafka&pageSize=100" | \
  python3 -c "
import sys, json
d = json.load(sys.stdin)
for p in d.get('policies', []):
    users = [u for item in p.get('policyItems',[]) for u in item.get('users',[])]
    groups = [g for item in p.get('policyItems',[]) for g in item.get('groups',[])]
    if 'alice' in users or 'streaming_dev' in groups or 'public' in groups:
        print(f'  Policy {p[\"id\"]:>4}  {p[\"name\"]:<35}  users={users}  groups={groups}')
"
```

### 6.2 Create a scoped Kafka policy for alice (optional)

By default alice inherits `streaming_dev` group policies.
To grant access to a specific topic only:

```bash
curl -su "admin:${RANGER_PASS}" \
  -X POST http://192.168.1.50:30680/service/plugins/policies \
  -H "Content-Type: application/json" \
  -d '{
    "service":   "kafka",
    "name":      "alice-orders-rw",
    "isEnabled": true,
    "resources": {
      "topic": {"values":["orders","orders.*"],"isExcludes":false,"isRecursive":false}
    },
    "policyItems": [{
      "users":       ["alice"],
      "groups":      [],
      "accesses":    [{"type":"publish","isAllowed":true},
                      {"type":"consume","isAllowed":true},
                      {"type":"describe","isAllowed":true}],
      "conditions":  [],
      "delegateAdmin": false
    }]
  }'
```

### 6.3 Test: produce (SCRAM-SHA-512)

```bash
ALICE_KAFKA_PASS=$(kubectl get secret alice -n prod \
  -o jsonpath='{.data.password}' | base64 -d)

kubectl run kafka-test -n prod --rm -it --restart=Never \
  --image=192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0-ranger-v6 \
  -- bash -c "
cat > /tmp/client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username=\"alice\" password=\"${ALICE_KAFKA_PASS}\";
EOF
echo 'hello-from-alice' | /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092 \
  --topic orders --producer.config /tmp/client.properties
echo Exit: \$?
"
# Expected: exit 0 → Ranger allowed
# Denied:   WARN [Producer clientId=…] Got error produce response with correlation id … ERROR_CODE=29 (NOT_AUTHORIZED)
```

### 6.4 Test: consume (SCRAM-SHA-512)

```bash
ALICE_KAFKA_PASS=$(kubectl get secret alice -n prod \
  -o jsonpath='{.data.password}' | base64 -d)

kubectl run kafka-consume -n prod --rm -it --restart=Never \
  --image=192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0-ranger-v6 \
  -- bash -c "
cat > /tmp/client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username=\"alice\" password=\"${ALICE_KAFKA_PASS}\";
EOF
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092 \
  --topic orders --consumer.config /tmp/client.properties \
  --from-beginning --max-messages 5 --timeout-ms 10000
"
# Expected: messages printed or "[5 messages consumed]"
# Denied:   WARN Not authorized to read from topic orders
```

### 6.5 Test: GSSAPI (when KRB listener enabled)

> Only applicable when the KRB listener is uncommented and deployed (port 9093).
> See §2.5 to enable it.

```bash
# Obtain a TGT from inside a pod that has kinit
kubectl exec -n prod deploy/spark-master -- bash -c "
  kinit -kt /etc/security/keytabs/alice.keytab alice@STARDATADBLABS.LOCAL
  klist
  cat > /tmp/krb.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=svc
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useTicketCache=true;
EOF
  /opt/spark/bin/kafka-console-producer.sh \
    --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9093 \
    --topic orders --producer.config /tmp/krb.properties
"
# Ranger sees username: alice (realm stripped)
# Policy match: alice → topic=orders → publish → Allow
```

### 6.6 Check Ranger audit logs for Kafka

```bash
RANGER_PASS=$(curl -sf \
  -H "X-Vault-Token: $(python3 -c "import json; print(json.load(open('${HOME}/openbao-init-keys.json'))['root_token'])")" \
  "http://192.168.1.50:30820/v1/secret/data/ranger/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

curl -su "admin:${RANGER_PASS}" \
  "http://192.168.1.50:30680/service/audit/access?serviceType=kafka&requestUser=alice&pageSize=20" | \
  python3 -c "
import sys, json
for a in json.load(sys.stdin).get('vXAccessAudits', []):
    print(a.get('eventTime',''), a.get('requestUser',''), a.get('resourcePath',''),
          a.get('action',''), 'ALLOW' if a.get('accessResult')==1 else 'DENY')
"
# Or via UI: http://192.168.1.50:30680 → Audit → Access → filter User=alice, Service=kafka
```

---

## 7. Doris — Testing Alice's Access

> Doris uses SQL password auth. Kerberos principal and SQL user share the same username
> but authenticate independently. A Kerberos ticket does NOT log you in to Doris.

### 7.1 What Ranger controls in Doris

When `access_controller_type = ranger-doris` is set in `fe.conf`, Doris replaces its
native privilege check with a Ranger decision on every query. No SQL `GRANT` is needed —
Ranger policies are the sole source of access rights.

**Alice's access when in `processing_dev`:**
- `SELECT` on databases covered by group policies
- `CREATE` in default database (via `public` group policy 94)
- `SELECT` on information_schema (via `public` group policy 95)

### 7.2 Create a scoped Doris policy for alice

```bash
RANGER_PASS=$(curl -sf \
  -H "X-Vault-Token: $(python3 -c "import json; print(json.load(open('${HOME}/openbao-init-keys.json'))['root_token'])")" \
  "http://192.168.1.50:30820/v1/secret/data/ranger/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

curl -su "admin:${RANGER_PASS}" \
  -X POST http://192.168.1.50:30680/service/plugins/policies \
  -H "Content-Type: application/json" \
  -d '{
    "service":   "doris",
    "name":      "alice-analytics-select",
    "isEnabled": true,
    "resources": {
      "database": {"values":["analytics"],"isExcludes":false,"isRecursive":false},
      "table":    {"values":["*"],         "isExcludes":false,"isRecursive":false},
      "column":   {"values":["*"],         "isExcludes":false,"isRecursive":false}
    },
    "policyItems": [{
      "users":      ["alice"],
      "groups":     [],
      "accesses":   [{"type":"select","isAllowed":true}],
      "conditions": [],
      "delegateAdmin": false
    }]
  }'
```

### 7.3 Test: SELECT (should be allowed)

```bash
kubectl run doris-test -n prod --rm -it --restart=Never \
  --image=mysql:8.0 \
  -- mysql -h doris-fe.prod.svc.cluster.local -P9030 \
     -ualice -p'AliceDoris1!' \
     -e "SELECT DATABASE(), USER(); SELECT * FROM analytics.orders LIMIT 5;"
# Expected: rows returned (Ranger: Allow)
# Denied:   ERROR 1105 (HY000): Access denied; user 'alice' has no privilege on …
```

### 7.4 Test: INSERT (should be denied — not in policy)

```bash
kubectl run doris-test -n prod --rm -it --restart=Never \
  --image=mysql:8.0 \
  -- mysql -h doris-fe.prod.svc.cluster.local -P9030 \
     -ualice -p'AliceDoris1!' \
     -e "INSERT INTO analytics.orders VALUES (999, 'test-row');"
# Expected: ERROR 1105 (HY000): Access denied (Ranger: Deny)
```

### 7.5 Test: verify Ranger decision (audit log)

```bash
RANGER_PASS=$(curl -sf \
  -H "X-Vault-Token: $(python3 -c "import json; print(json.load(open('${HOME}/openbao-init-keys.json'))['root_token'])")" \
  "http://192.168.1.50:30820/v1/secret/data/ranger/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

curl -su "admin:${RANGER_PASS}" \
  "http://192.168.1.50:30680/service/audit/access?serviceType=hive&requestUser=alice&pageSize=20" | \
  python3 -c "
import sys, json
for a in json.load(sys.stdin).get('vXAccessAudits', []):
    result = 'ALLOW' if a.get('accessResult')==1 else 'DENY'
    print(a.get('eventTime',''), result, a.get('action',''), a.get('resourcePath',''))
"
# Or via UI: http://192.168.1.50:30680 → Audit → Access → filter User=alice, Service=doris
```

### 7.6 Optional: column-level masking

1. Ranger Admin → **Access Manager → doris**
2. Click the **Masking** tab → **Add New Policy**
3. Resource: `database=analytics`, `table=customers`, `column=email`
4. User: `alice` → Masking Option: **MASK** (shows `xxxx` in query result)
5. **Save** → active within 30 seconds

---

## 8. Spark — Testing Alice's Access

Spark in this cluster has no cluster-level authentication. Kerberos applies at the
**job level** — the keytab controls what data sources the job can reach, not whether
the job can run.

### 8.1 Submit a job as alice (keytab auth)

```bash
# Mount alice's keytab into the spark-master pod (or pass via spark-submit secrets)
kubectl exec -n prod deploy/spark-master -- \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master-svc.prod.svc.cluster.local:7077 \
    --conf spark.kerberos.enabled=true \
    --conf spark.kerberos.principal=alice@STARDATADBLABS.LOCAL \
    --conf spark.kerberos.keytab=/etc/security/keytabs/keytab \
    --class org.example.MyApp \
    /path/to/app.jar
# The job authenticates as alice@STARDATADBLABS.LOCAL to Kerberized data sources.
# Ranger controls what alice can access in Kafka / Doris / HDFS from within the job.
```

### 8.2 What Ranger governs in Spark jobs

When a Spark job reads from Kafka or queries Doris, Ranger evaluates alice's policies:

| Data source accessed from Spark | How Ranger sees alice | Policy needed |
|---|---|---|
| Kafka topic via Kafka client | SCRAM username `alice` or GSSAPI `alice` | Kafka `publish`/`consume` policy |
| Doris table via JDBC | SQL username `alice` | Doris `select` policy |
| OpenSearch index | HTTP user `alice` (SPNEGO) | OpenSearch policy (pending Ranger plugin) |

### 8.3 Verify alice's Ranger policies cover Spark job access

```bash
RANGER_PASS=$(curl -sf \
  -H "X-Vault-Token: $(python3 -c "import json; print(json.load(open('${HOME}/openbao-init-keys.json'))['root_token'])")" \
  "http://192.168.1.50:30820/v1/secret/data/ranger/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

# Check kafka + doris policies for alice
for svc in kafka hive; do
  echo "=== $svc ==="
  curl -su "admin:${RANGER_PASS}" \
    "http://192.168.1.50:30680/service/plugins/policies?serviceType=${svc}&pageSize=100" | \
    python3 -c "
import sys, json
d = json.load(sys.stdin)
for p in d.get('policies', []):
    for item in p.get('policyItems', []):
        if 'alice' in item.get('users', []) or \
           any(g in item.get('groups', []) for g in ['processing_dev','streaming_dev','public']):
            print(f'  Policy {p[\"id\"]}  {p[\"name\"]}  → {[a[\"type\"] for a in item.get(\"accesses\",[])]}')
"
done
```

---

## 9. OpenSearch — Testing Alice's Access

> **Current state:** OpenSearch authentication uses Kerberos SPNEGO (for HTTPS requests)
> and HTTP Basic auth as fallback. Ranger authorization plugin JAR is not yet installed —
> Ranger policies for OpenSearch are defined but not enforced. Access is controlled by
> OpenSearch built-in roles (`readall`, `all_access`, etc.).

### 9.1 How alice is authorised in OpenSearch today

Alice was created with `backend_roles: ["readall"]` in Step 5.
The `readall` backend role maps to read-only access on all indices via OpenSearch built-in roles.

**What `readall` allows:**
- `GET /<index>/_search`
- `GET /<index>/_doc/<id>`
- `GET /_cat/indices`

**What `readall` denies:**
- `PUT /<index>/_doc/` (write)
- `DELETE /<index>` (delete)
- `PUT /<index>` (create index)

### 9.2 Test: read access (should be allowed)

```bash
OPENSEARCH_PASS=$(kubectl get secret opensearch-credentials -n prod \
  -o jsonpath='{.data.opensearch-password}' | base64 -d)

# Test as alice using HTTP Basic (HTTPS because SSL is now enabled)
curl -sk -u "alice:AliceOS1!" \
  "https://192.168.1.53:30920/_cat/indices?v"
# Expected: list of indices alice can see

curl -sk -u "alice:AliceOS1!" \
  "https://192.168.1.53:30920/orders/_search?pretty&size=3"
# Expected: search results or {"hits":{"total":…}}
# Denied:   {"status":403,"error":{"type":"security_exception"…}}
```

### 9.3 Test: write access (should be denied for readall role)

```bash
curl -sk -u "alice:AliceOS1!" \
  -X PUT "https://192.168.1.53:30920/orders/_doc/999" \
  -H "Content-Type: application/json" \
  -d '{"test":"alice-write-attempt"}'
# Expected: {"status":403,"error":{"type":"security_exception"…}}
```

### 9.4 Test: SPNEGO authentication (Kerberos enabled)

> Requires a client machine with `kinit` and the cluster `krb5.conf`.

```bash
# First, copy the cluster krb5.conf to your client machine
kubectl get cm kerberos-integration-config -n prod \
  -o jsonpath='{.data.krb5\.conf}' > /tmp/krb5-cluster.conf

# Obtain a TGT
KRB5_CONFIG=/tmp/krb5-cluster.conf kinit alice@STARDATADBLABS.LOCAL

# Test SPNEGO auth (HTTP Negotiate)
curl -sk --negotiate -u : \
  "https://192.168.1.53:30920/_cluster/health?pretty"
# Expected: {"status":"green","cluster_name":"opensearch-cluster",…}
# Denied:   {"status":401,"error":{"reason":"Authentication finally failed"}}
```

### 9.5 Change OpenSearch role for alice

To grant alice write access, update her backend role:

```bash
OPENSEARCH_PASS=$(kubectl get secret opensearch-credentials -n prod \
  -o jsonpath='{.data.opensearch-password}' | base64 -d)

curl -sk -u "admin:${OPENSEARCH_PASS}" \
  -X PUT "https://192.168.1.53:30920/_plugins/_security/api/internalusers/alice" \
  -H "Content-Type: application/json" \
  -d '{
    "password":      "AliceOS1!",
    "backend_roles": ["readall", "readall_and_monitor"],
    "attributes":    {}
  }'
```

Available backend roles:
| Role | What it allows |
|---|---|
| `readall` | Read all indices |
| `readall_and_monitor` | Read + cluster monitoring |
| `all_access` | Full admin access |
| `kibana_user` | Dashboards access |

---

## 10. Deleting a User — Full Cleanup

> **Order matters:** remove from Ranger policies → delete service identities → delete Ranger user → revoke Kerberos → delete from OpenBao.

### Step 1 — Find all Ranger policies referencing alice

```bash
RANGER_PASS=$(curl -sf \
  -H "X-Vault-Token: $(python3 -c "import json; print(json.load(open('${HOME}/openbao-init-keys.json'))['root_token'])")" \
  "http://192.168.1.50:30820/v1/secret/data/ranger/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

curl -su "admin:${RANGER_PASS}" \
  "http://192.168.1.50:30680/service/plugins/policies?pageSize=500" | \
  python3 -c "
import sys, json
for p in json.load(sys.stdin).get('policies', []):
    for item in p.get('policyItems', []) + p.get('denyPolicyItems', []):
        if 'alice' in item.get('users', []):
            print(f'  Policy ID={p[\"id\"]}  service={p[\"service\"]}  name={p[\"name\"]}')
"
```

### Step 2 — Remove alice from each policy

**Via UI:** Open each policy → Allow Conditions → click ✕ next to `alice` → Save

**Via REST (patch then PUT):**
```bash
POLICY_ID=113   # replace with actual ID
curl -su "admin:${RANGER_PASS}" \
  "http://192.168.1.50:30680/service/plugins/policies/${POLICY_ID}" | \
  python3 -c "
import sys, json
p = json.load(sys.stdin)
for item in p.get('policyItems', []):
    item['users'] = [u for u in item.get('users', []) if u != 'alice']
print(json.dumps(p))
" > /tmp/policy-patched.json

curl -su "admin:${RANGER_PASS}" \
  -X PUT "http://192.168.1.50:30680/service/plugins/policies/${POLICY_ID}" \
  -H "Content-Type: application/json" -d @/tmp/policy-patched.json
```

### Step 3 — Delete service identities

```bash
# Kafka
kubectl delete kafkauser alice -n prod
kubectl get secret alice -n prod 2>/dev/null && echo "WARNING: secret still exists" || echo "OK"

# Doris
DORIS_ROOT_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
kubectl exec -n prod statefulset/doris-fe -it -- \
  mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_ROOT_PASS}" \
  -e "DROP USER 'alice'@'%';"

# OpenSearch
OPENSEARCH_PASS=$(kubectl get secret opensearch-credentials -n prod \
  -o jsonpath='{.data.opensearch-password}' | base64 -d)
curl -sk -u "admin:${OPENSEARCH_PASS}" \
  -X DELETE "https://192.168.1.53:30920/_plugins/_security/api/internalusers/alice"
```

### Step 4 — Delete the Ranger user record

```bash
RANGER_USER_ID=$(curl -su "admin:${RANGER_PASS}" \
  "http://192.168.1.50:30680/service/xusers/users?pageSize=100" | \
  python3 -c "
import sys, json
for u in json.load(sys.stdin)['vXUsers']:
    if u['name'] == 'alice':
        print(u['id'])
")

curl -su "admin:${RANGER_PASS}" \
  -X DELETE \
  "http://192.168.1.50:30680/service/xusers/users/${RANGER_USER_ID}?forceDelete=true"
```

### Step 5 — Delete the Kerberos principal

```bash
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "delprinc -force alice@STARDATADBLABS.LOCAL"

# Delete keytab K8s secret
kubectl delete secret alice-keytab -n prod 2>/dev/null || echo "not found"

# Verify
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "getprinc alice@STARDATADBLABS.LOCAL" 2>&1 | \
  grep -i "does not exist" && echo "OK — KDC clean"
```

### Step 6 — Delete from OpenBao

```bash
BAO_ADDR="http://192.168.1.50:30820"
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${HOME}/openbao-init-keys.json'))['root_token'])")

for path in kafka doris opensearch kerberos; do
  curl -sf -X DELETE -H "X-Vault-Token: ${ROOT_TOKEN}" \
    "${BAO_ADDR}/v1/secret/data/${path}/users/alice" && echo "${path} deleted"
done
```

### Step 7 — Verify complete cleanup

```bash
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${HOME}/openbao-init-keys.json'))['root_token'])")
RANGER_PASS=$(curl -sf -H "X-Vault-Token: ${ROOT_TOKEN}" \
  "http://192.168.1.50:30820/v1/secret/data/ranger/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

echo "--- Ranger policies ---"
curl -su "admin:${RANGER_PASS}" \
  "http://192.168.1.50:30680/service/plugins/policies?pageSize=500" | \
  python3 -c "
import sys,json; raw=sys.stdin.read()
print('CLEAN' if 'alice' not in raw else 'WARNING: alice still in policies')
"

echo "--- Ranger user ---"
curl -su "admin:${RANGER_PASS}" \
  "http://192.168.1.50:30680/service/xusers/users?pageSize=100" | \
  python3 -c "
import sys,json
names=[u['name'] for u in json.load(sys.stdin)['vXUsers']]
print('CLEAN' if 'alice' not in names else 'WARNING: alice still in Ranger')
"

echo "--- KDC ---"
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "listprincs" 2>/dev/null | grep "alice@" && \
  echo "WARNING: still in KDC" || echo "CLEAN"

echo "--- K8s secrets ---"
kubectl get secret alice alice-keytab -n prod 2>&1 | grep -v "not found" | \
  grep -q "NAME" && echo "WARNING: secrets still exist" || echo "CLEAN"

echo "--- OpenBao ---"
for path in kafka doris opensearch kerberos; do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "X-Vault-Token: ${ROOT_TOKEN}" \
    "http://192.168.1.50:30820/v1/secret/data/${path}/users/alice")
  echo "  ${path}: $([ "$STATUS" = "404" ] && echo CLEAN || echo "WARNING HTTP $STATUS")"
done

echo "=== Cleanup verification complete ==="
```

---

## 11. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Policy change not taking effect | Plugin polls every 30s | Wait 30s. Check: `kubectl logs -n prod strimzi-kafka-combined-0 --tail=50 \| grep "policy version"` |
| Policy version stays at `-1` | Plugin can't auth to Ranger Admin | Check `ranger-kafka-security.xml` credentials in ConfigMap `kafka-ranger-config`. Verify `kafka-app-user` exists in Ranger |
| Kafka user denied even in policy | `allow.everyone.if.no.acl.found=true` fallback off | Lab: set `"true"`. Prod: leave `"false"` and ensure policy is correct |
| KafkaUser stuck `NotReady` | AclCache error (old image) | Use image `ranger-v6` or later. Check: `kubectl logs -n prod deploy/strimzi-kafka-entity-operator -c user-operator --tail=50` |
| Doris: Ranger allows but query fails | Doris Ranger plugin cache stale | `kubectl rollout restart statefulset/doris-fe -n prod` |
| OpenSearch SPNEGO 401 | `kerberos_auth_domain` not applied | Run `securityadmin.sh` — pod restart does NOT apply security index changes (see §2.4) |
| OpenSearch security API 403 | HTTP SSL was disabled | HTTP SSL is now enabled. Use `https://` on port 9200/30920 |
| `kinit: KDC unreachable` | Pod can't reach KDC | `kubectl exec -n prod deploy/spark-master -- nc -zv kerberos-kdc.prod.svc.cluster.local 88` |
| `Clock skew too great` | Client clock > 5 min drift from KDC | Sync NTP. Check: `date` on all worker nodes |
| Ranger username mismatch | Realm not stripped | Confirm `strip_realm_from_principal=true` in service Kerberos config. Ranger sees `alice`, not `alice@STARDATADBLABS.LOCAL` |
| Kafka GSSAPI: `KafkaServer entry not found` | JAAS file not mounted | Uncomment `kafka-jaas` volume+mount in `kafka-cluster.yaml` AND add `-Djava.security.auth.login.config` to `KAFKA_OPTS` |

### Useful diagnostic commands

```bash
# Ranger: check all services
curl -su admin:Priya1982 \
  "http://192.168.1.50:30680/service/plugins/services?pageSize=100" | \
  python3 -c "import sys,json; [print(s['id'],s['name'],s['type'],s['isEnabled']) \
    for s in json.load(sys.stdin)['services']]"

# Ranger: list all users and their groups
curl -su admin:Priya1982 \
  "http://192.168.1.50:30680/service/xusers/users?pageSize=100" | \
  python3 -c "import sys,json; [print(u['id'],u['name'],u.get('groupNameList',[])) \
    for u in json.load(sys.stdin)['vXUsers']]"

# Kafka: check policy sync
kubectl logs -n prod strimzi-kafka-combined-0 --tail=200 | \
  grep -E "policy version|PolicyRefresher|Ranger"

# Kafka: check KRB listener status
kubectl get kafka strimzi-kafka -n prod \
  -o jsonpath='{.status.listeners[*].name}' && echo

# Doris: check Ranger plugin cache
kubectl exec -n prod statefulset/doris-fe -- \
  ls -la /opt/apache-doris/fe/ranger-cache/ 2>/dev/null | head -10

# OpenSearch: verify auth config
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  mkdir -p /tmp/check
  /usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh \
    -backup /tmp/check -icl -nhnv \
    -cacert /usr/share/opensearch/config/tls/root-ca.pem \
    -cert   /usr/share/opensearch/config/tls/admin.pem \
    -key    /usr/share/opensearch/config/tls/admin-key.pem \
    -h localhost -p 9200 2>/dev/null
  grep -A5 'kerberos_auth_domain' /tmp/check/config.yml
"

# KDC: list all principals
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "listprincs" 2>/dev/null

# Kerberos toggle current value
kubectl get cm kerberos-integration-config -n prod \
  -o jsonpath='{.data.kerberos\.enabled}' && echo

# Force Kafka policy reload (last resort)
kubectl rollout restart statefulset/strimzi-kafka-combined -n prod
```

---

## 12. Quick Reference

### Service endpoints

| Service | URL | Auth |
|---|---|---|
| Ranger Admin UI | `http://192.168.1.50:30680` | `admin / Priya1982` |
| Ranger REST API | `http://192.168.1.50:30680/service/` | Basic auth |
| OpenSearch | `https://192.168.1.53:30920` | Basic or SPNEGO |
| Doris MySQL | `192.168.1.50:30090` | SQL password |
| Kafka SCRAM | `192.168.1.54:30093` (external) / bootstrap:9092 (in-cluster) | SCRAM-SHA-512 |
| KDC | `kerberos-kdc.prod.svc.cluster.local:88` | — |
| OpenBao | `http://192.168.1.50:30820` | Root token |

### Ranger service IDs

| Service | ID | Type |
|---|---|---|
| `kafka` | 18 | kafka |
| `doris` | 13 | hive |
| `opensearch` | 15 | elasticsearch |

### Plugin poll interval
All Ranger plugins poll every **30 seconds**. Policy changes take effect within 30s.

### Kerberos toggle file
`manifests/kerberos/kerberos-integration-config.yaml` — key `kerberos.enabled`

| Value | Effect |
|---|---|
| `"true"` | Kerberos active — OpenSearch SPNEGO on, Spark job auth on, Ranger SPNEGO on |
| `"false"` | Kerberos disabled — services use SCRAM / SQL password / HTTP Basic |

### Bootstrap K8s secrets

| Secret | Namespace | Key | Used by |
|---|---|---|---|
| `ranger-db-credentials` | `prod` | `admin-password` | Ranger Admin |
| `kafka-app-user` | `prod` | `password` | Ranger plugin auth |
| `opensearch-credentials` | `prod` | `opensearch-password` | OpenSearch admin |
| `doris-credentials` | `prod` | `admin-password` | Doris root |
| `kerberos-admin` | `prod` | `admin-password` | kadmin.local |
