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
# Note: pyyaml is not available in the OpenSearch container (AL2023, non-root).
# Uses Python stdlib re module only — see §2.4 for full explanation.
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  mkdir -p /tmp/backup-secconfig
  /usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh \
    -backup /tmp/backup-secconfig -icl -nhnv \
    -cacert /usr/share/opensearch/config/tls/root-ca.pem \
    -cert   /usr/share/opensearch/config/tls/admin.pem \
    -key    /usr/share/opensearch/config/tls/admin-key.pem \
    -h localhost -p 9200 2>/dev/null

  python3 - <<'PYEOF'
import re
with open('/tmp/backup-secconfig/config.yml') as f:
    text = f.read()
lines = text.splitlines()
out = []
in_krb = False
for line in lines:
    if 'kerberos_auth_domain:' in line:
        in_krb = True
    if in_krb and re.match(r'\s+http_enabled\s*:', line):
        line = re.sub(r'(http_enabled\s*:\s*).*', r'\g<1>false', line)
    out.append(line)
with open('/tmp/backup-secconfig/config.yml', 'w') as f:
    f.write('\n'.join(out) + '\n')
print('Kerberos disabled in config')
PYEOF

  /usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh \
    -f /tmp/backup-secconfig/config.yml -t config -icl -nhnv \
    -cacert /usr/share/opensearch/config/tls/root-ca.pem \
    -cert   /usr/share/opensearch/config/tls/admin.pem \
    -key    /usr/share/opensearch/config/tls/admin-key.pem \
    -h localhost -p 9200 2>&1 | tail -5
"
# Expected output:
# Kerberos disabled in config
# Done with success

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

> **Note:** The OpenSearch container runs on Amazon Linux 2023 as a non-root user. `pyyaml` is not available and
> cannot be installed (`dnf` requires root, `pip` is absent). The script below uses only Python's built-in `re`
> module which is always present.

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
import re

with open('/tmp/backup-secconfig/config.yml') as f:
    text = f.read()

lines = text.splitlines()
out = []
in_krb = False
for line in lines:
    if 'kerberos_auth_domain:' in line:
        in_krb = True
    if in_krb and re.match(r'\s+http_enabled\s*:', line):
        line = re.sub(r'(http_enabled\s*:\s*).*', r'\g<1>true', line)
    if in_krb and re.match(r'\s+order\s*:', line):
        line = re.sub(r'(order\s*:\s*).*', r'\g<1>1', line)
    if in_krb and re.match(r'\s+krb_debug\s*:', line):
        line = re.sub(r'(krb_debug\s*:\s*).*', r'\g<1>false', line)
    if in_krb and re.match(r'\s+strip_realm_from_principal\s*:', line):
        line = re.sub(r'(strip_realm_from_principal\s*:\s*).*', r'\g<1>true', line)
    if in_krb and re.match(r'\s+krb_service_principal\s*:', line):
        line = re.sub(r'(krb_service_principal\s*:\s*).*', r'\g<1>svc/opensearch@STARDATADBLABS.LOCAL', line)
    if in_krb and re.match(r'\s+krb_keytab_path\s*:', line):
        line = re.sub(r'(krb_keytab_path\s*:\s*).*', r'\g<1>/etc/security/keytabs/opensearch.service.keytab', line)
    out.append(line)

with open('/tmp/backup-secconfig/config.yml', 'w') as f:
    f.write('\n'.join(out) + '\n')
print('Kerberos SPNEGO enabled')
PYEOF

  /usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh \
    -f /tmp/backup-secconfig/config.yml -t config -icl -nhnv \
    -cacert /usr/share/opensearch/config/tls/root-ca.pem \
    -cert   /usr/share/opensearch/config/tls/admin.pem \
    -key    /usr/share/opensearch/config/tls/admin-key.pem \
    -h localhost -p 9200 2>&1 | tail -5
"
# Expected output:
# Kerberos SPNEGO enabled
# Done with success
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

> **Enforcement model (as of current deployment):**
>
> | Service | Auth gate | KDC enforced at protocol level? |
> |---|---|---|
> | Kafka | GSSAPI only (port 9093) — no SCRAM for users | ✅ Yes — ticket required |
> | OpenSearch | SPNEGO only — Basic auth disabled | ✅ Yes — ticket required |
> | Doris | SQL password (Doris 4.x has no GSSAPI) | ⚠️ Procedural — KDC principal is prerequisite for SQL user creation |
> | Spark | No cluster-level auth gate | ⚠️ Procedural — keytab required per-job |
>
> Steps 1–2 (KDC principal + keytab) are mandatory for **all services**.
> Without them, Kafka and OpenSearch refuse connection at the protocol layer.
> Doris and Spark rely on the operational procedure: the SQL user and keytab secret
> are only created after the KDC principal exists. See §5.6 for full details.

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

### Step 3 — Register the Kafka user (GSSAPI — no password needed)

Kafka now uses **GSSAPI only**. A `KafkaUser` CR is still required so Strimzi registers
the username with the User Operator and Ranger can track the identity — but the
authentication is done by the Kerberos ticket from Step 1/2, not a SCRAM password.

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
  # No authentication block — user connects via GSSAPI (Kerberos ticket).
  # Strimzi registers the username so Ranger can assign policies to it.
  # The KDC principal alice@STARDATADBLABS.LOCAL is the actual credential.
EOF

# Verify the KafkaUser was accepted (Ready=True, no auth secret created)
kubectl wait kafkauser alice -n prod --for=condition=Ready --timeout=60s
kubectl get kafkauser alice -n prod -o wide
# Note: no K8s Secret named "alice" will be created (no SCRAM credential).
# alice connects to Kafka with:
#   kinit -kt /path/to/alice.keytab alice@STARDATADBLABS.LOCAL
#   then use GSSAPI on bootstrap:9093
```

### Step 4 — Create the Doris SQL user

Doris uses SQL authentication. The username must match the Kerberos short name (`alice`).

> **Known gotchas — read before running:**
>
> 1. **Always specify `-c doris-fe`** — the FE pod has a `krb-doris-guard` sidecar as the
>    default container. Without `-c doris-fe`, kubectl exec targets the sidecar which has
>    no `mysql` binary: `exec: "mysql": executable file not found in $PATH`.
>
> 2. **Escape `!` in passwords** — bash history expansion treats `!` as a special character
>    in double-quoted strings. Use `\!` inside double-quotes:
>    `"AliceDoris1\!"` not `"AliceDoris1!"`.
>
> 3. **`root` user, not `admin`** — the `admin-password` secret sets the `root` user's
>    password via the `DORIS_ROOT_PASSWORD` env var on first boot. Use `-uroot`, not `-uadmin`.
>    The `admin` user exists but has limited privileges.
>
> 4. **BE must be alive** — `CREATE USER` requires a live backend. If `SHOW BACKENDS\G`
>    shows `Alive: false`, fix BE registration first (see [BE not registering with FE](../doris.md#be-not-registering-with-fe)).

```bash
BAO_ADDR="http://192.168.1.50:30820"
KEYS_FILE="${HOME}/openbao-init-keys.json"
[ -f "${KEYS_FILE}" ] || KEYS_FILE="/root/openbao-init-keys.json"
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${KEYS_FILE}'))['root_token'])")

DORIS_ROOT_PASS=$(curl -sf \
  -H "X-Vault-Token: ${ROOT_TOKEN}" \
  "${BAO_ADDR}/v1/secret/data/doris/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

# -c doris-fe is required — default container is krb-doris-guard (no mysql)
kubectl exec -n prod statefulset/doris-fe -c doris-fe -- \
  mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_ROOT_PASS}" \
  -e "CREATE USER 'alice'@'%' IDENTIFIED BY 'AliceDoris1\!';"

# Verify
kubectl exec -n prod statefulset/doris-fe -c doris-fe -- \
  mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_ROOT_PASS}" \
  -e "SELECT user, host FROM mysql.user WHERE user='alice';"
# Expected:
# user  host
# alice %
```

### Step 5 — Register the OpenSearch user (SPNEGO — no password needed)

OpenSearch now uses **SPNEGO only**. HTTP Basic is disabled.
The user must exist in OpenSearch's internal user store so that roles/backend_roles
can be assigned to the Kerberos identity after SPNEGO authentication maps the
principal name to the OpenSearch username.

The `securityadmin.sh` tool authenticates via TLS client cert (`admin_dn`) — it is
unaffected by the Basic auth disable.

```bash
# admin credentials are only used by securityadmin.sh via TLS cert — not Basic auth
# Use the security REST API (which requires admin TLS cert authentication):
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  /usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh \
    -backup /tmp/os-backup -icl -nhnv \
    -cacert /usr/share/opensearch/config/tls/root-ca.pem \
    -cert   /usr/share/opensearch/config/tls/admin.pem \
    -key    /usr/share/opensearch/config/tls/admin-key.pem \
    -h localhost -p 9200 2>/dev/null

  python3 - <<'PYEOF'
import yaml, json

# Load existing internal_users
with open('/tmp/os-backup/internal_users.yml') as f:
    users = yaml.safe_load(f)

# Add alice — no password hash needed; SPNEGO supplies the identity.
# backend_roles controls what alice can access in OpenSearch.
users['alice'] = {
    'hash': '',
    'reserved': False,
    'hidden': False,
    'backend_roles': ['readall'],
    'attributes': {},
    'description': 'alice — Kerberos SPNEGO identity, no password'
}

with open('/tmp/os-backup/internal_users.yml', 'w') as f:
    yaml.dump(users, f, default_flow_style=False)
print('alice added to internal_users')
PYEOF

  /usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh \
    -f /tmp/os-backup/internal_users.yml -t internalusers -icl -nhnv \
    -cacert /usr/share/opensearch/config/tls/root-ca.pem \
    -cert   /usr/share/opensearch/config/tls/admin.pem \
    -key    /usr/share/opensearch/config/tls/admin-key.pem \
    -h localhost -p 9200 2>&1 | tail -3
"
# Expected: Done with success
# alice can now authenticate via SPNEGO only — no password works.
```

### Step 6 — Store credentials in OpenBao

```bash
BAO_ADDR="http://192.168.1.50:30820"
KEYS_FILE="${HOME}/openbao-init-keys.json"
[ -f "${KEYS_FILE}" ] || KEYS_FILE="/root/openbao-init-keys.json"
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${KEYS_FILE}'))['root_token'])")

# Doris SQL password (the only service-level password that still exists)
curl -sf -X POST \
  -H "X-Vault-Token: ${ROOT_TOKEN}" -H "Content-Type: application/json" \
  -d '{"data":{"username":"alice","password":"AliceDoris1!","service":"doris","created_by":"admin"}}' \
  "${BAO_ADDR}/v1/secret/data/doris/users/alice" && echo "Doris stored"

# Kerberos principal + keytab metadata (source of truth for all Kerberos services)
curl -sf -X POST \
  -H "X-Vault-Token: ${ROOT_TOKEN}" -H "Content-Type: application/json" \
  -d '{"data":{"principal":"alice@STARDATADBLABS.LOCAL","keytab_secret":"alice-keytab","services":["kafka","opensearch","spark"],"created_by":"admin"}}' \
  "${BAO_ADDR}/v1/secret/data/kerberos/users/alice" && echo "Kerberos stored"

# NOTE: No Kafka SCRAM password — Kafka auth is now GSSAPI (keytab above).
# NOTE: No OpenSearch password — OpenSearch auth is now SPNEGO (keytab above).
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

| Service | Auth method | Access from group membership |
|---|---|---|
| Kafka | GSSAPI — ticket from `alice-keytab` | `streaming_dev` → produce/consume on topics covered by group policies |
| Doris | SQL password `AliceDoris1!` | `processing_dev` → SELECT on databases covered by group policies |
| OpenSearch | SPNEGO — ticket from `alice-keytab` | `readall` backend role → read all indices |
| Spark | keytab per-job | Accesses Kafka/OpenSearch as alice; Ranger enforces at data source |

---

## 5.5 Proving That a User Without a Kerberos Principal Has No Access

> **What this proves:** Kerberos is the *root of trust* for every identity on this platform.
> Without a KDC principal there is no keytab, no SPNEGO ticket, and no Kerberos short name
> for Ranger to evaluate. The tests below use a fictional user `bob` who exists
> **nowhere** in the platform (no KDC principal, no KafkaUser CR, no Doris SQL user,
> no OpenSearch internal user, no keytab K8s secret).

### How the security boundary actually works

Each service has **two independent enforcement layers**:

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Layer 1 — Service-level Authentication (identity check)                 │
│                                                                           │
│  Kafka   : SCRAM credential store  ← KafkaUser CR must exist             │
│            GSSAPI TGT              ← KDC principal must exist            │
│  Doris   : mysql.user row          ← CREATE USER must have been run      │
│  OpenSearch: internalusers entry   ← API PUT must have been done         │
│            OR valid Kerberos TGT   ← KDC principal must exist            │
│  Spark   : keytab file on pod      ← K8s secret must be mounted          │
│                                                                           │
│  Without a KDC principal → cannot generate keytab → cannot create        │
│  service credentials → Layer 1 rejects the user                          │
├──────────────────────────────────────────────────────────────────────────┤
│  Layer 2 — Ranger Authorization (access decision)                        │
│                                                                           │
│  Only reached if Layer 1 succeeds. Ranger evaluates policies against     │
│  the authenticated username. If no matching Allow policy exists →         │
│  DENY (default-deny). No Ranger user record → no policies → DENY.        │
│                                                                           │
│  Note: Kafka has allow.everyone.if.no.acl.found=true as a startup        │
│  safety fallback, but this only applies AFTER SCRAM authentication        │
│  succeeds. Bob cannot pass SCRAM auth, so he never reaches Ranger.        │
└──────────────────────────────────────────────────────────────────────────┘
```

> **Key fact about Kafka SCRAM:** Kafka SCRAM credentials are completely independent of
> Kerberos. They come from the `KafkaUser` CR (Strimzi manages them). The reason bob
> cannot use Kafka is not that he lacks a Kerberos principal — it is that no `KafkaUser`
> CR named `bob` exists, so no SCRAM secret was ever created.
> The KDC principal is the prerequisite for creating all other service credentials in
> the new-user workflow (§5), which is why it is the root of trust in practice.

---

### Pre-test: confirm bob does not exist anywhere

Run these checks first to establish the baseline. All should return "not found".

```bash
# KDC — no principal
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "getprinc bob@STARDATADBLABS.LOCAL" 2>&1 | \
  grep -E "does not exist|Principal:" | head -3
# Expected: Principal 'bob@STARDATADBLABS.LOCAL' does not exist

# Kafka — no KafkaUser CR and no SCRAM secret
kubectl get kafkauser bob -n prod 2>&1
# Expected: Error from server (NotFound): kafkausers.kafka.strimzi.io "bob" not found
kubectl get secret bob -n prod 2>&1
# Expected: Error from server (NotFound): secrets "bob" not found

# Doris — no SQL user
BAO_ADDR="http://192.168.1.50:30820"
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${HOME}/openbao-init-keys.json'))['root_token'])")
DORIS_ROOT_PASS=$(curl -sf \
  -H "X-Vault-Token: ${ROOT_TOKEN}" \
  "${BAO_ADDR}/v1/secret/data/doris/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")
kubectl exec -n prod statefulset/doris-fe -- \
  mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_ROOT_PASS}" \
  -e "SELECT user, host FROM mysql.user WHERE user='bob';" 2>/dev/null
# Expected: Empty set (0 rows)

# OpenSearch — no internal user
OPENSEARCH_PASS=$(kubectl get secret opensearch-credentials -n prod \
  -o jsonpath='{.data.opensearch-password}' | base64 -d)
curl -sk -u "admin:${OPENSEARCH_PASS}" \
  "https://192.168.1.53:30920/_plugins/_security/api/internalusers/bob" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status','?'), d.get('message',''))"
# Expected: NOT_FOUND User bob not found.

# Ranger — no user record
RANGER_PASS=$(curl -sf \
  -H "X-Vault-Token: ${ROOT_TOKEN}" \
  "${BAO_ADDR}/v1/secret/data/ranger/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")
curl -su "admin:${RANGER_PASS}" \
  "http://192.168.1.50:30680/service/xusers/users?pageSize=500" | \
  python3 -c "
import sys, json
names = [u['name'] for u in json.load(sys.stdin)['vXUsers']]
print('CLEAN — bob not in Ranger' if 'bob' not in names else 'WARNING: bob exists in Ranger')
"
# Expected: CLEAN — bob not in Ranger
```

---

### Test A — Kafka (SCRAM): no KafkaUser CR → SASL handshake fails

Strimzi only writes a SCRAM credential into the broker's credential store when a
`KafkaUser` CR exists. No CR → no credential → the SASL handshake is rejected at the
broker before the Ranger authorizer is even invoked.

```bash
# Attempt to produce as bob using a made-up password
kubectl run kafka-deny-bob -n prod --rm -it --restart=Never \
  --image=192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0-ranger-v6 \
  -- bash -c "
cat > /tmp/bob.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username=\"bob\" password=\"WrongPass1!\";
EOF
echo 'hello-bob' | /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092 \
  --topic orders --producer.config /tmp/bob.properties 2>&1 | tail -5
echo \"Exit: \$?\"
"
# Expected:
#   WARN  [Producer] SaslAuthenticationException: Authentication failed:
#         Invalid username or password
#   Exit: 1

# Also try consume — same result
kubectl run kafka-deny-bob-consume -n prod --rm -it --restart=Never \
  --image=192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0-ranger-v6 \
  -- bash -c "
cat > /tmp/bob.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username=\"bob\" password=\"WrongPass1!\";
EOF
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092 \
  --topic orders --consumer.config /tmp/bob.properties \
  --from-beginning --max-messages 1 --timeout-ms 5000 2>&1 | tail -5
echo \"Exit: \$?\"
"
# Expected:
#   WARN  Not authorized to read from topic orders  (or SaslAuthenticationException)
#   Exit: 1
```

> **Why Ranger is not consulted:** The Kafka broker SCRAM credential store has no entry
> for `bob`. The SASL exchange fails at step 2 of the handshake — the authorizer
> (`RangerKafkaAuthorizer`) is only called *after* authentication succeeds.
> `allow.everyone.if.no.acl.found=true` is a post-authentication fallback; it does not
> bypass SCRAM auth.

---

### Test B — Kafka (GSSAPI): no KDC principal → `kinit` fails → no ticket

> Only relevant when the KRB listener (port 9093) is enabled. See §2.5.
> The test is run from the spark-master pod which has `kinit` and the cluster `krb5.conf`.

```bash
# Attempt password-based kinit for a non-existent principal
# (MIT Kerberos kinit reads the password from the terminal; use -n to suppress prompt,
#  or pass via expect. The key observable is the KDC error, not the password method.)
kubectl exec -n prod deploy/spark-master -- bash -c "
  KRB5_CONFIG=/etc/krb5.conf.d/cluster.conf \
  kinit -V bob@STARDATADBLABS.LOCAL </dev/null 2>&1 || true
  echo '---'
  klist 2>&1 || true
"
# Expected:
#   kinit: Client 'bob@STARDATADBLABS.LOCAL' not found in Kerberos database
#          while getting initial credentials
#   ---
#   klist: No credentials cache found (filename: /tmp/krb5cc_0)
#
# Without a TGT, any subsequent GSSAPI client attempt fails immediately:
#   GSSException: No valid credentials provided
#     (Mechanism level: Failed to find any Kerberos tgt)
```

---

### Test C — Doris (SQL): no mysql.user row → auth rejected before Ranger

Doris FE authenticates SQL clients against its own `mysql.user` table. Even though
Ranger controls what the user can *do* (`access_controller_type=ranger-doris`), Doris
must first verify the user's password. No row → login rejected → Ranger never runs.

```bash
# Attempt MySQL connection as bob
kubectl run doris-deny-bob -n prod --rm -it --restart=Never \
  --image=mysql:8.0 \
  -- mysql -h doris-fe.prod.svc.cluster.local -P9030 \
     -ubob -p'WrongPass1!' \
     -e "SELECT 1;" 2>&1
# Expected:
#   ERROR 1045 (28000): Access denied for user 'bob'@'<IP>' (using password: YES)
#
# Ranger audit log will show ZERO entries for bob — Doris never called the plugin.

# Verify Ranger has no audit entry for bob in Doris
curl -su "admin:${RANGER_PASS}" \
  "http://192.168.1.50:30680/service/audit/access?serviceType=hive&requestUser=bob&pageSize=10" | \
  python3 -c "
import sys, json
count = len(json.load(sys.stdin).get('vXAccessAudits', []))
print(f'Ranger Doris audit entries for bob: {count}')
print('CONFIRMED: Ranger not consulted — Doris rejected bob at SQL auth layer'
      if count == 0 else 'WARNING: unexpected audit entries')
"
```

---

### Test D — OpenSearch (HTTP Basic): no internalusers entry → HTTP 401

The OpenSearch security plugin checks its internal user store before evaluating any
request. No entry for `bob` → 401 Unauthorized, regardless of Ranger policy.

```bash
OPENSEARCH_PASS=$(kubectl get secret opensearch-credentials -n prod \
  -o jsonpath='{.data.opensearch-password}' | base64 -d)

# Attempt cluster health as bob — must return 401
curl -sk -o /dev/null -w "HTTP %{http_code}\n" \
  -u "bob:WrongPass1!" \
  "https://192.168.1.53:30920/_cluster/health"
# Expected: HTTP 401

# Attempt search as bob — must return 401
curl -sk -o /dev/null -w "HTTP %{http_code}\n" \
  -u "bob:WrongPass1!" \
  "https://192.168.1.53:30920/orders/_search"
# Expected: HTTP 401

# Attempt write as bob — must return 401
curl -sk -o /dev/null -w "HTTP %{http_code}\n" \
  -u "bob:WrongPass1!" \
  -X PUT "https://192.168.1.53:30920/orders/_doc/1" \
  -H "Content-Type: application/json" \
  -d '{"test":"bob"}'
# Expected: HTTP 401
```

---

### Test D2 — OpenSearch (SPNEGO): no KDC principal → no TGT → HTTP 401

SPNEGO requires a valid Kerberos ticket. Without a KDC principal there is nothing to
`kinit` with — the GSSAPI layer has no credentials to present, and OpenSearch returns 401.

```bash
# Verify no Kerberos credentials exist in the pod's credential cache
kubectl exec -n prod opensearch-cluster-master-0 -- klist 2>&1 || true
# Expected: klist: No credentials cache found

# Attempt SPNEGO auth to OpenSearch — must return 401
# (run from a machine where no valid TGT for bob exists)
curl -sk -o /dev/null -w "HTTP %{http_code}\n" \
  --negotiate -u : \
  "https://192.168.1.53:30920/_cluster/health"
# Expected: HTTP 401
# (If you have a valid alice TGT in your cache this returns 200 as alice —
#  ensure you clear the cache first: kdestroy && klist must show empty)
```

---

### Test E — Spark: no keytab mounted → Kerberized data access fails

Spark standalone has no cluster-level authentication gate — `spark-submit` itself runs.
However, `spark.kerberos.enabled=true` requires a valid keytab on the driver pod.
Bob's keytab does not exist as a K8s secret and is not mounted, so the job fails at
`SparkContext` initialisation before it can touch any data source.

```bash
# Confirm there is no bob-keytab secret
kubectl get secret bob-keytab -n prod 2>&1
# Expected: Error from server (NotFound): secrets "bob-keytab" not found

# Attempt spark-submit with Kerberos enabled pointing at a non-existent keytab
kubectl exec -n prod deploy/spark-master -- \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master-svc.prod.svc.cluster.local:7077 \
    --conf spark.kerberos.enabled=true \
    --conf spark.kerberos.principal=bob@STARDATADBLABS.LOCAL \
    --conf spark.kerberos.keytab=/etc/security/keytabs/bob.keytab \
    --class org.apache.spark.examples.SparkPi \
    /opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar 100 2>&1 | \
  grep -E "keytab|Kerberos|KrbException|FileNotFound|GSSException|ERROR" | head -10
# Expected:
#   ERROR SparkContext: Error initializing SparkContext.
#   java.io.FileNotFoundException: /etc/security/keytabs/bob.keytab
#     (No such file or directory)
#
# If a fake file existed (not a real keytab):
#   javax.security.auth.login.LoginException: Unable to obtain password from user
#   OR: KrbException: Cannot locate KDC for realm STARDATADBLABS.LOCAL
#       (if krb5.conf is missing — it is mounted, so this path won't happen)
#   OR: GSSException: No valid credentials provided
#       (Mechanism level: Failed to find any Kerberos tgt)

# Even if the job submitted without Kerberos, accessing Kafka GSSAPI port 9093
# or OpenSearch SPNEGO endpoint would fail at the data-access layer:
#   kafka-clients: Failed to initiate SASL handshake (no TGT)
#   opensearch: HTTP 401 (no SPNEGO token)
```

---

### Test F — Ranger: no user record → confirm zero audit entries across all services

This is the final cross-service verification that bob never reached any Ranger
authorization decision on any service.

```bash
RANGER_PASS=$(curl -sf \
  -H "X-Vault-Token: $(python3 -c "import json; print(json.load(open('${HOME}/openbao-init-keys.json'))['root_token'])")" \
  "http://192.168.1.50:30820/v1/secret/data/ranger/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

# Check audit log for bob across all service types
for svctype in kafka hive elasticsearch; do
  COUNT=$(curl -su "admin:${RANGER_PASS}" \
    "http://192.168.1.50:30680/service/audit/access?serviceType=${svctype}&requestUser=bob&pageSize=10" | \
    python3 -c "import sys,json; print(len(json.load(sys.stdin).get('vXAccessAudits',[])))")
  echo "${svctype}: ${COUNT} audit entries for bob"
done
# Expected:
#   kafka: 0 audit entries for bob
#   hive: 0 audit entries for bob
#   elasticsearch: 0 audit entries for bob
#
# CONFIRMED: bob was rejected at the service auth layer on every service.
# Ranger was never consulted because authentication always failed first.
```

> **Via UI:** Ranger Admin → Audit → Access → set `User = bob` → click Search.
> All three service types should return zero rows.

---

### Summary: where bob is blocked on each service

| Service | Auth mechanism | Blocked at | Error | Ranger consulted? |
|---|---|---|---|---|
| Kafka (SCRAM) | SCRAM-SHA-512 | SASL handshake | `Authentication failed: Invalid username or password` | ❌ No — auth fails before authorizer |
| Kafka (GSSAPI) | Kerberos GSSAPI | `kinit` / KDC lookup | `Client not found in Kerberos database` | ❌ No — no TGT obtained |
| Doris | SQL password | MySQL auth layer | `ERROR 1045: Access denied` | ❌ No — Doris rejects before calling Ranger plugin |
| OpenSearch (Basic) | HTTP Basic | Security plugin internalusers | `HTTP 401 Unauthorized` | ❌ No — rejected before authz |
| OpenSearch (SPNEGO) | Kerberos SPNEGO | GSSAPI / Security plugin | `HTTP 401 Unauthorized` (no TGT) | ❌ No — no SPNEGO token |
| Spark (data access) | Kerberos keytab | Keytab missing / SparkContext init | `FileNotFoundException` or `GSSException` | ❌ No — job fails before reaching data |
| All services (2nd wall) | — | Ranger default-deny | `DENY` — no user record, no Allow policy | ✅ Yes (if auth bypassed) |

> **Takeaway:** The Kerberos KDC is the prerequisite for creating *all* service credentials
> in this platform's user workflow (§5). A user without a KDC principal has no keytab, no
> SCRAM secret, no SQL row, and no OpenSearch entry — they are blocked at the authentication
> layer of every service independently, without Ranger being consulted.
> Ranger's default-deny is a second, independent enforcement layer that catches anything
> that might slip past authentication (e.g., a misconfigured listener), ensuring no
> unknown user can ever receive an Allow decision.

---



## 6. Kafka — Testing a New Kerberos User (GSSAPI)

> **Auth model:** Kafka only accepts connections on port 9093 (GSSAPI).
> There is no SCRAM listener for users. A valid Kerberos keytab is the only credential.
> Port 9092 is reserved for Strimzi operators only.

### 6.1 Prerequisites — verify keytab secret exists

```bash
# alice-keytab must exist in the prod namespace (created in §5 Step 2)
kubectl get secret alice-keytab -n prod \
  -o jsonpath='{.metadata.name}' && echo " — keytab secret OK"
# Expected: alice-keytab — keytab secret OK

# KafkaUser CR must exist (created in §5 Step 3)
kubectl get kafkauser alice -n prod
# Expected: NAME    CLUSTER         AUTHENTICATION   READY
#           alice   strimzi-kafka                    True
```

### 6.2 Step-by-step: obtain a TGT and produce a message via GSSAPI

All commands run from inside the `spark-master` pod because it has `kinit`, the
cluster `krb5.conf`, and access to alice's keytab secret.

```bash
# 1. Copy alice's keytab into the spark-master pod temporarily
kubectl cp prod/$(kubectl get secret alice-keytab -n prod \
    -o jsonpath='{.metadata.name}')/..data/keytab /tmp/alice.keytab 2>/dev/null || \
  kubectl exec -n prod deploy/kerberos-kdc -- \
    kadmin.local -q "ktadd -norandkey -k /tmp/alice-test.keytab alice@STARDATADBLABS.LOCAL" && \
  KDC_POD=$(kubectl get pod -n prod -l app=kerberos-kdc -o jsonpath='{.items[0].metadata.name}') && \
  kubectl cp prod/${KDC_POD}:/tmp/alice-test.keytab /tmp/alice-test.keytab

MASTER_POD=$(kubectl get pod -n prod -l app=spark,component=master \
  -o jsonpath='{.items[0].metadata.name}')
kubectl cp /tmp/alice-test.keytab prod/${MASTER_POD}:/tmp/alice.keytab

# 2. Obtain a Kerberos TGT using the keytab
kubectl exec -n prod deploy/spark-master -- bash -c "
  KRB5_CONFIG=/etc/krb5.conf.d/cluster.conf \
  kinit -kt /tmp/alice.keytab alice@STARDATADBLABS.LOCAL
  klist
"
# Expected:
#   Credentials cache: FILE:/tmp/krb5cc_0
#   Principal: alice@STARDATADBLABS.LOCAL
#   Issued    Expires   Principal
#   <date>    <date>    krbtgt/STARDATADBLABS.LOCAL@STARDATADBLABS.LOCAL

# 3. Produce a test message to the 'orders' topic via GSSAPI on port 9093
kubectl exec -n prod deploy/spark-master -- bash -c "
cat > /tmp/alice-gssapi.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=svc
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required \
  useTicketCache=true;
EOF

echo 'kerberos-test-message-from-alice' | \
  /opt/spark/jars/../bin/../bin/../bin/../bin/kafka-console-producer.sh \
    --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9093 \
    --topic orders \
    --producer.config /tmp/alice-gssapi.properties 2>&1
echo \"Produce exit: \$?\"
"
# Expected: Produce exit: 0
# Denied (no Ranger policy): ERROR_CODE=29 (NOT_AUTHORIZED) — add Ranger policy first (§6.3)
# Auth failed (bad keytab):  WARN SaslHandshakeException — check keytab with klist -kt
```

### 6.3 Create a Ranger policy for alice on Kafka

Ranger enforces what alice can do after the GSSAPI handshake succeeds.
Ranger sees username `alice` (realm stripped by `sasl.kerberos.principal.to.local.rules: DEFAULT`).

```bash
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${HOME}/openbao-init-keys.json'))['root_token'])")
RANGER_PASS=$(curl -sf \
  -H "X-Vault-Token: ${ROOT_TOKEN}" \
  "http://192.168.1.50:30820/v1/secret/data/ranger/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

curl -su "admin:${RANGER_PASS}" \
  -X POST http://192.168.1.50:30680/service/plugins/policies \
  -H "Content-Type: application/json" \
  -d '{
    "service":   "kafka",
    "name":      "alice-orders-gssapi",
    "isEnabled": true,
    "resources": {
      "topic": {"values":["orders"],"isExcludes":false,"isRecursive":false}
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
# Wait up to 30s for Ranger plugin to reload the policy cache
```

### 6.4 Test: produce (GSSAPI — full end-to-end)

```bash
MASTER_POD=$(kubectl get pod -n prod -l app=spark,component=master \
  -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n prod ${MASTER_POD} -- bash -c "
  # Ensure TGT is still valid (or re-kinit)
  klist 2>/dev/null | grep -q 'alice@STARDATADBLABS' || \
    kinit -kt /tmp/alice.keytab alice@STARDATADBLABS.LOCAL

cat > /tmp/alice-gssapi.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=svc
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useTicketCache=true;
EOF

  echo 'alice-produce-test-$(date +%s)' | \
    /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9093 \
      --topic orders \
      --producer.config /tmp/alice-gssapi.properties 2>&1
  echo \"Produce exit: \$?\"
"
# Expected: Produce exit: 0
# Denied:   ERROR [Producer] NOT_AUTHORIZED — Ranger policy not yet applied (wait 30s)
```

### 6.5 Test: consume (GSSAPI — full end-to-end)

```bash
kubectl exec -n prod ${MASTER_POD} -- bash -c "
  klist 2>/dev/null | grep -q 'alice@STARDATADBLABS' || \
    kinit -kt /tmp/alice.keytab alice@STARDATADBLABS.LOCAL

  /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9093 \
    --topic orders \
    --consumer.config /tmp/alice-gssapi.properties \
    --from-beginning --max-messages 3 --timeout-ms 10000 2>&1
  echo \"Consume exit: \$?\"
"
# Expected: 1–3 messages printed, then exit 0
# Denied:   WARN Not authorized to read from partition orders-0
```

### 6.6 Test: verify Kerberos identity in Ranger audit

```bash
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${HOME}/openbao-init-keys.json'))['root_token'])")
RANGER_PASS=$(curl -sf \
  -H "X-Vault-Token: ${ROOT_TOKEN}" \
  "http://192.168.1.50:30820/v1/secret/data/ranger/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

curl -su "admin:${RANGER_PASS}" \
  "http://192.168.1.50:30680/service/audit/access?serviceType=kafka&requestUser=alice&pageSize=10" | \
  python3 -c "
import sys, json
audits = json.load(sys.stdin).get('vXAccessAudits', [])
print(f'Ranger audit entries for alice on Kafka: {len(audits)}')
for a in audits[:5]:
    print(f'  {a[\"eventTime\"]}  {a[\"requestUser\"]}  {a[\"resourcePath\"]}  '\
          f'{a[\"action\"]}  {\"ALLOW\" if a[\"accessResult\"]==1 else \"DENY\"}')
"
# Expected: entries showing alice ALLOW publish/consume on topic=orders
# Via UI: http://192.168.1.50:30680 → Audit → Access → User=alice, Service=kafka
```

### 6.7 Test: confirm user without KDC principal is rejected (GSSAPI)

```bash
# Try kinit for a non-existent principal — must fail at the KDC
kubectl exec -n prod deploy/spark-master -- bash -c "
  KRB5_CONFIG=/etc/krb5.conf.d/cluster.conf \
  kinit -V newuser@STARDATADBLABS.LOCAL </dev/null 2>&1 || true
"
# Expected:
#   kinit: Client 'newuser@STARDATADBLABS.LOCAL' not found in Kerberos database
#
# Without a TGT, any GSSAPI connect attempt gives:
#   GSSException: No valid credentials provided
```

---

## 7. Doris — Testing a New Kerberos User (krb-doris-guard)

> **Auth model:** The `krb-doris-guard` sidecar intercepts every MySQL connection on
> port 9030/19030. It checks whether the connecting username exists as a KDC principal
> before the connection reaches Doris. A user whose principal is missing gets a MySQL
> `ERROR 1045` from the guard — Doris never sees the connection.

### 7.1 Architecture reminder

```
Client (mysql / JDBC)
  │
  ▼ port 9030 (Service → targetPort 19030)
krb-doris-guard sidecar
  ├─ extracts username from MySQL HandshakeResponse
  ├─ runs: kinit -V -n <username>@REALM </dev/null
  │         KDC says NOT FOUND → MySQL ERR 1045 → connection closed
  │         KDC says found     → proxy to Doris :9030
  ▼ port 9030 (loopback inside pod)
Doris FE — validates SQL password, then Ranger enforces policies
```

### 7.2 Prerequisites — verify SQL user exists

```bash
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${HOME}/openbao-init-keys.json'))['root_token'])")
DORIS_ROOT_PASS=$(curl -sf \
  -H "X-Vault-Token: ${ROOT_TOKEN}" \
  "http://192.168.1.50:30820/v1/secret/data/doris/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

# Connect via root (exempt from KDC check) and verify alice SQL user exists
kubectl exec -n prod statefulset/doris-fe -- \
  mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_ROOT_PASS}" \
  -e "SELECT user, host, authentication_string FROM mysql.user WHERE user='alice';" 2>/dev/null
# Expected: alice  %  <password_hash>
```

### 7.3 Test: connect as alice through the guard (KDC check + Doris auth)

```bash
# This goes through the guard: Service port 9030 → guard :19030 → Doris :9030
kubectl run doris-krb-test -n prod --rm -it --restart=Never \
  --image=mysql:8.0 \
  -- mysql -h doris-fe.prod.svc.cluster.local -P9030 \
     -ualice -p'AliceDoris1!' \
     -e "SELECT USER(), DATABASE();" 2>&1
# Expected:
#   USER()              DATABASE()
#   alice@<pod-ip>      (null)
#
# Guard log (kubectl logs -n prod doris-fe-0 -c krb-doris-guard --tail=5):
#   INFO Login attempt: user='alice' from ('10.x.x.x', xxxxx)
#   INFO ALLOWED user='alice' — forwarding to Doris
```

### 7.4 Test: confirm user without KDC principal is blocked by the guard

This is the key KDC enforcement test for Doris.

```bash
# 'newuser' has no KDC principal and no SQL user — guard blocks first
kubectl run doris-guard-test -n prod --rm -it --restart=Never \
  --image=mysql:8.0 \
  -- mysql -h doris-fe.prod.svc.cluster.local -P9030 \
     -unewuser -p'AnyPassword1!' \
     -e "SELECT 1;" 2>&1
# Expected:
#   ERROR 1045 (28000): Access denied for user 'newuser'@'%':
#   principal newuser@STARDATADBLABS.LOCAL not found in Kerberos KDC.
#   Create the principal first: kadmin.local -q "addprinc newuser@STARDATADBLABS.LOCAL"
#
# Guard log:
#   WARNING BLOCKED user='newuser' from (...) — not in KDC
```

### 7.5 Test: grant Ranger policy and verify SELECT works

```bash
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${HOME}/openbao-init-keys.json'))['root_token'])")
RANGER_PASS=$(curl -sf \
  -H "X-Vault-Token: ${ROOT_TOKEN}" \
  "http://192.168.1.50:30820/v1/secret/data/ranger/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

# Create a Ranger policy allowing alice SELECT on analytics.*
curl -su "admin:${RANGER_PASS}" \
  -X POST http://192.168.1.50:30680/service/plugins/policies \
  -H "Content-Type: application/json" \
  -d '{
    "service":   "doris",
    "name":      "alice-analytics-select",
    "isEnabled": true,
    "resources": {
      "database": {"values":["analytics"],"isExcludes":false,"isRecursive":false},
      "table":    {"values":["*"],"isExcludes":false,"isRecursive":false},
      "column":   {"values":["*"],"isExcludes":false,"isRecursive":false}
    },
    "policyItems": [{
      "users": ["alice"], "groups": [],
      "accesses": [{"type":"select","isAllowed":true}],
      "conditions": [], "delegateAdmin": false
    }]
  }'

# Wait 30s for Ranger plugin to reload, then test
sleep 30
kubectl run doris-select-test -n prod --rm -it --restart=Never \
  --image=mysql:8.0 \
  -- mysql -h doris-fe.prod.svc.cluster.local -P9030 \
     -ualice -p'AliceDoris1!' \
     -e "SELECT * FROM analytics.orders LIMIT 3;" 2>&1
# Expected: rows returned
# Denied:   ERROR 1105 (HY000): Access denied; user alice has no privilege
```

### 7.6 Test: INSERT is denied by Ranger (not in policy)

```bash
kubectl run doris-insert-test -n prod --rm -it --restart=Never \
  --image=mysql:8.0 \
  -- mysql -h doris-fe.prod.svc.cluster.local -P9030 \
     -ualice -p'AliceDoris1!' \
     -e "INSERT INTO analytics.orders VALUES (9999, 'test');" 2>&1
# Expected: ERROR 1105 (HY000): Access denied (Ranger: DENY — insert not in policy)
```

### 7.7 Verify Ranger audit for Doris

```bash
curl -su "admin:${RANGER_PASS}" \
  "http://192.168.1.50:30680/service/audit/access?serviceType=hive&requestUser=alice&pageSize=10" | \
  python3 -c "
import sys, json
audits = json.load(sys.stdin).get('vXAccessAudits', [])
print(f'Ranger Doris audit entries for alice: {len(audits)}')
for a in audits[:5]:
    print(f'  {a[\"eventTime\"]}  {\"ALLOW\" if a[\"accessResult\"]==1 else \"DENY\"}  '\
          f'{a[\"action\"]}  {a[\"resourcePath\"]}')
"
# Expected: ALLOW entries for SELECT, DENY entry for INSERT
# Via UI: http://192.168.1.50:30680 → Audit → Access → User=alice, Service=doris
```

---

## 8. Spark — Testing a New Kerberos User (krb-spark-guard)

> **Auth model:** The `krb-spark-guard` sidecar controls port 7077 (Spark RPC).
> Users must authenticate via the guard HTTP API (port 7078) with their keytab.
> The guard verifies the keytab against the KDC, issues a short-lived token,
> and the `spark-submit-krb` wrapper injects that token as the first TCP line.
> Without a valid KDC keytab the connection to port 7077 is rejected.

### 8.1 Architecture reminder

```
spark-submit-krb --principal alice@REALM --keytab alice.keytab [...]
    │
    ├─1─ POST :7078/auth  {username:"alice", keytab_b64:"<base64>"}
    │         Guard runs: kinit -kt <keytab> alice@REALM
    │         ✅ KDC accepts → token issued (TTL 300s)
    │         ❌ principal not in KDC → HTTP 403
    │
    └─2─ TCP :7077  first line: "X-Krb-Token: <token>\n"
              ✅ token valid → forward to Spark master :17077
              ❌ missing header / no token → rejected
```

### 8.2 Prerequisites — verify alice keytab exists and guard is running

```bash
# alice-keytab secret must exist
kubectl get secret alice-keytab -n prod -o jsonpath='{.metadata.name}' && echo " OK"
# Expected: alice-keytab OK

# Guard must be running
kubectl get pod -n prod -l app=spark,component=master \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{range .status.containerStatuses[*]}  {.name} ready={.ready}{"\n"}{end}{end}'
# Expected:
#   spark-master-<hash>
#     krb-spark-guard ready=true
#     spark-master ready=true

# Guard logs should show both servers started
kubectl logs -n prod -l app=spark,component=master -c krb-spark-guard --tail=5
# Expected:
#   INFO krb-spark-guard RPC proxy on :7077 → spark master 127.0.0.1:17077
#   INFO krb-spark-guard HTTP auth API on :7078 — realm STARDATADBLABS.LOCAL
```

### 8.3 Step-by-step: authenticate alice against the guard

```bash
# 1. Get alice's keytab bytes (from the K8s secret)
kubectl get secret alice-keytab -n prod \
  -o jsonpath='{.data.keytab}' > /tmp/alice-keytab-b64.txt
# The secret value is already base64-encoded — use it directly

ALICE_KEYTAB_B64=$(cat /tmp/alice-keytab-b64.txt)

# 2. Call the guard HTTP auth API — guard verifies keytab against KDC
AUTH_RESPONSE=$(curl -sf \
  -X POST \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"alice\",\"keytab_b64\":\"${ALICE_KEYTAB_B64}\"}" \
  "http://192.168.1.50:30778/auth")
echo "${AUTH_RESPONSE}"
# Expected:
#   {"token": "abc123...", "expires_in": 300}
#
# Guard log:
#   INFO AUTH ALLOWED user='alice' from (...) — token issued (TTL 300s)
#
# If principal NOT in KDC:
#   HTTP 403: {"error": "Principal alice@STARDATADBLABS.LOCAL does not exist in KDC",
#              "hint": "Create the principal: kadmin.local -q \"addprinc alice@REALM\""}

# 3. Extract the token
ALICE_TOKEN=$(echo "${AUTH_RESPONSE}" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")
echo "Token: ${ALICE_TOKEN}"
```

### 8.4 Test: submit a Spark job via spark-submit-krb

The `spark-submit-krb` wrapper (installed in the guard image) handles steps 8.3 and
the token injection automatically.

```bash
# Copy alice's keytab to the spark-master pod
kubectl cp /tmp/alice-keytab-b64.txt prod/$(kubectl get pod -n prod \
  -l app=spark,component=master -o jsonpath='{.items[0].metadata.name}'):/tmp/alice-keytab-b64.txt

kubectl exec -n prod deploy/spark-master -- bash -c "
  # Decode keytab from base64
  base64 -d /tmp/alice-keytab-b64.txt > /tmp/alice.keytab

  # spark-submit-krb calls the guard auth API, injects token, rewrites --master
  SPARK_GUARD_HOST=spark-master-svc.prod.svc.cluster.local \
  SPARK_GUARD_PORT=7078 \
  SPARK_SUBMIT=/opt/spark/bin/spark-submit \
  /usr/local/bin/spark-submit-krb \
    --principal alice@STARDATADBLABS.LOCAL \
    --keytab /tmp/alice.keytab \
    --master spark://spark-master-svc.prod.svc.cluster.local:7077 \
    --class org.apache.spark.examples.SparkPi \
    /opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar 10 2>&1
"
# Expected:
#   [spark-submit-krb] Authenticating alice@STARDATADBLABS.LOCAL against KDC via guard...
#   [spark-submit-krb] KDC authentication OK for alice@STARDATADBLABS.LOCAL — token issued.
#   [spark-submit-krb] Submitting job as alice@STARDATADBLABS.LOCAL to spark://...:7077
#   ...
#   Pi is roughly 3.14...
```

### 8.5 Test: confirm user without KDC principal is blocked by the guard

```bash
# newuser has no KDC principal — guard rejects at the auth API
curl -sf \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"username":"newuser","keytab_b64":"aW52YWxpZA=="}' \
  "http://192.168.1.50:30778/auth" | python3 -m json.tool
# Expected HTTP 403:
# {
#   "error": "Principal newuser@STARDATADBLABS.LOCAL does not exist in KDC",
#   "hint": "Create the principal: kadmin.local -q \"addprinc newuser@REALM\"\n
#            Then export keytab: kadmin.local -q \"ktadd -k /tmp/newuser.keytab ...\""
# }
#
# Guard log:
#   WARNING AUTH DENIED user='newuser' from (...): Principal ... does not exist in KDC

# Attempting to connect to port 7077 directly (no token) is also blocked
kubectl run spark-raw-test -n prod --rm --restart=Never \
  --image=busybox -- sh -c \
  "echo 'hello' | nc spark-master-svc.prod.svc.cluster.local 7077; echo exit \$?" 2>&1
# Expected output contains:
#   [krb-spark-guard] REJECTED: first bytes are not an X-Krb-Token header.
```

### 8.6 Test: Spark job data access via Kafka GSSAPI (end-to-end Kerberos chain)

With a valid token the job runs under alice's Kerberos identity. Any data source
access (Kafka GSSAPI, OpenSearch SPNEGO) uses alice's TGT — Ranger enforces policy.

```bash
kubectl exec -n prod deploy/spark-master -- bash -c "
  base64 -d /tmp/alice-keytab-b64.txt > /tmp/alice.keytab

  SPARK_GUARD_HOST=spark-master-svc.prod.svc.cluster.local \
  SPARK_GUARD_PORT=7078 \
  SPARK_SUBMIT=/opt/spark/bin/spark-submit \
  /usr/local/bin/spark-submit-krb \
    --principal alice@STARDATADBLABS.LOCAL \
    --keytab /tmp/alice.keytab \
    --master spark://spark-master-svc.prod.svc.cluster.local:7077 \
    --conf spark.kerberos.enabled=true \
    --conf spark.kerberos.principal=alice@STARDATADBLABS.LOCAL \
    --conf spark.kerberos.keytab=/tmp/alice.keytab \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    --class org.apache.spark.examples.SparkPi \
    /opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar 5 2>&1 | tail -5
"
# The job authenticates as alice@STARDATADBLABS.LOCAL
# Ranger Kafka audit will show alice producing/consuming under GSSAPI identity
```

---

## 9. OpenSearch — Testing a New Kerberos User (SPNEGO)

> **Auth model:** OpenSearch accepts **SPNEGO only** for HTTP clients.
> HTTP Basic auth is disabled. A user must have a valid Kerberos TGT to connect.
> The admin user connects via TLS client cert (`securityadmin.sh`), not Basic auth.

### 9.1 Prerequisites — verify alice is registered in OpenSearch

```bash
# Check alice exists in OpenSearch internal users (applied via securityadmin.sh in §5)
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  /usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh \
    -backup /tmp/os-verify -icl -nhnv \
    -cacert /usr/share/opensearch/config/tls/root-ca.pem \
    -cert   /usr/share/opensearch/config/tls/admin.pem \
    -key    /usr/share/opensearch/config/tls/admin-key.pem \
    -h localhost -p 9200 2>/dev/null
  python3 -c \"
import yaml
with open('/tmp/os-verify/internal_users.yml') as f:
    u = yaml.safe_load(f)
if 'alice' in u:
    print('alice found — backend_roles:', u['alice'].get('backend_roles','?'))
else:
    print('alice NOT found — run §5 Step 5 to register her')
\"
"
# Expected: alice found — backend_roles: ['readall']
```

### 9.2 Step-by-step: obtain a TGT and authenticate via SPNEGO

Run from `master.local` (which has `kinit` and can reach the KDC).

```bash
# 1. Write cluster krb5.conf to local file
kubectl get cm kerberos-integration-config -n prod \
  -o jsonpath='{.data.krb5\.conf}' > /tmp/krb5-cluster.conf

# 2. Decode alice's keytab from the K8s secret
kubectl get secret alice-keytab -n prod \
  -o jsonpath='{.data.keytab}' | base64 -d > /tmp/alice.keytab

# 3. Obtain a TGT using the keytab (non-interactive)
KRB5_CONFIG=/tmp/krb5-cluster.conf \
  kinit -kt /tmp/alice.keytab alice@STARDATADBLABS.LOCAL

# Verify TGT was issued
klist
# Expected:
#   Credentials cache: FILE:/tmp/krb5cc_<uid>
#   Principal: alice@STARDATADBLABS.LOCAL
#   Issued    Expires   Principal
#   <date>    <date>    krbtgt/STARDATADBLABS.LOCAL@STARDATADBLABS.LOCAL

# 4. Test SPNEGO auth against OpenSearch cluster health
KRB5_CONFIG=/tmp/krb5-cluster.conf \
  curl -sk --negotiate -u : \
  "https://192.168.1.53:30920/_cluster/health?pretty"
# Expected:
#   {
#     "cluster_name": "opensearch-prod",
#     "status": "green",
#     ...
#   }
# If 401: SPNEGO not negotiated — check krb5.conf points to correct KDC
# If 403: alice authenticated but lacks permission — check backend_roles
```

### 9.3 Test: HTTP Basic is rejected (SPNEGO-only mode)

With Basic auth disabled, password-based access must return 401.

```bash
# Attempt HTTP Basic — must be rejected even with correct password
curl -sk -o /dev/null -w "HTTP %{http_code}\n" \
  -u "alice:AliceDoris1!" \
  "https://192.168.1.53:30920/_cluster/health"
# Expected: HTTP 401
# (Basic auth is disabled — only SPNEGO/Negotiate is accepted)
```

### 9.4 Test: read an index via SPNEGO (alice has readall role)

```bash
# List indices
KRB5_CONFIG=/tmp/krb5-cluster.conf \
  curl -sk --negotiate -u : \
  "https://192.168.1.53:30920/_cat/indices?v&h=index,docs.count,store.size"
# Expected: list of index names with doc counts

# Search orders index
KRB5_CONFIG=/tmp/krb5-cluster.conf \
  curl -sk --negotiate -u : \
  "https://192.168.1.53:30920/orders/_search?pretty&size=3" | \
  python3 -c "
import sys,json
d=json.load(sys.stdin)
hits=d.get('hits',{}).get('hits',[])
print(f'Returned {len(hits)} hits (readall role allows search)')
"
# Expected: Returned N hits (readall role allows search)
```

### 9.5 Test: write is denied by readall role

```bash
KRB5_CONFIG=/tmp/krb5-cluster.conf \
  curl -sk --negotiate -u : \
  -o /dev/null -w "HTTP %{http_code}\n" \
  -X PUT "https://192.168.1.53:30920/orders/_doc/9999" \
  -H "Content-Type: application/json" \
  -d '{"test":"alice-write-spnego"}'
# Expected: HTTP 403
# (readall role does not include write permission)
```

### 9.6 Test: confirm user without KDC principal is rejected (SPNEGO)

```bash
# No TGT in cache for newuser — SPNEGO has nothing to present
kdestroy 2>/dev/null; klist 2>&1 || true
# Expected: klist: No credentials cache found

# Attempt with no ticket — must return 401
curl -sk -o /dev/null -w "HTTP %{http_code}\n" \
  --negotiate -u : \
  "https://192.168.1.53:30920/_cluster/health"
# Expected: HTTP 401
# OpenSearch returns: {"error":{"reason":"Authentication finally failed"},"status":401}

# Attempt with a made-up password (Basic auth is disabled) — also 401
curl -sk -o /dev/null -w "HTTP %{http_code}\n" \
  -u "newuser:WrongPass1!" \
  "https://192.168.1.53:30920/_cluster/health"
# Expected: HTTP 401
```

### 9.7 Upgrade alice's role to allow writes

```bash
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  /usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh \
    -backup /tmp/os-upgrade -icl -nhnv \
    -cacert /usr/share/opensearch/config/tls/root-ca.pem \
    -cert   /usr/share/opensearch/config/tls/admin.pem \
    -key    /usr/share/opensearch/config/tls/admin-key.pem \
    -h localhost -p 9200 2>/dev/null

  python3 - <<'PYEOF'
import yaml
with open('/tmp/os-upgrade/internal_users.yml') as f:
    users = yaml.safe_load(f)
users['alice']['backend_roles'] = ['readall', 'readall_and_monitor']
with open('/tmp/os-upgrade/internal_users.yml', 'w') as f:
    yaml.dump(users, f, default_flow_style=False)
print('alice backend_roles updated')
PYEOF

  /usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh \
    -f /tmp/os-upgrade/internal_users.yml -t internalusers -icl -nhnv \
    -cacert /usr/share/opensearch/config/tls/root-ca.pem \
    -cert   /usr/share/opensearch/config/tls/admin.pem \
    -key    /usr/share/opensearch/config/tls/admin-key.pem \
    -h localhost -p 9200 2>&1 | tail -3
"
# Expected: Done with success
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
