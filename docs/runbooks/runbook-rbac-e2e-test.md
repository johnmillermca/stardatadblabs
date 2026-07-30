# RBAC End-to-End Test Runbook

**Platform:** K8s Data Platform — SOC2 RBAC (Kerberos + Apache Ranger)  
**Realm:** `STARDATADBLABS.LOCAL`  
**Namespace:** `prod`  
**Ranger UI:** `http://192.168.1.50:30680`

---

## Overview

This runbook validates the complete RBAC lifecycle across all three security zones:

| Zone | Services | Admin Group | Dev Group |
|---|---|---|---|
| `CACHING_ZONE` | `doris_service`, `polaris_service` | `caching_admin` | `caching_dev` |
| `PROCESSING_ZONE` | `spark_service`, `sqlmesh_service`, `kestra_service`, `opensearch_service`, `polaris_service` | `processing_admin` | `processing_dev` |
| `STREAMING_ZONE` | `kafka_service`, `schema_registry_service`, `debezium_service`, `akhq_service` | `streaming_admin` | `streaming_dev` |

**Test coverage:**

1. Prerequisites — secrets, pods, principals
2. Automated verify script
3. Ranger UI — zones, policies, masking
4. Doris — access control and column masking
5. Polaris — external catalog (CACHING_ZONE) + REST catalog (PROCESSING_ZONE)
6. Kafka — topic produce/consume RBAC
7. Schema Registry — subject access
8. OpenSearch — index access
9. Kestra — workflow access
10. Cross-zone — account_admin superuser validation

---

## 0. Setup — Read Credentials

All tests below use these variables. Run once at the start of each session.

```bash
# Ranger admin
RANGER_PASS=$(kubectl get secret ranger-db-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
RANGER_AUTH="admin:${RANGER_PASS}"

# Doris root
DORIS_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)

# RBAC persona passwords (raw from secret, without Ranger suffix)
CA_PASS_RAW=$(kubectl get secret rbac-users -n prod \
  -o jsonpath='{.data.caching_admin_user-password}' | base64 -d | tr -d '\n\r')
CD_PASS_RAW=$(kubectl get secret rbac-users -n prod \
  -o jsonpath='{.data.caching_dev_user-password}' | base64 -d | tr -d '\n\r')
SA_PASS_RAW=$(kubectl get secret rbac-users -n prod \
  -o jsonpath='{.data.streaming_admin_user-password}' | base64 -d | tr -d '\n\r')
SD_PASS_RAW=$(kubectl get secret rbac-users -n prod \
  -o jsonpath='{.data.streaming_dev_user-password}' | base64 -d | tr -d '\n\r')

# Ranger appends "Aa1!" suffix for password complexity — use for Doris user logins
CA_PASS="${CA_PASS_RAW}Aa1!"; CA_PASS="${CA_PASS:0:28}"
CD_PASS="${CD_PASS_RAW}Aa1!"; CD_PASS="${CD_PASS:0:28}"
SA_PASS="${SA_PASS_RAW}Aa1!"; SA_PASS="${SA_PASS:0:28}"
SD_PASS="${SD_PASS_RAW}Aa1!"; SD_PASS="${SD_PASS:0:28}"

# Pod references
FE_POD=$(kubectl get pod -n prod -l app=doris-fe \
  -o jsonpath='{.items[0].metadata.name}')
KDC_POD=$(kubectl get pod -n prod -l app=kerberos-kdc \
  -o jsonpath='{.items[0].metadata.name}')

echo "FE_POD=${FE_POD}  KDC_POD=${KDC_POD}"
```

---

## 1. Prerequisites

### 1.1 All pods running

```bash
kubectl get pods -n prod --no-headers \
  | awk '{print $1, $3}' \
  | column -t
```

**Expected:** All pods in `Running` or `Completed` state. No `CrashLoopBackOff`.

Key pods to confirm:

| Pod (prefix) | Label |
|---|---|
| `doris-fe-0` | `app=doris-fe` |
| `doris-be-*` | `app=doris-be` |
| `ranger-admin-*` | `app=ranger-admin` |
| `kerberos-kdc-*` | `app=kerberos-kdc` |

### 1.2 Required K8s secrets exist

```bash
for secret in ranger-db-credentials doris-credentials rbac-users \
              doris-keytab spark-keytab kafka-keytab polaris-keytab \
              schema-registry-keytab debezium-keytab akhq-keytab \
              opensearch-keytab kestra-keytab sqlmesh-keytab; do
  kubectl get secret "$secret" -n prod -o name 2>/dev/null \
    && echo "  ✓ $secret" \
    || echo "  ✗ MISSING: $secret"
done
```

**If any are missing:** Re-run `bash scripts/master/12-seed-openbao-secrets.sh` then `bash scripts/master/15-seed-rbac-principals.sh`.

### 1.3 Ranger reachable

```bash
curl -sf -u "${RANGER_AUTH}" \
  http://192.168.1.50:30680/service/public/v2/api/service \
  -o /dev/null && echo "Ranger OK" || echo "Ranger UNREACHABLE"
```

---

## 2. Automated Verify Script

Run the full automated check first. All subsequent manual steps dig into any remaining failures.

```bash
bash scripts/master/17-verify-rbac.sh 2>&1
```

**Expected result:**

```
Results: 73/73 passed  |  0 failed
✓ All RBAC checks passed. Platform is SOC2-ready.
```

**If failures remain:** Continue with the relevant section below to diagnose manually.

---

## 3. Ranger UI Validation

Open `http://192.168.1.50:30680` and login as `admin`.

### 3.1 Security zones

Navigate to **Security Zone** (top-right gear icon → Security Zone).

| Zone | Expected services |
|---|---|
| `CACHING_ZONE` | `doris_service`, `polaris_service` |
| `PROCESSING_ZONE` | `spark_service`, `sqlmesh_service`, `kestra_service`, `opensearch_service`, `polaris_service` |
| `STREAMING_ZONE` | `kafka_service`, `schema_registry_service`, `debezium_service`, `akhq_service` |

Or verify via API:

```bash
curl -sf -u "${RANGER_AUTH}" \
  http://192.168.1.50:30680/service/public/v2/api/zones \
  | python3 -c "
import sys, json
for z in json.load(sys.stdin):
    svcs = list(z.get('services', {}).keys())
    print(f\"  {z['name']} (id={z['id']}): {', '.join(svcs)}\")
"
```

### 3.2 Policy count per service

```bash
for svc in doris_service spark_service sqlmesh_service kestra_service \
           opensearch_service polaris_service kafka_service \
           schema_registry_service debezium_service akhq_service; do
  # Check both zone and non-zone policies
  for zone in CACHING_ZONE PROCESSING_ZONE STREAMING_ZONE ""; do
    zq="${zone:+&zoneName=${zone}}"
    count=$(curl -sf -u "${RANGER_AUTH}" \
      "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=${svc}${zq}&pageSize=500" \
      | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
print(len(ps))" 2>/dev/null)
    [[ "${count}" -gt 0 ]] && \
      echo "  ${svc} [${zone:-no-zone}]: ${count} policy(ies)"
  done
done
```

### 3.3 Column masking policies (Doris)

```bash
curl -sf -u "${RANGER_AUTH}" \
  "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=doris_service&zoneName=CACHING_ZONE&pageSize=500" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
ps = d if isinstance(d, list) else d.get('policies', [])
for p in ps:
    if p.get('policyType') == 1:
        cols = p.get('resources', {}).get('column', {}).get('values', [])
        items = p.get('dataMaskPolicyItems', [])
        mask = items[0].get('dataMaskInfo', {}).get('dataMaskType', '') if items else ''
        print(f\"  {p['name']}: column={cols}  mask={mask}\")
"
```

**Expected output:**

```
  caching-dev-mask-email:   column=['email']      mask=MASK_SHOW_FIRST_4
  caching-dev-mask-user-id: column=['user_id']    mask=MASK_HASH
  caching-dev-mask-amount:  column=['amount', 'total_spend']  mask=MASK_NULL
```

### 3.4 Users and group membership

```bash
# Users exist (Ranger stores REST-created users outside the list endpoint)
for uid in 7 8 9 10 11 12 13; do
  curl -sf -u "${RANGER_AUTH}" \
    "http://192.168.1.50:30680/service/xusers/users/${uid}" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); \
      print(f\"  id={d['id']}  name={d['name']}  role={d.get('userRoleList',['?'])[0]}\")" \
    2>/dev/null || echo "  id=${uid} not found"
done
```

**Expected:**

```
  id=7   name=platform_admin        role=*****
  id=8   name=caching_admin_user    role=*****
  id=9   name=caching_dev_user      role=*****
  id=10  name=processing_admin_user role=*****
  id=11  name=processing_dev_user   role=*****
  id=12  name=streaming_admin_user  role=*****
  id=13  name=streaming_dev_user    role=*****
```

---

## 4. Kerberos Principal Validation

```bash
kubectl exec -n prod "${KDC_POD}" -- \
  kadmin.local -q "listprincs" 2>/dev/null \
  | sort \
  | grep -E 'svc/|_admin|_dev'
```

**Expected principals:**

```
caching_admin_user@STARDATADBLABS.LOCAL
caching_dev_user@STARDATADBLABS.LOCAL
platform_admin@STARDATADBLABS.LOCAL
processing_admin_user@STARDATADBLABS.LOCAL
processing_dev_user@STARDATADBLABS.LOCAL
streaming_admin_user@STARDATADBLABS.LOCAL
streaming_dev_user@STARDATADBLABS.LOCAL
svc/akhq@STARDATADBLABS.LOCAL
svc/debezium@STARDATADBLABS.LOCAL
svc/doris@STARDATADBLABS.LOCAL
svc/kafka@STARDATADBLABS.LOCAL
svc/kestra@STARDATADBLABS.LOCAL
svc/opensearch@STARDATADBLABS.LOCAL
svc/polaris@STARDATADBLABS.LOCAL
svc/schema-registry@STARDATADBLABS.LOCAL
svc/spark@STARDATADBLABS.LOCAL
svc/sqlmesh@STARDATADBLABS.LOCAL
```

**Verify keytab secrets are non-empty:**

```bash
for secret in doris-keytab spark-keytab kafka-keytab polaris-keytab; do
  size=$(kubectl get secret "${secret}" -n prod \
    -o jsonpath='{.data.keytab}' | base64 -d | wc -c)
  echo "  ${secret}: ${size} bytes"
done
```

**Expected:** Each keytab > 0 bytes.

---

## 5. CACHING_ZONE — Apache Doris

### 5.1 Ensure test users exist in Doris

Doris manages its own user store independently of Ranger. Create the test personas if not already present:

```bash
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${DORIS_PASS}" -e "
    CREATE USER IF NOT EXISTS 'caching_admin_user'@'%' IDENTIFIED BY '${CA_PASS}';
    CREATE USER IF NOT EXISTS 'caching_dev_user'@'%'   IDENTIFIED BY '${CD_PASS}';
    GRANT ALL ON *.* TO 'caching_admin_user'@'%';
  " 2>/dev/null
echo "Doris users created/verified"
```

### 5.2 Create test schema (as root)

```bash
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${DORIS_PASS}" -e "
    CREATE DATABASE IF NOT EXISTS analytics;
    USE analytics;
    CREATE TABLE IF NOT EXISTS users (
      user_id   BIGINT,
      email     VARCHAR(200),
      username  VARCHAR(100)
    ) DISTRIBUTED BY HASH(user_id) BUCKETS 1
    PROPERTIES ('replication_num' = '1');

    CREATE TABLE IF NOT EXISTS events (
      event_id  BIGINT,
      user_id   BIGINT,
      amount    DECIMAL(10,2),
      ts        DATETIME
    ) DISTRIBUTED BY HASH(event_id) BUCKETS 1
    PROPERTIES ('replication_num' = '1');

    INSERT INTO users VALUES (1, 'alice@example.com', 'alice');
    INSERT INTO users VALUES (2, 'bob@example.com',   'bob');
    INSERT INTO events VALUES (100, 1, 99.99, NOW());
    INSERT INTO events VALUES (101, 2, 19.50, NOW());
  " 2>/dev/null
echo "Test data loaded"
```

### 5.3 caching_admin — full access

```bash
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u caching_admin_user -p"${CA_PASS}" \
    -e "SHOW DATABASES; USE analytics; SELECT * FROM users;" 2>&1 \
  | grep -v Warning
```

**Expected:** Lists databases including `analytics`; returns both user rows with real email values.

```bash
# DDL access — create and drop a test table
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u caching_admin_user -p"${CA_PASS}" \
    -e "USE analytics; CREATE TABLE IF NOT EXISTS rbac_test (id INT)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num'='1');
        DROP TABLE IF EXISTS rbac_test;" 2>&1 \
  | grep -v Warning
```

**Expected:** No errors.

### 5.4 caching_dev — SELECT allowed, INSERT denied

```bash
# SELECT — should succeed
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u caching_dev_user -p"${CD_PASS}" \
    -e "USE analytics; SELECT user_id, email FROM users;" 2>&1 \
  | grep -v Warning
```

**Expected:** Returns rows. If Ranger plugin is active, `email` column shows `alic**` (MASK_SHOW_FIRST_4).

```bash
# INSERT — should be denied (or fail: table/db not accessible)
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u caching_dev_user -p"${CD_PASS}" \
    -e "USE analytics; INSERT INTO users VALUES (99, 'hacker@bad.com', 'hacker');" 2>&1 \
  | grep -v Warning
```

**Expected:** Error containing `denied`, `permission`, or `Access denied`. The dev persona must not write.

```bash
# DDL — must be denied
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u caching_dev_user -p"${CD_PASS}" \
    -e "USE analytics; DROP TABLE users;" 2>&1 \
  | grep -v Warning
```

**Expected:** Error — dev cannot drop tables.

### 5.5 Column masking spot-check (Ranger plugin required)

> **Note:** Column masking is enforced by the Ranger Hive plugin running inside Doris. In this lab the plugin is registered but not yet embedded in Doris. The policies are correct in Ranger — this test confirms them when the plugin is active.

```bash
# Confirm masking policies exist
curl -sf -u "${RANGER_AUTH}" \
  "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=doris_service&zoneName=CACHING_ZONE&pageSize=500" \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
masks=[p for p in ps if p.get('policyType')==1]
print(f'  {len(masks)} masking policies found')
for p in masks:
    print(f\"  • {p['name']}\")
"
```

**Expected:** 3 masking policies (`caching-dev-mask-email`, `caching-dev-mask-user-id`, `caching-dev-mask-amount`).

---

## 6. CACHING_ZONE — Polaris REST Catalog

Doris uses the Polaris REST API to create external Iceberg catalogs. The `polaris_service` is registered in both `CACHING_ZONE` and `PROCESSING_ZONE`.

### 6.1 Polaris API reachable

```bash
curl -sf http://192.168.1.50:30181/api/catalog/v1/config \
  | python3 -m json.tool 2>/dev/null \
  || echo "Polaris unreachable — check pod in catalog namespace"
```

**Expected:** JSON config response with `defaults` and `overrides`.

### 6.2 Ranger policies on polaris_service exist in CACHING_ZONE

```bash
curl -sf -u "${RANGER_AUTH}" \
  "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=polaris_service&zoneName=CACHING_ZONE&pageSize=100" \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    groups=[g for item in p.get('policyItems',[]) for g in item.get('groups',[])]
    print(f\"  {p['name']}  groups={groups}\")
"
```

**Expected:**

```
  caching-admin-polaris-api  groups=['caching_admin', 'account_admin', 'caching_dev']
```

### 6.3 Doris external catalog creation (as caching_admin)

```bash
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u caching_admin_user -p"${CA_PASS}" -e "
    CREATE CATALOG IF NOT EXISTS iceberg_polaris
    PROPERTIES (
      'type'                      = 'iceberg',
      'iceberg.catalog.type'      = 'rest',
      'uri'                       = 'http://polaris-rest.catalog.svc.cluster.local:8181/api/catalog',
      'warehouse'                 = 's3://warehouse/iceberg'
    );
    SHOW CATALOGS;
  " 2>&1 | grep -v Warning
```

**Expected:** `iceberg_polaris` appears in the catalog list.

### 6.4 Ranger policy on doris_service for iceberg_polaris database

```bash
curl -sf -u "${RANGER_AUTH}" \
  "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=doris_service&zoneName=CACHING_ZONE&pageSize=500" \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    dbs=p.get('resources',{}).get('database',{}).get('values',[])
    if 'iceberg_polaris' in dbs:
        groups=[g for item in p.get('policyItems',[]) for g in item.get('groups',[])]
        print(f\"  {p['name']}  db={dbs}  groups={groups}\")
"
```

**Expected:**

```
  caching-admin-polaris-catalog  db=['iceberg_polaris']  groups=['caching_admin', 'account_admin', 'caching_dev']
```

---

## 7. STREAMING_ZONE — Apache Kafka

The `kafka_service` is registered as native `kafka` type (not `tag`). Policies use `topic` and `consumergroup` resources.

### 7.1 Verify Kafka policies in Ranger

```bash
curl -sf -u "${RANGER_AUTH}" \
  "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=kafka_service&zoneName=STREAMING_ZONE&pageSize=100" \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    res=list(p.get('resources',{}).keys())
    groups=[g for item in p.get('policyItems',[]) for g in item.get('groups',[])]
    accesses=[a['type'] for item in p.get('policyItems',[]) for a in item.get('accesses',[])]
    print(f\"  {p['name']}  resource={res}  groups={set(groups)}  accesses={set(accesses)}\")
"
```

**Expected (3 policies):**

```
  streaming-admin-kafka-all            resource=['topic']         groups={'streaming_admin','account_admin','streaming_dev'}
  streaming-admin-kafka-consumergroup  resource=['consumergroup'] groups={'streaming_admin','account_admin'}
  streaming-dev-kafka-consumergroup    resource=['consumergroup'] groups={'streaming_dev'}
```

### 7.2 Kafka topic list (admin access)

```bash
KAFKA_PASS=$(kubectl get secret kafka-app-user -n prod \
  -o jsonpath='{.data.password}' | base64 -d | tr -d '\n')

kubectl run kafka-test --rm -it --restart=Never -n prod \
  --image=bitnami/kafka:3.9 \
  --env="BOOTSTRAP=strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092" \
  --env="KAFKA_PASS=${KAFKA_PASS}" \
  -- bash -c '
    echo "security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-app-user\" password=\"${KAFKA_PASS}\";" > /tmp/client.props

    kafka-topics.sh --bootstrap-server ${BOOTSTRAP} \
      --command-config /tmp/client.props \
      --list
  ' 2>/dev/null
```

**Expected:** Topic list (including `_schemas` and any CDC topics).

### 7.3 Produce and consume a message (streaming_admin)

```bash
# Create test topic as admin
kubectl run kafka-admin --rm -it --restart=Never -n prod \
  --image=bitnami/kafka:3.9 \
  --env="BOOTSTRAP=strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092" \
  --env="KAFKA_PASS=${KAFKA_PASS}" \
  -- bash -c '
    echo "security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-app-user\" password=\"${KAFKA_PASS}\";" > /tmp/client.props

    kafka-topics.sh --bootstrap-server ${BOOTSTRAP} \
      --command-config /tmp/client.props \
      --create --if-not-exists \
      --topic rbac-test-topic \
      --partitions 1 \
      --replication-factor 1

    echo "rbac-test-message" | kafka-console-producer.sh \
      --bootstrap-server ${BOOTSTRAP} \
      --producer.config /tmp/client.props \
      --topic rbac-test-topic

    kafka-console-consumer.sh \
      --bootstrap-server ${BOOTSTRAP} \
      --consumer.config /tmp/client.props \
      --topic rbac-test-topic \
      --from-beginning \
      --max-messages 1 \
      --timeout-ms 5000
  ' 2>/dev/null
```

**Expected:** Produces and consumes `rbac-test-message` without error.

### 7.4 AKHQ UI access

```bash
curl -sf http://192.168.1.50:30808 -o /dev/null -w "HTTP %{http_code}\n"
```

**Expected:** `HTTP 200`.

Navigate to `http://192.168.1.50:30808` — confirm topic list loads and `rbac-test-topic` appears.

---

## 8. STREAMING_ZONE — Schema Registry

### 8.1 Schema Registry reachable

```bash
curl -sf http://192.168.1.54:30810/subjects | python3 -m json.tool
```

**Expected:** JSON array of subject names (may be empty `[]` if no schemas registered).

### 8.2 Ranger policy on schema_registry_service

```bash
curl -sf -u "${RANGER_AUTH}" \
  "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=schema_registry_service&zoneName=STREAMING_ZONE&pageSize=100" \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    groups=[g for item in p.get('policyItems',[]) for g in item.get('groups',[])]
    print(f\"  {p['name']}  groups={groups}\")
"
```

**Expected:** `schema_registry_service-admin-all` with `streaming_admin`, `account_admin`, `streaming_dev`.

### 8.3 Register and retrieve a test schema

```bash
# Register
curl -sf -X POST \
  http://192.168.1.54:30810/subjects/rbac-test-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"RbacTest\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"msg\",\"type\":\"string\"}]}"}' \
  | python3 -m json.tool

# Retrieve
curl -sf http://192.168.1.54:30810/subjects/rbac-test-value/versions/latest \
  | python3 -m json.tool
```

**Expected:** Schema registered with an `id` returned; retrieval shows `schema`, `version`, `id`.

---

## 9. PROCESSING_ZONE — OpenSearch

> **Note:** OpenSearch security plugin is disabled in this lab. Ranger policies are registered for policy management but are not enforced at the API level until the plugin is installed.

### 9.1 OpenSearch cluster health

```bash
curl -sf http://192.168.1.50:30920/_cluster/health?pretty \
  | python3 -c "import sys,json; d=json.load(sys.stdin); \
    print(f\"  status={d['status']}  nodes={d['number_of_nodes']}  shards={d['active_shards']}\")"
```

**Expected:** `status=green` or `status=yellow`, `nodes≥1`.

### 9.2 Ranger policy on opensearch_service

```bash
curl -sf -u "${RANGER_AUTH}" \
  "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=opensearch_service&zoneName=PROCESSING_ZONE&pageSize=100" \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    items=p.get('policyItems',[])
    for item in items:
        groups=item.get('groups',[])
        accesses=[a['type'] for a in item.get('accesses',[])]
        print(f\"  {p['name']}  groups={groups}  accesses={accesses}\")
"
```

**Expected:** `processing-admin-opensearch-all` — two policy items: admin with `_READ _UPDATE _CREATE _DELETE _MANAGE _ALL`, dev with `_READ`.

### 9.3 Create and query a test index

```bash
# Create index
curl -sf -X PUT http://192.168.1.50:30920/rbac-test-index \
  -H "Content-Type: application/json" \
  -d '{"settings":{"number_of_shards":1,"number_of_replicas":0}}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('  acknowledged:', d.get('acknowledged'))"

# Index a document
curl -sf -X POST http://192.168.1.50:30920/rbac-test-index/_doc/1 \
  -H "Content-Type: application/json" \
  -d '{"zone":"PROCESSING_ZONE","test":"rbac","user":"processing_admin"}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('  result:', d.get('result'))"

# Query
curl -sf http://192.168.1.50:30920/rbac-test-index/_search?pretty \
  | python3 -c "import sys,json; d=json.load(sys.stdin); \
    print('  hits:', d['hits']['total']['value'])"
```

**Expected:** `acknowledged: True`, `result: created`, `hits: 1`.

---

## 10. PROCESSING_ZONE — Polaris REST Catalog

### 10.1 Verify polaris_service exists in PROCESSING_ZONE

```bash
curl -sf -u "${RANGER_AUTH}" \
  "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=polaris_service&zoneName=PROCESSING_ZONE&pageSize=100" \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    groups=[g for item in p.get('policyItems',[]) for g in item.get('groups',[])]
    print(f\"  {p['name']}  groups={groups}\")
"
```

**Expected:** `processing-admin-polaris-all` with `processing_admin`, `account_admin`, `processing_dev`.

### 10.2 List catalogs via Polaris REST API

```bash
curl -sf http://192.168.1.50:30181/api/catalog/v1/namespaces 2>/dev/null \
  || curl -sf http://192.168.1.50:30181/api/management/v1/principal-roles 2>/dev/null \
  | python3 -m json.tool | head -20
```

### 10.3 Spark → Polaris connectivity (policy check)

The `processing-admin-spark-all` policy on `spark_service` allows Spark to discover and read Iceberg tables via Polaris. Confirm the policy has the correct resource and both policy items:

```bash
curl -sf -u "${RANGER_AUTH}" \
  "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=spark_service&zoneName=PROCESSING_ZONE&pageSize=100" \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    res=list(p.get('resources',{}).keys())
    n_items=len(p.get('policyItems',[]))
    print(f\"  {p['name']}  resource={res}  policyItems={n_items}\")
"
```

**Expected:** `processing-admin-spark-all` with `resource=['tag']` and `policyItems=2` (admin item + dev item).

---

## 11. PROCESSING_ZONE — Kestra

### 11.1 Kestra UI reachable

```bash
curl -sf http://192.168.1.50:30880/api/v1/health/ready \
  -o /dev/null -w "HTTP %{http_code}\n"
```

**Expected:** `HTTP 200`.

### 11.2 Ranger policy on kestra_service

```bash
curl -sf -u "${RANGER_AUTH}" \
  "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=kestra_service&zoneName=PROCESSING_ZONE&pageSize=100" \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    n_items=len(p.get('policyItems',[]))
    print(f\"  {p['name']}  policyItems={n_items}\")
"
```

**Expected:** `processing-admin-kestra-all` with `policyItems=2`.

### 11.3 Create a test workflow via API

```bash
curl -sf -X POST http://192.168.1.50:30880/api/v1/flows \
  -H "Content-Type: application/x-yaml" \
  --data-binary '
id: rbac-test-flow
namespace: prod
tasks:
  - id: log
    type: io.kestra.core.tasks.log.Log
    message: "RBAC e2e test — processing_admin"
' | python3 -m json.tool | grep -E '"id"|"namespace"'
```

**Expected:** `"id": "rbac-test-flow"`, `"namespace": "prod"`.

```bash
# Verify it appears
curl -sf http://192.168.1.50:30880/api/v1/flows/prod/rbac-test-flow \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('  flow:', d.get('id'), 'ns:', d.get('namespace'))"
```

---

## 12. Cross-Zone — account_admin Superuser

`account_admin` and `platform_admin` have admin access to all three zones. Verify the Ranger policy items include this group everywhere.

```bash
echo "=== Policies containing account_admin ==="
for svc in doris_service spark_service kafka_service polaris_service; do
  for zone in CACHING_ZONE PROCESSING_ZONE STREAMING_ZONE; do
    curl -sf -u "${RANGER_AUTH}" \
      "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=${svc}&zoneName=${zone}&pageSize=100" \
      | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    for item in p.get('policyItems',[]):
        if 'account_admin' in item.get('groups',[]):
            print(f'  ✓ {svc}/{zone}/{p[\"name\"]}')
            break
" 2>/dev/null
  done
done
```

**Expected:** At least one `✓` per service/zone combination that account_admin should cover.

---

## 13. Cleanup

Remove test artefacts created during this runbook.

### 13.1 Doris

```bash
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${DORIS_PASS}" -e "
    USE analytics;
    DELETE FROM users  WHERE user_id = 99;
    DELETE FROM events WHERE event_id IN (100, 101);
    DROP TABLE IF EXISTS rbac_test;
    DROP CATALOG IF EXISTS iceberg_polaris;
  " 2>/dev/null
echo "Doris cleaned"
```

### 13.2 Kafka

```bash
kubectl run kafka-cleanup --rm -it --restart=Never -n prod \
  --image=bitnami/kafka:3.9 \
  --env="BOOTSTRAP=strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092" \
  --env="KAFKA_PASS=${KAFKA_PASS}" \
  -- bash -c '
    echo "security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-app-user\" password=\"${KAFKA_PASS}\";" > /tmp/client.props
    kafka-topics.sh --bootstrap-server ${BOOTSTRAP} \
      --command-config /tmp/client.props \
      --delete --topic rbac-test-topic 2>/dev/null || true
  ' 2>/dev/null
echo "Kafka cleaned"
```

### 13.3 Schema Registry

```bash
curl -sf -X DELETE \
  http://192.168.1.54:30810/subjects/rbac-test-value \
  | python3 -m json.tool
```

### 13.4 OpenSearch

```bash
curl -sf -X DELETE http://192.168.1.50:30920/rbac-test-index \
  | python3 -c "import sys,json; print('  deleted:', json.load(sys.stdin).get('acknowledged'))"
```

### 13.5 Kestra

```bash
curl -sf -X DELETE \
  http://192.168.1.50:30880/api/v1/flows/prod/rbac-test-flow \
  -o /dev/null -w "HTTP %{http_code}\n"
```

---

## 14. Re-Bootstrap (if needed)

If Ranger state is corrupted or policies need to be rebuilt from scratch:

```bash
# Step 1 — Seed K8s secrets from OpenBao
bash scripts/master/12-seed-openbao-secrets.sh

# Step 2 — Create Kerberos principals and keytab secrets
bash scripts/master/15-seed-rbac-principals.sh

# Step 3 — Bootstrap Ranger (groups, users, services, zones, policies)
bash scripts/master/16-seed-ranger-rbac.sh

# Step 4 — Verify everything
bash scripts/master/17-verify-rbac.sh
```

Expected final output of step 4: `73/73 passed`.

---

## 15. Quick Reference — Endpoints and Credentials

| Service | External URL | Auth |
|---|---|---|
| Ranger Admin UI | `http://192.168.1.50:30680` | `admin` / `ranger-db-credentials` secret |
| Doris FE MySQL | `192.168.1.50:30090` | `root` / `doris-credentials` secret |
| Doris FE Web UI | `http://192.168.1.50:30030` | same |
| Polaris REST | `http://192.168.1.50:30181` | none (lab) |
| Kafka (Strimzi) | `192.168.1.50:30093` | SCRAM-SHA-512, `kafka-app-user` |
| Schema Registry | `http://192.168.1.54:30810` | none (lab) |
| AKHQ | `http://192.168.1.50:30808` | `akhq-credentials` secret |
| OpenSearch | `http://192.168.1.50:30920` | none (security plugin disabled) |
| Kestra UI | `http://192.168.1.50:30880` | none (lab) |
| Spark Master UI | `http://192.168.1.50:30707` | none |
| Debezium Connect | `http://192.168.1.50:30083` | none (lab) |

| Script | Purpose |
|---|---|
| `scripts/master/12-seed-openbao-secrets.sh` | Create / refresh all K8s secrets from OpenBao |
| `scripts/master/15-seed-rbac-principals.sh` | Create Kerberos principals + keytab K8s secrets |
| `scripts/master/16-seed-ranger-rbac.sh` | Bootstrap Ranger groups, users, services, zones, policies |
| `scripts/master/17-verify-rbac.sh` | Full automated RBAC verification (73 checks) |

| Persona | Ranger group | Kerberos principal | Allowed zones |
|---|---|---|---|
| `platform_admin` | `account_admin` | `platform_admin@STARDATADBLABS.LOCAL` | All (superuser) |
| `caching_admin_user` | `caching_admin` | `caching_admin_user@STARDATADBLABS.LOCAL` | `CACHING_ZONE` |
| `caching_dev_user` | `caching_dev` | `caching_dev_user@STARDATADBLABS.LOCAL` | `CACHING_ZONE` (read-only, masked) |
| `processing_admin_user` | `processing_admin` | `processing_admin_user@STARDATADBLABS.LOCAL` | `PROCESSING_ZONE` |
| `processing_dev_user` | `processing_dev` | `processing_dev_user@STARDATADBLABS.LOCAL` | `PROCESSING_ZONE` (read-only) |
| `streaming_admin_user` | `streaming_admin` | `streaming_admin_user@STARDATADBLABS.LOCAL` | `STREAMING_ZONE` |
| `streaming_dev_user` | `streaming_dev` | `streaming_dev_user@STARDATADBLABS.LOCAL` | `STREAMING_ZONE` (consume-only) |

---

---

## 16. New User — `account_admin` (Global Superuser, All Zones)

This section walks through creating a **brand-new** Kerberos principal and Ranger user, adding them to `account_admin`, and verifying they can perform privileged operations in every zone.

Use the name `test_super_user` throughout. Replace it with your real username if needed.

### 16.1 Create Kerberos principal

```bash
KDC_POD=$(kubectl get pod -n prod -l app=kerberos-kdc \
  -o jsonpath='{.items[0].metadata.name}')

# Create principal with a known password
kubectl exec -n prod "${KDC_POD}" -- \
  kadmin.local -q "addprinc -pw SuperPass1! test_super_user@STARDATADBLABS.LOCAL"

# Verify it was created
kubectl exec -n prod "${KDC_POD}" -- \
  kadmin.local -q "getprinc test_super_user@STARDATADBLABS.LOCAL" 2>&1 \
  | grep -E 'Principal:|Expiration'
```

**Expected:** `Principal: test_super_user@STARDATADBLABS.LOCAL` with no "does not exist" error.

### 16.2 Export keytab and store as K8s Secret

```bash
# Export keytab inside KDC pod
kubectl exec -n prod "${KDC_POD}" -- \
  kadmin.local -q "ktadd -k /tmp/test-super-user.keytab test_super_user@STARDATADBLABS.LOCAL"

# Copy keytab from pod to local
kubectl cp prod/${KDC_POD}:/tmp/test-super-user.keytab /tmp/test-super-user.keytab

# Store as K8s Secret
kubectl create secret generic test-super-user-keytab \
  --from-file=keytab=/tmp/test-super-user.keytab \
  -n prod \
  && echo "Keytab secret created"
```

### 16.3 Create Ranger user and add to `account_admin`

```bash
RANGER_PASS=$(kubectl get secret ranger-db-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
RANGER_AUTH="admin:${RANGER_PASS}"

# Get account_admin group id
ACCT_GID=$(curl -sf -u "${RANGER_AUTH}" \
  "http://192.168.1.50:30680/service/xusers/groups" \
  | python3 -c "
import sys,json
for g in json.load(sys.stdin).get('vXGroups',[]):
    if g['name']=='account_admin': print(g['id'])
")
echo "account_admin group id: ${ACCT_GID}"

# Create user
USER_RESP=$(curl -s -u "${RANGER_AUTH}" \
  -H "Content-Type: application/json" \
  -X POST "http://192.168.1.50:30680/service/xusers/users" \
  -d "{
    \"name\":\"test_super_user\",
    \"firstName\":\"Test\",
    \"lastName\":\"SuperUser\",
    \"password\":\"SuperPass1!\",
    \"userRoleList\":[\"ROLE_SYS_ADMIN\"],
    \"groupIdList\":[${ACCT_GID}],
    \"groupNameList\":[\"account_admin\"]
  }")
USER_ID=$(echo "${USER_RESP}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))")
echo "Created user id: ${USER_ID}"

# Explicitly link user to group (Ranger ignores groupIdList on POST)
curl -s -u "${RANGER_AUTH}" \
  -H "Content-Type: application/json" \
  -X POST "http://192.168.1.50:30680/service/xusers/groupusers" \
  -d "{\"name\":\"account_admin\",\"userId\":${USER_ID},\"groupId\":${ACCT_GID}}" \
  -o /dev/null && echo "Group link created"
```

### 16.4 Verify user exists in Ranger

```bash
curl -sf -u "${RANGER_AUTH}" \
  "http://192.168.1.50:30680/service/xusers/users/${USER_ID}" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); \
    print(f\"  name={d['name']}  role={d.get('userRoleList',['?'])[0]}\")"
```

**Expected:** `name=test_super_user  role=ROLE_SYS_ADMIN`

### 16.5 Test — CACHING_ZONE (Doris full access)

```bash
FE_POD=$(kubectl get pod -n prod -l app=doris-fe \
  -o jsonpath='{.items[0].metadata.name}')
DORIS_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)

# Create the user in Doris (Doris has its own user store)
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${DORIS_PASS}" \
    -e "CREATE USER IF NOT EXISTS 'test_super_user'@'%' IDENTIFIED BY 'SuperPass1!';
        GRANT ALL ON *.* TO 'test_super_user'@'%';" 2>/dev/null

# SELECT — should succeed
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u test_super_user -p"SuperPass1!" \
    -e "SHOW DATABASES; USE analytics; SELECT * FROM users LIMIT 3;" 2>&1 \
  | grep -v Warning
```

**Expected:** Database list and rows returned.

```bash
# DDL — should succeed (full admin)
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u test_super_user -p"SuperPass1!" \
    -e "USE analytics;
        CREATE TABLE IF NOT EXISTS super_test (id INT)
          DISTRIBUTED BY HASH(id) BUCKETS 1
          PROPERTIES ('replication_num'='1');
        INSERT INTO super_test VALUES (1),(2),(3);
        SELECT * FROM super_test;
        DROP TABLE super_test;" 2>&1 \
  | grep -v Warning
```

**Expected:** Table created, rows inserted/selected, table dropped — no errors.

### 16.6 Test — PROCESSING_ZONE (Ranger policy check)

```bash
# Confirm test_super_user inherits account_admin policy coverage in PROCESSING_ZONE
curl -sf -u "${RANGER_AUTH}" \
  "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=spark_service&zoneName=PROCESSING_ZONE&pageSize=100" \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    for item in p.get('policyItems',[]):
        if 'account_admin' in item.get('groups',[]):
            accesses=[a['type'] for a in item.get('accesses',[])]
            print(f'  ✓ {p[\"name\"]}  account_admin accesses: {accesses}')
" 2>/dev/null

# OpenSearch — create and delete index (full admin)
curl -sf -X PUT http://192.168.1.50:30920/super-test-index \
  -H "Content-Type: application/json" \
  -d '{"settings":{"number_of_shards":1,"number_of_replicas":0}}' \
  | python3 -c "import sys,json; print('  created:', json.load(sys.stdin).get('acknowledged'))"

curl -sf -X DELETE http://192.168.1.50:30920/super-test-index \
  | python3 -c "import sys,json; print('  deleted:', json.load(sys.stdin).get('acknowledged'))"
```

**Expected:** Policy shows `account_admin` with full access types. OpenSearch create + delete both `True`.

### 16.7 Test — STREAMING_ZONE (Ranger policy check)

```bash
# Confirm account_admin coverage in STREAMING_ZONE
for svc in kafka_service schema_registry_service debezium_service akhq_service; do
  curl -sf -u "${RANGER_AUTH}" \
    "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=${svc}&zoneName=STREAMING_ZONE&pageSize=100" \
    | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    for item in p.get('policyItems',[]):
        if 'account_admin' in item.get('groups',[]):
            print(f'  ✓ ${svc} / {p[\"name\"]}')
            break
" 2>/dev/null
done
```

**Expected:** `✓` for every streaming service — `kafka_service`, `schema_registry_service`, `debezium_service`, `akhq_service`.

### 16.8 Cleanup — remove test_super_user

```bash
# Drop from Doris
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${DORIS_PASS}" \
    -e "DROP USER IF EXISTS 'test_super_user'@'%';" 2>/dev/null

# Delete from Ranger (use USER_ID captured in 16.3)
curl -s -u "${RANGER_AUTH}" \
  -X DELETE "http://192.168.1.50:30680/service/xusers/users/${USER_ID}?forceDelete=true" \
  -o /dev/null && echo "Ranger user deleted"

# Remove Kerberos principal
kubectl exec -n prod "${KDC_POD}" -- \
  kadmin.local -q "delprinc -force test_super_user@STARDATADBLABS.LOCAL"

# Remove K8s Secret
kubectl delete secret test-super-user-keytab -n prod --ignore-not-found
```

---

## 17. New User — Per-Zone Admin

This section creates one new user per admin group (`caching_admin`, `processing_admin`, `streaming_admin`) and verifies each can perform **full administrative operations** only within their assigned zone.

Use names `test_cache_admin`, `test_proc_admin`, `test_stream_admin`.

### 17.1 Create Kerberos principals (all three)

```bash
KDC_POD=$(kubectl get pod -n prod -l app=kerberos-kdc \
  -o jsonpath='{.items[0].metadata.name}')

for user in test_cache_admin test_proc_admin test_stream_admin; do
  kubectl exec -n prod "${KDC_POD}" -- \
    kadmin.local -q "addprinc -pw AdminPass1! ${user}@STARDATADBLABS.LOCAL"
  echo "  ✓ principal: ${user}@STARDATADBLABS.LOCAL"
done

# Verify all three
kubectl exec -n prod "${KDC_POD}" -- \
  kadmin.local -q "listprincs" 2>/dev/null | grep test_
```

### 17.2 Create Ranger users and link to their groups

```bash
RANGER_PASS=$(kubectl get secret ranger-db-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
RANGER_AUTH="admin:${RANGER_PASS}"

# Fetch group ids
declare -A GROUP_IDS
for grp in caching_admin processing_admin streaming_admin; do
  gid=$(curl -sf -u "${RANGER_AUTH}" \
    "http://192.168.1.50:30680/service/xusers/groups" \
    | python3 -c "
import sys,json
for g in json.load(sys.stdin).get('vXGroups',[]):
    if g['name']=='${grp}': print(g['id'])
")
  GROUP_IDS[$grp]=$gid
  echo "  ${grp} id=${gid}"
done

# Create and link each user
declare -A USER_IDS
for pair in "test_cache_admin:caching_admin" "test_proc_admin:processing_admin" "test_stream_admin:streaming_admin"; do
  user="${pair%%:*}"
  grp="${pair##*:}"
  gid="${GROUP_IDS[$grp]}"

  resp=$(curl -s -u "${RANGER_AUTH}" \
    -H "Content-Type: application/json" \
    -X POST "http://192.168.1.50:30680/service/xusers/users" \
    -d "{\"name\":\"${user}\",\"firstName\":\"Test\",\"lastName\":\"Admin\",
         \"password\":\"AdminPass1!\",\"userRoleList\":[\"ROLE_USER\"],
         \"groupIdList\":[${gid}],\"groupNameList\":[\"${grp}\"]}")
  uid=$(echo "${resp}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))")
  USER_IDS[$user]=$uid

  # Explicit group link
  curl -s -u "${RANGER_AUTH}" -H "Content-Type: application/json" \
    -X POST "http://192.168.1.50:30680/service/xusers/groupusers" \
    -d "{\"name\":\"${grp}\",\"userId\":${uid},\"groupId\":${gid}}" -o /dev/null

  echo "  ✓ ${user} (id=${uid}) → ${grp}"
done
```

### 17.3 Test — `test_cache_admin` in CACHING_ZONE

```bash
FE_POD=$(kubectl get pod -n prod -l app=doris-fe \
  -o jsonpath='{.items[0].metadata.name}')
DORIS_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)

# Create Doris user
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${DORIS_PASS}" \
    -e "CREATE USER IF NOT EXISTS 'test_cache_admin'@'%' IDENTIFIED BY 'AdminPass1!';
        GRANT ALL ON *.* TO 'test_cache_admin'@'%';" 2>/dev/null

# Full DDL/DML — should succeed
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u test_cache_admin -p"AdminPass1!" \
    -e "SHOW DATABASES;
        USE analytics;
        CREATE TABLE IF NOT EXISTS cache_admin_test (id INT)
          DISTRIBUTED BY HASH(id) BUCKETS 1
          PROPERTIES ('replication_num'='1');
        INSERT INTO cache_admin_test VALUES (10),(20);
        SELECT * FROM cache_admin_test;
        DROP TABLE cache_admin_test;" 2>&1 \
  | grep -v Warning
```

**Expected:** All statements succeed — DDL, INSERT, SELECT, DROP.

```bash
# Ranger policy confirmation: caching_admin in caching-admin-all
curl -sf -u "${RANGER_AUTH}" \
  "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=doris_service&zoneName=CACHING_ZONE&pageSize=100" \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    for item in p.get('policyItems',[]):
        if 'caching_admin' in item.get('groups',[]):
            print(f'  ✓ {p[\"name\"]}  accesses={[a[\"type\"] for a in item.get(\"accesses\",[])]}')"
```

**Expected:** `caching-admin-all` and `caching-admin-polaris-catalog` with full access types.

### 17.4 Test — `test_proc_admin` in PROCESSING_ZONE

```bash
# Ranger policy confirmation: processing_admin in PROCESSING_ZONE policies
for svc in spark_service sqlmesh_service kestra_service opensearch_service polaris_service; do
  curl -sf -u "${RANGER_AUTH}" \
    "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=${svc}&zoneName=PROCESSING_ZONE&pageSize=100" \
    | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    for item in p.get('policyItems',[]):
        if 'processing_admin' in item.get('groups',[]):
            print(f'  ✓ ${svc} / {p[\"name\"]}')" 2>/dev/null
done
```

**Expected:** `✓` for all 5 processing services.

```bash
# OpenSearch — full admin operations (index create + delete)
curl -sf -X PUT http://192.168.1.50:30920/proc-admin-test-index \
  -H "Content-Type: application/json" \
  -d '{"settings":{"number_of_shards":1,"number_of_replicas":0}}' \
  | python3 -c "import sys,json; print('  created:', json.load(sys.stdin).get('acknowledged'))"

curl -sf -X DELETE http://192.168.1.50:30920/proc-admin-test-index \
  | python3 -c "import sys,json; print('  deleted:', json.load(sys.stdin).get('acknowledged'))"

# Kestra — create and delete a workflow
curl -sf -X POST http://192.168.1.50:30880/api/v1/flows \
  -H "Content-Type: application/x-yaml" \
  --data-binary '
id: proc-admin-test-flow
namespace: prod
tasks:
  - id: log
    type: io.kestra.core.tasks.log.Log
    message: "processing_admin test"
' | python3 -c "import sys,json; d=json.load(sys.stdin); print('  flow:', d.get('id'))"

curl -sf -X DELETE http://192.168.1.50:30880/api/v1/flows/prod/proc-admin-test-flow \
  -o /dev/null -w "  Kestra delete: HTTP %{http_code}\n"
```

**Expected:** OpenSearch `True`/`True`. Kestra flow created and deleted without error.

### 17.5 Test — `test_stream_admin` in STREAMING_ZONE

```bash
# Ranger policy confirmation: streaming_admin in STREAMING_ZONE policies
for svc in kafka_service schema_registry_service debezium_service akhq_service; do
  curl -sf -u "${RANGER_AUTH}" \
    "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=${svc}&zoneName=STREAMING_ZONE&pageSize=100" \
    | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    for item in p.get('policyItems',[]):
        if 'streaming_admin' in item.get('groups',[]):
            print(f'  ✓ ${svc} / {p[\"name\"]}')" 2>/dev/null
done
```

**Expected:** `✓` for all 4 streaming services.

```bash
# Kafka — full admin: create topic, produce, consume, delete topic
KAFKA_PASS=$(kubectl get secret kafka-app-user -n prod \
  -o jsonpath='{.data.password}' | base64 -d | tr -d '\n')

kubectl run stream-admin-test --rm -it --restart=Never -n prod \
  --image=bitnami/kafka:3.9 \
  --env="BOOTSTRAP=strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092" \
  --env="KAFKA_PASS=${KAFKA_PASS}" \
  -- bash -c '
    echo "security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-app-user\" password=\"${KAFKA_PASS}\";" > /tmp/c.props

    kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --command-config /tmp/c.props \
      --create --if-not-exists --topic stream-admin-test --partitions 1 --replication-factor 1 \
      && echo "  topic created"

    echo "admin-test-message" | kafka-console-producer.sh \
      --bootstrap-server ${BOOTSTRAP} --producer.config /tmp/c.props \
      --topic stream-admin-test && echo "  produced"

    kafka-console-consumer.sh --bootstrap-server ${BOOTSTRAP} \
      --consumer.config /tmp/c.props --topic stream-admin-test \
      --from-beginning --max-messages 1 --timeout-ms 5000 && echo "  consumed"

    kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --command-config /tmp/c.props \
      --delete --topic stream-admin-test && echo "  topic deleted"
  ' 2>/dev/null
```

**Expected:** topic created → produced → consumed → topic deleted.

```bash
# Schema Registry — register and delete a schema
curl -sf -X POST http://192.168.1.54:30810/subjects/stream-admin-test-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"AdminTest\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}' \
  | python3 -c "import sys,json; print('  schema id:', json.load(sys.stdin).get('id'))"

curl -sf -X DELETE http://192.168.1.54:30810/subjects/stream-admin-test-value \
  | python3 -m json.tool
```

**Expected:** Schema registered (returns numeric id), then deleted.

### 17.6 Cleanup — remove per-zone admin users

```bash
FE_POD=$(kubectl get pod -n prod -l app=doris-fe \
  -o jsonpath='{.items[0].metadata.name}')
DORIS_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)

# Doris
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${DORIS_PASS}" \
    -e "DROP USER IF EXISTS 'test_cache_admin'@'%';" 2>/dev/null

# Ranger — delete all three users (USER_IDS set in 17.2)
for user in test_cache_admin test_proc_admin test_stream_admin; do
  uid="${USER_IDS[$user]}"
  [[ -n "${uid}" ]] && \
    curl -s -u "${RANGER_AUTH}" \
      -X DELETE "http://192.168.1.50:30680/service/xusers/users/${uid}?forceDelete=true" \
      -o /dev/null && echo "  Deleted Ranger user: ${user} (id=${uid})"
done

# Kerberos
for user in test_cache_admin test_proc_admin test_stream_admin; do
  kubectl exec -n prod "${KDC_POD}" -- \
    kadmin.local -q "delprinc -force ${user}@STARDATADBLABS.LOCAL" 2>/dev/null
  echo "  Deleted principal: ${user}"
done
```

---

## 18. New User — Per-Zone Dev

This section creates one new user per dev group (`caching_dev`, `processing_dev`, `streaming_dev`) and verifies that each can only **read** within their zone — writes, DDL, and cross-zone access are all denied.

Use names `test_cache_dev`, `test_proc_dev`, `test_stream_dev`.

### 18.1 Create Kerberos principals (all three)

```bash
KDC_POD=$(kubectl get pod -n prod -l app=kerberos-kdc \
  -o jsonpath='{.items[0].metadata.name}')

for user in test_cache_dev test_proc_dev test_stream_dev; do
  kubectl exec -n prod "${KDC_POD}" -- \
    kadmin.local -q "addprinc -pw DevPass1! ${user}@STARDATADBLABS.LOCAL"
  echo "  ✓ principal: ${user}@STARDATADBLABS.LOCAL"
done

# Verify
kubectl exec -n prod "${KDC_POD}" -- \
  kadmin.local -q "listprincs" 2>/dev/null | grep test_
```

### 18.2 Create Ranger users and link to their dev groups

```bash
RANGER_PASS=$(kubectl get secret ranger-db-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
RANGER_AUTH="admin:${RANGER_PASS}"

# Fetch dev group ids
declare -A DEV_GROUP_IDS
for grp in caching_dev processing_dev streaming_dev; do
  gid=$(curl -sf -u "${RANGER_AUTH}" \
    "http://192.168.1.50:30680/service/xusers/groups" \
    | python3 -c "
import sys,json
for g in json.load(sys.stdin).get('vXGroups',[]):
    if g['name']=='${grp}': print(g['id'])
")
  DEV_GROUP_IDS[$grp]=$gid
  echo "  ${grp} id=${gid}"
done

# Create and link each dev user
declare -A DEV_USER_IDS
for pair in "test_cache_dev:caching_dev" "test_proc_dev:processing_dev" "test_stream_dev:streaming_dev"; do
  user="${pair%%:*}"
  grp="${pair##*:}"
  gid="${DEV_GROUP_IDS[$grp]}"

  resp=$(curl -s -u "${RANGER_AUTH}" \
    -H "Content-Type: application/json" \
    -X POST "http://192.168.1.50:30680/service/xusers/users" \
    -d "{\"name\":\"${user}\",\"firstName\":\"Test\",\"lastName\":\"Dev\",
         \"password\":\"DevPass1!\",\"userRoleList\":[\"ROLE_USER\"],
         \"groupIdList\":[${gid}],\"groupNameList\":[\"${grp}\"]}")
  uid=$(echo "${resp}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))")
  DEV_USER_IDS[$user]=$uid

  # Explicit group link
  curl -s -u "${RANGER_AUTH}" -H "Content-Type: application/json" \
    -X POST "http://192.168.1.50:30680/service/xusers/groupusers" \
    -d "{\"name\":\"${grp}\",\"userId\":${uid},\"groupId\":${gid}}" -o /dev/null

  echo "  ✓ ${user} (id=${uid}) → ${grp}"
done
```

### 18.3 Test — `test_cache_dev` in CACHING_ZONE

```bash
FE_POD=$(kubectl get pod -n prod -l app=doris-fe \
  -o jsonpath='{.items[0].metadata.name}')
DORIS_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)

# Create Doris user (read-only grant)
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${DORIS_PASS}" \
    -e "CREATE USER IF NOT EXISTS 'test_cache_dev'@'%' IDENTIFIED BY 'DevPass1!';
        GRANT SELECT ON analytics.* TO 'test_cache_dev'@'%';" 2>/dev/null

# ✓ SELECT — should succeed
echo "--- SELECT (expect: rows) ---"
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u test_cache_dev -p"DevPass1!" \
    -e "USE analytics; SELECT user_id, email FROM users LIMIT 3;" 2>&1 \
  | grep -v Warning

# ✗ INSERT — should be denied
echo "--- INSERT (expect: error/denied) ---"
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u test_cache_dev -p"DevPass1!" \
    -e "USE analytics; INSERT INTO users VALUES (999,'evil@bad.com','hacker');" 2>&1 \
  | grep -v Warning

# ✗ DROP — should be denied
echo "--- DROP TABLE (expect: error/denied) ---"
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u test_cache_dev -p"DevPass1!" \
    -e "USE analytics; DROP TABLE users;" 2>&1 \
  | grep -v Warning

# ✗ CREATE — should be denied
echo "--- CREATE TABLE (expect: error/denied) ---"
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u test_cache_dev -p"DevPass1!" \
    -e "USE analytics; CREATE TABLE dev_test (id INT)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num'='1');" 2>&1 \
  | grep -v Warning
```

**Expected results:**

| Operation | Expected outcome |
|---|---|
| `SELECT` | ✓ Returns rows (email masked if Ranger plugin active) |
| `INSERT` | ✗ Error: `Access denied` or `permission denied` |
| `DROP TABLE` | ✗ Error: `Access denied` or `permission denied` |
| `CREATE TABLE` | ✗ Error: `Access denied` or `permission denied` |

```bash
# Ranger policy confirmation: caching_dev appears as second policyItem (read-only)
curl -sf -u "${RANGER_AUTH}" \
  "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=doris_service&zoneName=CACHING_ZONE&pageSize=100" \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    if p.get('name')=='caching-dev-read':
        for item in p.get('policyItems',[]):
            if 'caching_dev' in item.get('groups',[]):
                print(f'  ✓ caching-dev-read  accesses={[a[\"type\"] for a in item[\"accesses\"]]}')"
```

**Expected:** `accesses=['select']` — read-only confirmed.

### 18.4 Test — `test_proc_dev` in PROCESSING_ZONE

```bash
# Ranger policy confirmation: processing_dev in PROCESSING_ZONE policies (read-only policyItem)
for svc in spark_service sqlmesh_service kestra_service opensearch_service polaris_service; do
  curl -sf -u "${RANGER_AUTH}" \
    "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=${svc}&zoneName=PROCESSING_ZONE&pageSize=100" \
    | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    for item in p.get('policyItems',[]):
        if 'processing_dev' in item.get('groups',[]):
            accesses=[a['type'] for a in item.get('accesses',[])]
            print(f'  ✓ ${svc}  processing_dev accesses={accesses}')" 2>/dev/null
done
```

**Expected:** Each processing service shows `processing_dev` with `['_READ']` only.

```bash
# OpenSearch — dev can read but not delete an index
curl -sf -X PUT http://192.168.1.50:30920/proc-dev-test-index \
  -H "Content-Type: application/json" \
  -d '{"settings":{"number_of_shards":1,"number_of_replicas":0}}' \
  | python3 -c "import sys,json; print('  setup index created:', json.load(sys.stdin).get('acknowledged'))"

# ✓ Read — should succeed (no auth on this lab OpenSearch)
echo "--- GET index (expect: index info) ---"
curl -sf "http://192.168.1.50:30920/proc-dev-test-index" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('  index exists:', 'proc-dev-test-index' in d)"

# ✗ Delete — would be denied by Ranger when plugin active
echo "--- DELETE index (expect: denied when Ranger plugin active; succeeds in lab) ---"
curl -sf -X DELETE http://192.168.1.50:30920/proc-dev-test-index \
  | python3 -c "import sys,json; print('  deleted:', json.load(sys.stdin).get('acknowledged'))"
```

> **Note:** OpenSearch security plugin is disabled in this lab — delete will succeed at the API level. The Ranger `processing_dev` policy restricts to `_READ` only; this will be enforced once the security plugin is enabled.

### 18.5 Test — `test_stream_dev` in STREAMING_ZONE

```bash
# Ranger policy confirmation: streaming_dev in STREAMING_ZONE
# Topics: streaming_dev policyItem in streaming-admin-kafka-all (consume/describe only)
curl -sf -u "${RANGER_AUTH}" \
  "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=kafka_service&zoneName=STREAMING_ZONE&pageSize=100" \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    for item in p.get('policyItems',[]):
        if 'streaming_dev' in item.get('groups',[]):
            accesses=[a['type'] for a in item.get('accesses',[])]
            print(f'  ✓ {p[\"name\"]}  streaming_dev accesses={accesses}')"
```

**Expected:**
- `streaming-admin-kafka-all`: `streaming_dev` → `['consume', 'describe']`
- `streaming-dev-kafka-consumergroup`: `streaming_dev` → `['consume', 'describe']` (scoped to `dev-*`)

```bash
KAFKA_PASS=$(kubectl get secret kafka-app-user -n prod \
  -o jsonpath='{.data.password}' | base64 -d | tr -d '\n')

# Setup: create a topic as admin first
kubectl run stream-dev-setup --rm -it --restart=Never -n prod \
  --image=bitnami/kafka:3.9 \
  --env="BOOTSTRAP=strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092" \
  --env="KAFKA_PASS=${KAFKA_PASS}" \
  -- bash -c '
    echo "security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-app-user\" password=\"${KAFKA_PASS}\";" > /tmp/c.props
    kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --command-config /tmp/c.props \
      --create --if-not-exists --topic dev-test-topic --partitions 1 --replication-factor 1
    echo "dev-consume-me" | kafka-console-producer.sh \
      --bootstrap-server ${BOOTSTRAP} --producer.config /tmp/c.props \
      --topic dev-test-topic
    echo "  setup: topic created and message produced"
  ' 2>/dev/null

# ✓ CONSUME — streaming_dev can consume
echo "--- CONSUME (expect: dev-consume-me) ---"
kubectl run stream-dev-consume --rm -it --restart=Never -n prod \
  --image=bitnami/kafka:3.9 \
  --env="BOOTSTRAP=strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092" \
  --env="KAFKA_PASS=${KAFKA_PASS}" \
  -- bash -c '
    echo "security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-app-user\" password=\"${KAFKA_PASS}\";" > /tmp/c.props
    kafka-console-consumer.sh --bootstrap-server ${BOOTSTRAP} \
      --consumer.config /tmp/c.props --topic dev-test-topic \
      --from-beginning --max-messages 1 --timeout-ms 5000
  ' 2>/dev/null
```

**Expected:** `dev-consume-me` printed — consume succeeds.

```bash
# ✓ DESCRIBE (list topics) — should succeed
kubectl run stream-dev-describe --rm -it --restart=Never -n prod \
  --image=bitnami/kafka:3.9 \
  --env="BOOTSTRAP=strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092" \
  --env="KAFKA_PASS=${KAFKA_PASS}" \
  -- bash -c '
    echo "security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-app-user\" password=\"${KAFKA_PASS}\";" > /tmp/c.props
    kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --command-config /tmp/c.props --list
  ' 2>/dev/null
```

**Expected:** Topic list returned without error.

```bash
# ✗ Schema Registry — dev can read subjects but not delete them
echo "--- List schemas (expect: array) ---"
curl -sf http://192.168.1.54:30810/subjects | python3 -m json.tool

echo "--- Register schema as dev (expect: allowed per tag _READ policy) ---"
curl -sf -X POST http://192.168.1.54:30810/subjects/dev-test-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"DevTest\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}' \
  | python3 -c "import sys,json; print('  result:', json.load(sys.stdin))"
```

> **Note:** Schema Registry has no auth in this lab — all operations succeed. The Ranger `streaming_dev` policy restricts to `_READ` only; this will be enforced once Schema Registry is integrated with the Ranger plugin.

### 18.6 Test — cross-zone isolation (dev must not access other zones)

A `caching_dev` user must have no access to PROCESSING_ZONE or STREAMING_ZONE services.

```bash
# caching_dev has NO policy item in any PROCESSING_ZONE service
echo "=== caching_dev must NOT appear in PROCESSING_ZONE ==="
for svc in spark_service sqlmesh_service kestra_service opensearch_service polaris_service; do
  result=$(curl -sf -u "${RANGER_AUTH}" \
    "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=${svc}&zoneName=PROCESSING_ZONE&pageSize=100" \
    | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    for item in p.get('policyItems',[]):
        if 'caching_dev' in item.get('groups',[]):
            print('FOUND')
" 2>/dev/null)
  if [[ -z "${result}" ]]; then
    echo "  ✓ ${svc} — caching_dev correctly absent"
  else
    echo "  ✗ ${svc} — caching_dev unexpectedly present!"
  fi
done

echo ""
echo "=== streaming_dev must NOT appear in CACHING_ZONE ==="
for svc in doris_service polaris_service; do
  result=$(curl -sf -u "${RANGER_AUTH}" \
    "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=${svc}&zoneName=CACHING_ZONE&pageSize=100" \
    | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d if isinstance(d,list) else d.get('policies',[])
for p in ps:
    for item in p.get('policyItems',[]):
        if 'streaming_dev' in item.get('groups',[]):
            print('FOUND')
" 2>/dev/null)
  if [[ -z "${result}" ]]; then
    echo "  ✓ ${svc} — streaming_dev correctly absent"
  else
    echo "  ✗ ${svc} — streaming_dev unexpectedly present!"
  fi
done
```

**Expected:** All `✓` — dev groups have no policy coverage outside their own zone.

### 18.7 Cleanup — remove dev test users

```bash
FE_POD=$(kubectl get pod -n prod -l app=doris-fe \
  -o jsonpath='{.items[0].metadata.name}')
DORIS_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)

# Doris
kubectl exec -n prod "${FE_POD}" -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${DORIS_PASS}" \
    -e "DROP USER IF EXISTS 'test_cache_dev'@'%';" 2>/dev/null

# Kafka topic cleanup
kubectl run stream-dev-cleanup --rm -it --restart=Never -n prod \
  --image=bitnami/kafka:3.9 \
  --env="BOOTSTRAP=strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092" \
  --env="KAFKA_PASS=${KAFKA_PASS}" \
  -- bash -c '
    echo "security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-app-user\" password=\"${KAFKA_PASS}\";" > /tmp/c.props
    kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --command-config /tmp/c.props \
      --delete --topic dev-test-topic 2>/dev/null || true
  ' 2>/dev/null

# Schema Registry
curl -sf -X DELETE http://192.168.1.54:30810/subjects/dev-test-value \
  -o /dev/null && echo "Schema deleted"

# OpenSearch
curl -sf -X DELETE http://192.168.1.50:30920/proc-dev-test-index \
  -o /dev/null && echo "Index deleted"

# Ranger — delete all three dev users
for user in test_cache_dev test_proc_dev test_stream_dev; do
  uid="${DEV_USER_IDS[$user]}"
  [[ -n "${uid}" ]] && \
    curl -s -u "${RANGER_AUTH}" \
      -X DELETE "http://192.168.1.50:30680/service/xusers/users/${uid}?forceDelete=true" \
      -o /dev/null && echo "  Deleted Ranger user: ${user} (id=${uid})"
done

# Kerberos
for user in test_cache_dev test_proc_dev test_stream_dev; do
  kubectl exec -n prod "${KDC_POD}" -- \
    kadmin.local -q "delprinc -force ${user}@STARDATADBLABS.LOCAL" 2>/dev/null
  echo "  Deleted principal: ${user}"
done
```

---

## 19. Known Limitations (Lab vs. Production)

| Item | Lab state | Production fix |
|---|---|---|
| Doris Ranger plugin | Not embedded in Doris image — policies exist but column masking/row filter not enforced | Build Doris with Ranger Hive plugin jar on classpath |
| OpenSearch security | Plugin disabled | Enable OpenSearch Security plugin; point to Ranger for policy sync |
| Kafka RBAC | `kafka_service` registered as `kafka` type; Ranger policies created but KRaft cluster does not load Ranger plugin by default | Install Ranger Kafka plugin in broker classpath |
| Ranger UI auth | Username/password (dev mode) | Enable Kerberos SPNEGO for Ranger Admin UI |
| User group membership | Created via REST API (not usersync) — does not appear in Ranger list endpoint; verified via ID scan | Deploy Ranger UserSync with LDAP/AD |
| Column masking test | Passes policy existence check only | Run with Ranger plugin active to verify live masking |
