# Runbook 11 — Kerberos Client Authentication Integration

> **KDC:** `kerberos-kdc.prod.svc.cluster.local:88` · **Realm:** `STARDATADBLABS.LOCAL`
> **Toggle:** `kubectl get cm kerberos-integration-config -n prod -o jsonpath='{.data.kerberos\.enabled}'`
> **Related runbooks:** [01 — OpenBao](runbook-01-openbao.md) · [08 — Security & Access](runbook-08-security-access.md)

---

## 1. Architecture

Kerberos provides **authentication** (who are you?). Each service's own authorization mechanism (Doris native GRANT/REVOKE, Kafka `allow.everyone.if.no.acl.found`, OpenSearch security plugin) controls what authenticated users can access.

```
User gets a TGT from KDC
  │  kinit -kt alice.keytab alice@STARDATADBLABS.LOCAL
  ▼
Client connects to service using GSSAPI/SPNEGO ticket
  │  Kafka: SASL_PLAINTEXT + GSSAPI on port 9093
  │  OpenSearch: HTTP Negotiate header
  │  Spark: keytab in spark-submit --conf spark.kerberos.*
  ▼
Service validates ticket against its own keytab (mounted from K8s secret)
  │  Identity confirmed: "alice" (realm stripped)
  ▼
Service authorization: alice → resource → Allow/Deny
```

**Identity convention:** KDC principal name with realm stripped = service username.
`alice@STARDATADBLABS.LOCAL` → Kafka/OpenSearch/Doris username `alice`.
The same short name is used consistently across all services.

---

## 2. Platform Toggle

### Check current state

```bash
kubectl get cm kerberos-integration-config -n prod \
  -o jsonpath='{.data.kerberos\.enabled}' && echo
# "false" = Kerberos disabled (default)
# "true"  = Kerberos enabled
```

### Enable Kerberos (platform-wide)

```bash
# 1. Flip the toggle
kubectl patch cm kerberos-integration-config -n prod \
  --type merge -p '{"data":{"kerberos.enabled":"true"}}'

# 2. Restart services (in order — Kafka last because it causes client reconnects)
kubectl rollout restart deploy/spark-master deploy/spark-worker -n prod
kubectl rollout restart statefulset/doris-fe -n prod

# 3. For Kafka: uncomment KRB listener + keytab blocks in kafka-cluster.yaml
#    commit to git, ArgoCD reconciles → Strimzi rolls the broker

# 4. For OpenSearch: apply updated config.yml via securityadmin.sh (see §6.3)
```

### Disable Kerberos (platform-wide)

```bash
kubectl patch cm kerberos-integration-config -n prod \
  --type merge -p '{"data":{"kerberos.enabled":"false"}}'
# Then restart services and revert manifest changes
```

---

## 3. Service Principals (already provisioned)

| Service | Principal | K8s Secret | Key |
|---|---|---|---|
| Kafka | `svc/kafka@STARDATADBLABS.LOCAL` | `kafka-keytab` | `keytab` |
| Doris | `svc/doris@STARDATADBLABS.LOCAL` | `doris-keytab` | `keytab` |
| Spark | `svc/spark@STARDATADBLABS.LOCAL` | `spark-keytab` | `keytab` |
| OpenSearch | `svc/opensearch@STARDATADBLABS.LOCAL` | `opensearch-keytab` | `keytab` |

Verify any keytab is valid:
```bash
# Extract and check
kubectl get secret kafka-keytab -n prod -o jsonpath='{.data.keytab}' | \
  base64 -d > /tmp/kafka.keytab
klist -ekt /tmp/kafka.keytab
```

---

## 4. Adding a New User

> **Complete these steps in order.** Kerberos principal alone grants zero access.

### Step 1 — Create the KDC principal

```bash
# Interactive (prompts for password)
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc alice@STARDATADBLABS.LOCAL"

# Non-interactive (set password directly)
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc -pw TempPass1! alice@STARDATADBLABS.LOCAL"

# Verify
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "getprinc alice@STARDATADBLABS.LOCAL"
```

### Step 2 — Export keytab (for service accounts / Spark jobs)

```bash
# Export inside the KDC pod
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "ktadd -k /tmp/alice.keytab alice@STARDATADBLABS.LOCAL"

# Copy to master
KDC_POD=$(kubectl get pod -n prod -l app=kerberos-kdc \
  -o jsonpath='{.items[0].metadata.name}')
kubectl cp prod/${KDC_POD}:/tmp/alice.keytab /tmp/alice.keytab
klist -ekt /tmp/alice.keytab

# Store as K8s secret
kubectl create secret generic alice-keytab \
  --from-file=keytab=/tmp/alice.keytab \
  -n prod \
  --dry-run=client -o yaml | kubectl apply -f -

# Clean up temp files
kubectl exec -n prod deploy/kerberos-kdc -- rm /tmp/alice.keytab
rm /tmp/alice.keytab
```

### Step 3 — Create Doris SQL user (same username)

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
```

> Doris 4.0.x does not support native GSSAPI. A matching SQL username with the same
> short name as the KDC principal ensures unified identity governance via Doris native SQL GRANT/REVOKE.

### Step 4 — Store credentials in OpenBao

```bash
BAO_ADDR="http://192.168.1.50:30820"
KEYS_FILE="${HOME}/openbao-init-keys.json"
[ -f "${KEYS_FILE}" ] || KEYS_FILE="/root/openbao-init-keys.json"
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${KEYS_FILE}'))['root_token'])")

# Store Kerberos principal metadata
curl -sf -X POST \
  -H "X-Vault-Token: ${ROOT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"data":{"principal":"alice@STARDATADBLABS.LOCAL","keytab_secret":"alice-keytab","created_by":"admin"}}' \
  "${BAO_ADDR}/v1/secret/data/kerberos/users/alice" && echo "Kerberos entry stored"

# Store Doris password
curl -sf -X POST \
  -H "X-Vault-Token: ${ROOT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"data":{"username":"alice","password":"AliceDoris1!","service":"doris","created_by":"admin"}}' \
  "${BAO_ADDR}/v1/secret/data/doris/users/alice" && echo "Doris credential stored"
```

### Step 5 — Grant Doris permissions

```bash
BAO_ADDR="http://192.168.1.50:30820"
KEYS_FILE="${HOME}/openbao-init-keys.json"
[ -f "${KEYS_FILE}" ] || KEYS_FILE="/root/openbao-init-keys.json"
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${KEYS_FILE}'))['root_token'])")
DORIS_ROOT_PASS=$(curl -sf \
  -H "X-Vault-Token: ${ROOT_TOKEN}" \
  "${BAO_ADDR}/v1/secret/data/doris/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

# Grant privileges via Doris native SQL
kubectl exec -n prod statefulset/doris-fe -it -- \
  mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_ROOT_PASS}" \
  -e "GRANT SELECT ON *.* TO 'alice'@'%';"
```

---

## 5. Kafka — Enable GSSAPI Listener (Phase 3)

> **Default: DISABLED.** The KRB listener block is commented out in `kafka-cluster.yaml`.
> SCRAM listeners always remain active.

### To enable

1. In `manifests/strimzi/kafka-cluster.yaml`, uncomment **three** blocks (all marked `# kerberos.enabled=true`):
   - The `#- name: krb` listener block (port 9093) in `spec.kafka.listeners`
   - The `#- name: kafka-keytab` volume in `KafkaNodePool.template.pod.volumes`
   - The `#- name: kafka-keytab` volumeMount in `KafkaNodePool.template.kafkaContainer.volumeMounts`

2. Commit and push — ArgoCD reconciles, Strimzi performs a rolling restart.

3. Verify the new listener is up:
```bash
kubectl logs -n prod strimzi-kafka-combined-0 | grep -iE "krb|GSSAPI|9093" | tail -20
```

### Client configuration (GSSAPI)

```bash
# Obtain a TGT first (on a machine with krb5 client tools + krb5.conf)
kinit -kt alice.keytab alice@STARDATADBLABS.LOCAL
klist  # verify TGT

# client.properties for GSSAPI
cat > /tmp/krb-client.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=svc
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required \
  useTicketCache=true;
EOF

# Test produce
echo "hello-kerberos" | kafka-console-producer.sh \
  --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9093 \
  --topic orders \
  --producer.config /tmp/krb-client.properties
```

---

## 6. OpenSearch — Enable SPNEGO (Phase 4)

> **Default: DISABLED** (`kerberos_auth_domain.http_enabled: false` in config.yml).
> Basic auth against internal users database remains active at all times.

### 6.1 Edit the security config

In `manifests/opensearch/opensearch-security-config.yaml`, change:
```yaml
kerberos_auth_domain:
  http_enabled: false   # ← change to: true
```

### 6.2 Apply to the pod

```bash
# Copy the updated config.yml into the pod
kubectl cp manifests/opensearch/opensearch-security-config.yaml \
  prod/opensearch-cluster-master-0:/tmp/security-config.yaml

# Extract just the config.yml content and apply
kubectl exec -n prod opensearch-cluster-master-0 -- bash -c "
  python3 -c \"
import yaml, sys
d = yaml.safe_load(open('/tmp/security-config.yaml'))
print(d['data']['config.yml'])
\" > /tmp/config.yml
  /usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh \
    -f /tmp/config.yml -t config -icl -nhnv \
    -cacert /usr/share/opensearch/config/tls/root-ca.pem \
    -cert   /usr/share/opensearch/config/tls/admin.pem \
    -key    /usr/share/opensearch/config/tls/admin-key.pem \
    -h opensearch-cluster-master-0.prod.svc.cluster.local
"
```

### 6.3 Test SPNEGO authentication

```bash
# From a machine with a valid TGT (kinit alice@STARDATADBLABS.LOCAL first)
curl --negotiate -u : \
  http://192.168.1.53:30920/_cluster/health
# Expected: {"status":"green",...}
```

---

## 7. Spark — Kerberos Job Submission (Phase 2)

> Spark workers already have krb5.conf and spark-keytab mounted.
> No manifest change needed — Kerberos is configured per job.

### Submit a Spark job with Kerberos

```bash
kubectl exec -n prod deploy/spark-master -- \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master-svc.prod.svc.cluster.local:7077 \
    --conf spark.kerberos.enabled=true \
    --conf spark.kerberos.principal=alice@STARDATADBLABS.LOCAL \
    --conf spark.kerberos.keytab=/etc/security/keytabs/alice.keytab \
    --class org.example.MyApp \
    /path/to/app.jar
```

### Mount a user keytab into a Spark job pod

```yaml
# In spark-submit with Kubernetes mode, mount the alice-keytab secret:
--conf spark.kubernetes.driver.secrets.alice-keytab=/etc/security/keytabs
--conf spark.kubernetes.executor.secrets.alice-keytab=/etc/security/keytabs
--conf spark.kerberos.keytab=/etc/security/keytabs/keytab
--conf spark.kerberos.principal=alice@STARDATADBLABS.LOCAL
```

---

## 8. Removing a User

```bash
# 1. Delete KDC principal
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "delprinc -force alice@STARDATADBLABS.LOCAL"

# 3. Delete K8s keytab secret
kubectl delete secret alice-keytab -n prod 2>/dev/null || echo "not found"

# 4. Delete Doris SQL user
DORIS_ROOT_PASS=$(kubectl get secret doris-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
kubectl exec -n prod statefulset/doris-fe -it -- \
  mysql -h127.0.0.1 -P9030 -uroot -p"${DORIS_ROOT_PASS}" \
  -e "DROP USER 'alice'@'%';"

# 5. Delete from OpenBao
BAO_ADDR="http://192.168.1.50:30820"
KEYS_FILE="${HOME}/openbao-init-keys.json"
[ -f "${KEYS_FILE}" ] || KEYS_FILE="/root/openbao-init-keys.json"
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${KEYS_FILE}'))['root_token'])")
curl -sf -X DELETE -H "X-Vault-Token: ${ROOT_TOKEN}" \
  "${BAO_ADDR}/v1/secret/data/kerberos/users/alice" && echo "kerberos entry deleted"
curl -sf -X DELETE -H "X-Vault-Token: ${ROOT_TOKEN}" \
  "${BAO_ADDR}/v1/secret/data/doris/users/alice" && echo "doris entry deleted"

# 6. Verify KDC principal gone
kubectl exec -n prod deploy/kerberos-kdc -- \
  kadmin.local -q "listprincs" | grep "alice@" && \
  echo "WARNING: alice still in KDC" || echo "OK — KDC clean"
```

---

## 9. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `kinit: KDC unreachable` | Pod can't reach `kerberos-kdc.prod.svc.cluster.local:88` | Check KDC pod: `kubectl get pod -n prod -l app=kerberos-kdc`. Check `krb5.conf` is mounted correctly |
| `GSS-API error: No valid credentials` | Keytab not found or expired | Verify mount: `ls -la /etc/security/keytabs/`. Re-export keytab from KDC |
| `Clock skew too great` | Time difference between client and KDC > 5 min | Sync NTP on all nodes. Check: `date` on worker nodes |
| Kafka GSSAPI listener `UNKNOWN_SERVER_ERROR` | `sasl.kerberos.service.name` mismatch | Client must use `sasl.kerberos.service.name=svc` (matches principal prefix `svc/kafka@`) |
| OpenSearch SPNEGO returns 401 | `kerberos_auth_domain` not applied yet | Run `securityadmin.sh` — config.yml changes require explicit apply, not just pod restart |
| Doris: Kerberos ticket doesn't work | Doris has no native GSSAPI support | Doris uses SQL password auth. Kerberos principal and Doris SQL user must share the same username but auth separately |

### Useful debugging commands

```bash
# List all KDC principals
kubectl exec -n prod deploy/kerberos-kdc -- kadmin.local -q "listprincs"

# Verify a keytab secret is intact
kubectl get secret kafka-keytab -n prod -o jsonpath='{.data.keytab}' | \
  base64 -d | file -   # should show "Kerberos Keytab"

# Check krb5.conf is mounted in a service pod
kubectl exec -n prod strimzi-kafka-combined-0 -- \
  cat /mnt/krb5-conf/cluster.conf

kubectl exec -n prod statefulset/doris-fe -- \
  cat /etc/krb5.conf.d/cluster.conf 2>/dev/null || echo "not mounted"

kubectl exec -n prod deploy/spark-master -- \
  cat /etc/krb5.conf.d/cluster.conf

kubectl exec -n prod opensearch-cluster-master-0 -- \
  cat /etc/krb5.conf.d/cluster.conf

# Check KDC is reachable from a service pod
kubectl exec -n prod deploy/spark-master -- \
  nc -zv kerberos-kdc.prod.svc.cluster.local 88 2>&1

# Check Kafka broker sees the krb5.conf JVM flag
kubectl logs -n prod strimzi-kafka-combined-0 | grep "krb5" | head -5
```

---

## 10. Quick Reference

| Item | Value |
|---|---|
| KDC | `kerberos-kdc.prod.svc.cluster.local:88` |
| Kadmin | `kerberos-kadmin.prod.svc.cluster.local:749` |
| Realm | `STARDATADBLABS.LOCAL` |
| Admin principal | `admin/admin@STARDATADBLABS.LOCAL` |
| Admin password | Secret `kerberos-admin` key `admin-password` |
| Toggle ConfigMap | `kerberos-integration-config` in `prod` ns |
| Toggle key | `data.kerberos.enabled` (`"false"` / `"true"`) |
| krb5.conf source | `kerberos-integration-config` key `krb5.conf` |
| Kafka GSSAPI port | `9093` (commented out until enabled) |
| Kafka SCRAM port | `9092` (always active) |
| OpenSearch security config | `manifests/opensearch/opensearch-security-config.yaml` |
