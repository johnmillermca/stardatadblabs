# Runbook 01 — OpenBao Secret Manager

> **Cluster:** `192.168.1.50` (master) · **Namespace:** `prod` · **Version:** OpenBao 2.6.0  
> **UI:** `http://192.168.1.50:30820/ui` · **Helm chart:** `openbao/openbao`

---

## 1. What Is OpenBao?

OpenBao is the **community-maintained open-source fork** of HashiCorp Vault, created after Vault switched to the Business Source License (BSL) in 2023. OpenBao is licensed under the Mozilla Public License 2.0 (MPL-2.0) and is governed by the Linux Foundation.

OpenBao provides a **unified secret management plane** for the entire platform:

| Capability | Description |
|---|---|
| **KV v2 secrets engine** | Versioned key-value store for all application credentials |
| **Kubernetes auth** | Pods authenticate using their ServiceAccount JWT — no static credentials embedded in YAML |
| **Agent sidecar injector** | Automatically injects secrets into pods as files via a mutating webhook |
| **Dynamic credentials** | Generate short-lived DB/cloud credentials on demand (advanced) |
| **Audit logging** | Every secret read/write/delete is logged with caller identity |
| **Seal/Unseal mechanism** | Uses Shamir's Secret Sharing — the vault is encrypted at rest and must be unsealed with a quorum of key holders after every restart |

### Why OpenBao instead of Kubernetes Secrets?

| Feature | K8s Secrets | OpenBao |
|---|---|---|
| Encryption at rest | Only if etcd encryption configured | Always encrypted, AES-256-GCM |
| Secret versioning | No | Yes (KV v2 keeps full history) |
| Access audit trail | No | Yes — every read is logged |
| Dynamic/short-lived credentials | No | Yes |
| Fine-grained path-level policies | No (RBAC is namespace-scoped) | Yes (per-path capabilities) |
| Rotation without pod restart | No | Yes (via agent template re-render) |

---

## 2. Architecture in This Platform

```
┌──────────────────────────────────────────────────────────────────────────┐
│  prod namespace                                                          │
│                                                                          │
│  ┌────────────────────────────┐    ┌────────────────────────────────┐    │
│  │  openbao-0 (StatefulSet)   │    │  openbao-agent-injector        │    │
│  │  Port 8200 (API/UI)        │    │  Port 443 (mutating webhook)   │    │
│  │  Port 8201 (cluster)       │    │  Watches pod CREATE events     │    │
│  │  NodePort 30820            │    │  Injects vault-agent sidecar   │    │
│  │  PVC: 10 Gi (data)         │    └────────────────────────────────┘    │
│  │  PVC:  5 Gi (audit)        │                                          │
│  └─────────────┬──────────────┘                                          │
│                │                                                          │
│  ┌─────────────▼──────────────────────────────────────────────────────┐  │
│  │  Secrets stored (KV v2):                                          │  │
│  │  secret/data/grafana/credentials                                  │  │
│  │  secret/data/prometheus/credentials                               │  │
│  │  secret/data/postgresql/credentials                               │  │
│  │  secret/data/mongodb/credentials                                  │  │
│  │  secret/data/kafka/credentials                                    │  │
│  │  secret/data/opensearch/credentials                               │  │
│  │  secret/data/kestra/credentials                                   │  │
│  │  secret/data/doris/credentials                                    │  │
│  │  secret/data/ranger/credentials                                   │  │
│  │  secret/data/kerberos/credentials                                 │  │
│  │  secret/data/sqlmesh/credentials                                  │  │
│  │  secret/data/polaris/credentials                                  │  │
│  └────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Installation

### 3.1 Deploy via Script
```bash
cd ~/k8s-platform/scripts/master
sudo bash 10-deploy-openbao.sh
```

The script performs:
1. Adds the OpenBao Helm repo and runs `helm repo update`
2. Creates the `prod` namespace (idempotent)
3. Deploys via Helm with `helm/openbao/values.yaml`
4. Waits for `openbao-0` pod to reach `Ready`
5. Initializes with **5 key shares / threshold 3** (Shamir's Secret Sharing)
6. Saves unseal keys to `/root/openbao-init-keys.json` (chmod 600)
7. Auto-unseals using 3 of the 5 keys
8. Enables **KV v2** at path `secret/`
9. Enables and configures **Kubernetes auth method**

### 3.2 Manual Helm Deploy
```bash
helm repo add openbao https://openbao.github.io/openbao-helm
helm repo update

helm upgrade --install openbao openbao/openbao \
  --namespace prod \
  --create-namespace \
  -f helm/openbao/values.yaml \
  --wait --timeout 10m
```

### 3.3 Deployed Resources

| Resource | Type | Details |
|---|---|---|
| `openbao-0` | StatefulSet pod | Server pinned to `master.local` via nodeSelector |
| `openbao-agent-injector-*` | Deployment pod | Sidecar injector, schedules on workers |
| `service/openbao` | NodePort | `8200:30820` — UI and API |
| `service/openbao-agent-injector-svc` | ClusterIP | Webhook endpoint, port 443 |
| `service/openbao-internal` | Headless | Raft cluster communication, ports 8200/8201 |
| `PVC data-openbao-0` | 10 Gi | Encrypted data storage on `local-path` |
| `PVC audit-openbao-0` | 5 Gi | Audit log storage on `local-path` |

---

## 4. Daily Operations

### 4.1 Environment Setup
```bash
# Set the OpenBao address for all CLI commands
export BAO_ADDR="http://192.168.1.50:30820"

# Authenticate with root token (use a scoped token in production)
export BAO_TOKEN="<root-token>"

# Verify server is accessible and unsealed
bao status
```

Expected `bao status` output when healthy:
```
Key             Value
---             -----
Seal Type       shamir
Initialized     true
Sealed          false
Total Shares    5
Threshold       3
Version         2.6.0
HA Enabled      false
```

### 4.2 Checking Health
```bash
# Health endpoint (no auth required)
curl -s http://192.168.1.50:30820/v1/sys/health | python3 -m json.tool

# Pod health
kubectl get pods -n prod -l app.kubernetes.io/name=openbao

# Detailed pod status
kubectl describe pod openbao-0 -n prod

# View logs
kubectl logs openbao-0 -n prod

# Follow logs live
kubectl logs openbao-0 -n prod -f

# Agent injector logs
kubectl logs -n prod -l app.kubernetes.io/name=openbao-agent-injector
```

---

## 5. Unseal Operations

OpenBao **starts sealed after every restart**. In a sealed state all API calls return 503 and no secrets can be read.

### 5.1 Check Seal Status
```bash
export BAO_ADDR="http://192.168.1.50:30820"
bao status | grep -E "Sealed|Threshold|Unseal Progress"
```

### 5.2 Unseal Manually (3 of 5 keys required)
```bash
BAO_POD=$(kubectl get pod -n prod -l app.kubernetes.io/name=openbao \
  -o jsonpath='{.items[0].metadata.name}')

# Apply key 1
kubectl exec -n prod "${BAO_POD}" -- bao operator unseal <unseal-key-1>
# Apply key 2
kubectl exec -n prod "${BAO_POD}" -- bao operator unseal <unseal-key-2>
# Apply key 3 — vault becomes unsealed after this
kubectl exec -n prod "${BAO_POD}" -- bao operator unseal <unseal-key-3>

# Verify
kubectl exec -n prod "${BAO_POD}" -- bao status
```

### 5.3 Unseal Using the Init Keys File (Recovery)
```bash
# Read keys from the init file (if still present on the server)
KEYS=$(sudo cat /root/openbao-init-keys.json | python3 -c \
  "import sys,json; d=json.load(sys.stdin); [print(k) for k in d['unseal_keys_b64'][:3]]")

BAO_POD=$(kubectl get pod -n prod -l app.kubernetes.io/name=openbao \
  -o jsonpath='{.items[0].metadata.name}')

echo "${KEYS}" | while read key; do
  kubectl exec -n prod "${BAO_POD}" -- bao operator unseal "${key}"
done
```

### 5.4 Auto-Unseal via Script
The deploy script `10-deploy-openbao.sh` includes an unseal function. For automated unsealing after a pod restart, use:
```bash
# Re-run just the unseal portion
sudo bash scripts/master/10-deploy-openbao.sh --unseal-only
```

### 5.5 Seal (Emergency Lockdown)
```bash
export BAO_ADDR="http://192.168.1.50:30820"
export BAO_TOKEN="<root-token>"
bao operator seal
```

---

## 6. Secret Management

### 6.1 Write Secrets
```bash
export BAO_ADDR="http://192.168.1.50:30820"
export BAO_TOKEN="<root-token>"

# Write a single secret
bao kv put secret/myapp/config \
  db_password="s3cr3t" \
  api_key="abcdef123" \
  db_host="postgresql.databases.svc.cluster.local"

# Write from a JSON file
cat > /tmp/myapp-secrets.json <<'EOF'
{
  "db_password": "s3cr3t",
  "api_key": "abcdef123"
}
EOF
bao kv put secret/myapp/config @/tmp/myapp-secrets.json

# Update (creates new version, preserving old)
bao kv patch secret/myapp/config db_password="new_password"
```

### 6.2 Read Secrets
```bash
# Read all fields of a secret
bao kv get secret/myapp/config

# Read a specific field (useful in scripts)
bao kv get -field=db_password secret/myapp/config

# Read as JSON
bao kv get -format=json secret/myapp/config

# Read a specific version
bao kv get -version=2 secret/myapp/config
```

### 6.3 List Secrets
```bash
# List all secret paths under a prefix
bao kv list secret/

# List sub-paths
bao kv list secret/myapp/
```

### 6.4 Delete and Restore
```bash
# Soft delete (latest version — restorable)
bao kv delete secret/myapp/config

# Restore a soft-deleted version
bao kv undelete -versions=3 secret/myapp/config

# Permanently destroy a specific version
bao kv destroy -versions=1,2 secret/myapp/config

# Permanently destroy all versions (irreversible)
bao kv metadata delete secret/myapp/config
```

### 6.5 View Secret History
```bash
# View all version metadata
bao kv metadata get secret/myapp/config
```

### 6.6 Seed All Platform Secrets
```bash
# Seed all application secrets from the deploy script
sudo bash scripts/master/12-seed-openbao-secrets.sh
```

This seeds credentials for: grafana, prometheus, postgresql, mongodb, kafka, opensearch, kestra, doris, ranger, kerberos, sqlmesh, polaris, akhq, schema-registry, debezium.

---

## 7. Authentication & Authorization

### 7.1 Policy Management

Policies define what paths a token or role can access and with what capabilities (`create`, `read`, `update`, `delete`, `list`).

```bash
# Write a policy from stdin
bao policy write myapp-policy - <<'EOF'
# Allow reading all secrets under myapp/
path "secret/data/myapp/*" {
  capabilities = ["read", "list"]
}

# Allow listing secret paths
path "secret/metadata/myapp/*" {
  capabilities = ["list"]
}
EOF

# List all policies
bao policy list

# Read a policy
bao policy read myapp-policy

# Delete a policy
bao policy delete myapp-policy
```

### 7.2 Kubernetes Auth Configuration

The Kubernetes auth method lets pods authenticate using their ServiceAccount JWT token.

```bash
# Enable Kubernetes auth (already done by deploy script)
bao auth enable kubernetes

# Configure with current cluster settings
bao write auth/kubernetes/config \
  kubernetes_host="https://$(kubectl get svc kubernetes -o jsonpath='{.spec.clusterIP}'):443" \
  kubernetes_ca_cert=@/var/run/secrets/kubernetes.io/serviceaccount/ca.crt \
  token_reviewer_jwt=@/var/run/secrets/kubernetes.io/serviceaccount/token

# Verify auth configuration
bao read auth/kubernetes/config
```

### 7.3 Create a Kubernetes Auth Role
```bash
# Create a role: maps a ServiceAccount in specific namespaces to a policy
bao write auth/kubernetes/role/myapp \
  bound_service_account_names=myapp-sa \
  bound_service_account_namespaces=prod,staging \
  policies=myapp-policy \
  ttl=1h

# Allow multiple service accounts
bao write auth/kubernetes/role/data-platform \
  bound_service_account_names="kestra-sa,sqlmesh-sa,spark-sa" \
  bound_service_account_namespaces=analytics,orchestration \
  policies=data-platform-policy \
  ttl=4h

# List all roles
bao list auth/kubernetes/role

# Read a role
bao read auth/kubernetes/role/myapp
```

### 7.4 Token Management
```bash
# Create a token with specific policies (for external tools)
bao token create \
  -policy=myapp-policy \
  -ttl=24h \
  -display-name="myapp-deploy"

# Revoke a token
bao token revoke <token>

# Revoke all tokens with a specific accessor
bao token lookup -accessor <accessor>
bao token revoke -accessor <accessor>

# Renew a token
bao token renew <token>

# View current token info
bao token lookup
```

---

## 8. Agent Sidecar Injection

The OpenBao agent injector automatically injects a sidecar container into annotated pods. The sidecar authenticates to OpenBao using the pod's ServiceAccount and writes secrets to `/bao/secrets/` as files.

### 8.1 Annotated Pod Example
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: myapp
  namespace: prod
  annotations:
    # Enable injection
    bao-agent-inject: "true"
    # Specify the role (must exist in Kubernetes auth)
    bao-role: "myapp"
    # Secret to inject
    bao-agent-inject-secret-config: "secret/data/myapp/config"
    # Optional: custom template to format the output
    bao-agent-inject-template-config: |
      {{- with secret "secret/data/myapp/config" -}}
      export DB_PASSWORD="{{ .Data.data.db_password }}"
      export API_KEY="{{ .Data.data.api_key }}"
      {{- end }}
spec:
  serviceAccountName: myapp-sa
  containers:
    - name: myapp
      image: 192.168.1.50:30500/myapp:1.0
      # Source the injected file in your entrypoint:
      # source /bao/secrets/config
```

The secret is written to `/bao/secrets/config` inside the pod.

### 8.2 Verify Injection Worked
```bash
# Check the injected file is present
kubectl exec -n prod myapp -c myapp -- cat /bao/secrets/config

# Check the init container completed
kubectl describe pod myapp -n prod | grep -A5 "vault-agent-init"
```

---

## 9. Secrets Engines

### 9.1 List Enabled Engines
```bash
bao secrets list
```

### 9.2 Enable Additional Engines
```bash
# Enable a second KV mount (e.g., for a different team)
bao secrets enable -path=team-b/secret kv-v2

# Enable database engine for dynamic credentials
bao secrets enable database

# Configure PostgreSQL dynamic credentials
bao write database/config/postgresql \
  plugin_name=postgresql-database-plugin \
  allowed_roles="*" \
  connection_url="postgresql://{{username}}:{{password}}@postgresql.databases.svc.cluster.local:5432/metadata?sslmode=disable" \
  username="vault_admin" \
  password="<vault-admin-password>"

# Create a dynamic role
bao write database/roles/readonly \
  db_name=postgresql \
  creation_statements="CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}'; GRANT SELECT ON ALL TABLES IN SCHEMA public TO \"{{name}}\";" \
  default_ttl="1h" \
  max_ttl="24h"

# Generate dynamic credentials
bao read database/creds/readonly
```

---

## 10. Unseal Keys Management

### 10.1 Key Backup Procedure (CRITICAL — do this immediately after init)
```bash
# Step 1: Copy keys to a secure workstation
scp root@192.168.1.50:/root/openbao-init-keys.json ./openbao-keys-BACKUP.json

# Step 2: Confirm backup is readable
cat ./openbao-keys-BACKUP.json | python3 -m json.tool

# Step 3: Distribute keys to separate trusted team members
# Key 1 → Team Member A
# Key 2 → Team Member B
# Key 3 → Team Member C
# Key 4 → Team Member D (backup)
# Key 5 → Team Member E (backup)

# Step 4: DESTROY the file on the server
ssh root@192.168.1.50 "shred -u /root/openbao-init-keys.json"

# Step 5: Confirm file is gone
ssh root@192.168.1.50 "ls -la /root/openbao-init-keys.json 2>&1"
# Expected: ls: cannot access '/root/openbao-init-keys.json': No such file or directory
```

### 10.2 Rekey (Generate New Unseal Keys)
```bash
# Start a rekey operation (generates completely new keys)
bao operator rekey -init -key-shares=5 -key-threshold=3

# Supply old keys to authorize the rekey (need threshold=3)
bao operator rekey <old-key-1>
bao operator rekey <old-key-2>
bao operator rekey <old-key-3>
# New keys are printed — save them immediately
```

---

## 11. Audit Logging

### 11.1 Enable Audit Device
```bash
# Enable file audit (writes to the audit PVC mounted at /bao/audit)
bao audit enable file file_path=/bao/audit/audit.log

# List audit devices
bao audit list
```

### 11.2 Read Audit Logs
```bash
# From inside the pod
kubectl exec -n prod openbao-0 -- tail -f /bao/audit/audit.log | python3 -m json.tool
```

Each log entry is a JSON object containing: `type`, `time`, `auth.display_name`, `request.path`, `request.operation`, `response.auth`, and more.

---

## 12. Upgrade

```bash
# Check current version
bao version
kubectl get pod openbao-0 -n prod -o jsonpath='{.spec.containers[0].image}'

# Update Helm repo
helm repo update

# Check available chart versions
helm search repo openbao/openbao --versions | head -10

# Upgrade (OpenBao supports rolling upgrades)
helm upgrade openbao openbao/openbao \
  -n prod \
  -f helm/openbao/values.yaml

# Verify after upgrade
kubectl rollout status statefulset/openbao -n prod
bao status
```

> ⚠️ After upgrade the pod will restart. You must unseal with 3 of 5 keys before the API is available again.

---

## 13. Troubleshooting

### 13.1 OpenBao Sealed After Pod Restart
**Symptom:** `bao status` shows `Sealed: true`. All API calls return HTTP 503.

**Fix:** See [Section 5 — Unseal Operations](#5-unseal-operations).

### 13.2 Agent Injector Not Injecting Secrets
**Symptom:** Pod starts without `/bao/secrets/` directory. No `vault-agent-init` container visible.

**Diagnose:**
```bash
# Check the injector webhook is registered
kubectl get mutatingwebhookconfigurations | grep openbao

# Check injector pod is running
kubectl get pods -n prod -l app.kubernetes.io/name=openbao-agent-injector

# Check pod annotations are correct
kubectl get pod <pod-name> -n prod -o jsonpath='{.metadata.annotations}' | python3 -m json.tool
```

**Fix — common causes:**
- Missing `bao-agent-inject: "true"` annotation
- Role referenced in `bao-role` annotation does not exist
- ServiceAccount in pod does not match `bound_service_account_names` in the role
- OpenBao is sealed

### 13.3 `403 Permission Denied` Reading a Secret
**Symptom:** `bao kv get secret/myapp/config` returns `403 Forbidden`.

**Diagnose:**
```bash
# Check what policies the current token has
bao token lookup | grep -A5 policies

# Check the policy allows the path
bao policy read myapp-policy
```

**Fix:** Either update the policy to include the path, or authenticate with a token that has the correct policy.

### 13.4 `connection refused` on Port 30820
**Diagnose:**
```bash
kubectl get svc -n prod openbao
kubectl get pod openbao-0 -n prod
# Check iptables modules are loaded (RHEL 10 issue)
lsmod | grep iptable_filter
```

**Fix:** See `docs/platform-guide.md` §14.1 for the iptables modules fix.

---

## 14. Quick Reference

| Task | Command |
|---|---|
| Check seal status | `bao status` |
| Unseal (one key) | `bao operator unseal <key>` |
| Login | `bao login <token>` |
| Write secret | `bao kv put secret/app/key field=value` |
| Read secret | `bao kv get secret/app/key` |
| Read specific field | `bao kv get -field=myfield secret/app/key` |
| List secrets | `bao kv list secret/` |
| Delete (soft) | `bao kv delete secret/app/key` |
| List policies | `bao policy list` |
| Create policy | `bao policy write <name> <file>` |
| List K8s roles | `bao list auth/kubernetes/role` |
| Create K8s role | `bao write auth/kubernetes/role/<name> ...` |
| List tokens | `bao list auth/token/accessors` |
| Revoke token | `bao token revoke <token>` |
| View audit log | `kubectl exec -n prod openbao-0 -- tail /bao/audit/audit.log` |
