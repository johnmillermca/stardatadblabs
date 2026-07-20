# Runbook 08 — Security & Access: Kerberos, Ranger, Private Registry

> **Security namespace:** `security` · **Kerberos namespace:** `kerberos` · **Registry namespace:** `registry`  
> **Ranger Admin UI:** `http://192.168.1.50:30680`  
> **Private Registry:** `https://192.168.1.50:30500`  
> **Kerberos Realm:** `STARDATADBLABS.LOCAL`

---

## 1. Security Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Security Layer                                                          │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  OpenBao (prod namespace) — Secret Manager                       │   │
│  │  All application credentials stored and injected via sidecar     │   │
│  │  See Runbook 01 for full details                                  │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌─────────────────────────┐   ┌─────────────────────────────────────┐  │
│  │  Kerberos KDC           │   │  Apache Ranger 2.4.0                │  │
│  │  (kerberos namespace)   │   │  (security namespace)               │  │
│  │  Realm: STARDATADBLABS  │   │  Admin UI: NodePort 30680           │  │
│  │  KDC port: 88 TCP/UDP   │   │  RBAC + audit across all services   │  │
│  │  kadmin port: 749 TCP   │   │  Backed by PostgreSQL (ranger DB)   │  │
│  └────────────┬────────────┘   └────────────┬────────────────────────┘  │
│               │ authenticates               │ policy enforcement         │
│               │ service principals          │                            │
│  ┌────────────▼─────────────────────────────▼────────────────────────┐  │
│  │  Kerberized services:   Ranger-protected services:                │  │
│  │  Spark, HDFS, HBase     Kafka, OpenSearch, Doris, Spark, HDFS    │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  Private OCI Registry (registry namespace)                       │   │
│  │  TLS self-signed cert · NodePort 30500                           │   │
│  │  All platform images stored here                                  │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Kerberos KDC

### 2.1 What Is Kerberos in This Platform?
MIT Kerberos is the **centralized authentication service** for Hadoop-ecosystem services on the platform. It provides mutual authentication — both the client and the service prove their identity using cryptographic tickets issued by the Key Distribution Center (KDC), without passwords traveling over the network.

**How it works:**
1. A service (Spark executor, HDFS DataNode) registers a **principal** like `spark/worker1.local@STARDATADBLABS.LOCAL`
2. The KDC issues a **keytab** file containing a long-term key for that principal
3. At startup, the service uses the keytab to obtain a **Ticket Granting Ticket (TGT)** from the KDC
4. When a client connects, the service verifies the client's identity by demanding a valid Kerberos ticket

### 2.2 Deploy
```bash
# Seed admin credentials in OpenBao
sudo bash scripts/master/12-seed-openbao-secrets.sh

# Via ArgoCD
kubectl apply -f argocd-apps/app-kerberos.yaml

# Or manually
kubectl apply -f manifests/kerberos/kerberos-deployment.yaml
kubectl rollout status deployment/kerberos-kdc -n kerberos
```

### 2.3 Verify KDC is Running
```bash
# Check pod health
kubectl get pods -n kerberos
kubectl describe pod -n kerberos -l app=kerberos-kdc

# List all existing principals
kubectl exec -n kerberos deploy/kerberos-kdc -- \
  kadmin.local -q "listprincs"

# Expected output includes:
# K/M@STARDATADBLABS.LOCAL
# admin/admin@STARDATADBLABS.LOCAL
# krbtgt/STARDATADBLABS.LOCAL@STARDATADBLABS.LOCAL
```

### 2.4 Principal Management

#### Create a Service Principal
```bash
# Create a host principal
kubectl exec -n kerberos deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc -randkey host/worker1.local@STARDATADBLABS.LOCAL"

# Create a Spark service principal
kubectl exec -n kerberos deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc -randkey spark/worker1.local@STARDATADBLABS.LOCAL"

# Create a user principal (with password prompt)
kubectl exec -it -n kerberos deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc jdoe@STARDATADBLABS.LOCAL"

# Create a user principal with a specific password (non-interactive)
kubectl exec -n kerberos deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc -pw <password> jdoe@STARDATADBLABS.LOCAL"

# Delete a principal
kubectl exec -n kerberos deploy/kerberos-kdc -- \
  kadmin.local -q "delprinc spark/old-worker.local@STARDATADBLABS.LOCAL"

# Change a principal's password
kubectl exec -n kerberos deploy/kerberos-kdc -- \
  kadmin.local -q "cpw -pw <new-password> jdoe@STARDATADBLABS.LOCAL"

# View details for a principal
kubectl exec -n kerberos deploy/kerberos-kdc -- \
  kadmin.local -q "getprinc spark/worker1.local@STARDATADBLABS.LOCAL"
```

#### Export a Keytab
```bash
# Export a keytab for a service principal (stores it in /tmp inside the pod)
kubectl exec -n kerberos deploy/kerberos-kdc -- \
  kadmin.local -q "ktadd -k /tmp/spark.keytab spark/worker1.local@STARDATADBLABS.LOCAL"

# Copy the keytab out of the pod
kubectl cp kerberos/$(kubectl get pod -n kerberos -l app=kerberos-kdc \
  -o jsonpath='{.items[0].metadata.name}'):/tmp/spark.keytab ./spark.keytab

# Verify the keytab contents
klist -ekt ./spark.keytab
```

#### Create Keytabs for Multiple Workers
```bash
KDC_POD=$(kubectl get pod -n kerberos -l app=kerberos-kdc \
  -o jsonpath='{.items[0].metadata.name}')

for i in 1 2 3 4; do
  # Add principal
  kubectl exec -n kerberos "${KDC_POD}" -- \
    kadmin.local -q "addprinc -randkey spark/worker${i}.local@STARDATADBLABS.LOCAL"

  # Export keytab
  kubectl exec -n kerberos "${KDC_POD}" -- \
    kadmin.local -q "ktadd -k /tmp/spark-worker${i}.keytab spark/worker${i}.local@STARDATADBLABS.LOCAL"

  # Copy to local
  kubectl cp kerberos/${KDC_POD}:/tmp/spark-worker${i}.keytab ./spark-worker${i}.keytab
  echo "Keytab for worker${i} created"
done
```

### 2.5 Store Keytabs as Kubernetes Secrets
```bash
# Store a keytab as a K8s Secret for pod mounting
kubectl create secret generic spark-keytab \
  --from-file=spark.keytab=./spark.keytab \
  -n analytics

# Mount in a pod spec
# spec:
#   volumes:
#     - name: spark-keytab
#       secret:
#         secretName: spark-keytab
#   containers:
#     - name: spark
#       volumeMounts:
#         - name: spark-keytab
#           mountPath: /etc/security/keytabs
#           readOnly: true
```

### 2.6 krb5.conf for Client Pods
```bash
# Create a ConfigMap with krb5.conf
kubectl create configmap krb5-conf \
  --from-literal=krb5.conf='
[libdefaults]
    default_realm = STARDATADBLABS.LOCAL
    dns_lookup_realm = false
    dns_lookup_kdc = false
    forwardable = true
    renewable = true
    ticket_lifetime = 24h
    renew_lifetime = 7d

[realms]
    STARDATADBLABS.LOCAL = {
        kdc = kerberos-kdc.kerberos.svc.cluster.local
        admin_server = kerberos-kadmin.kerberos.svc.cluster.local
    }

[domain_realm]
    .cluster.local = STARDATADBLABS.LOCAL
    cluster.local = STARDATADBLABS.LOCAL
' -n analytics
```

### 2.7 Obtain a Kerberos Ticket (kinit)
```bash
# Exec into a Kerberized pod and obtain a ticket
kubectl exec -it -n analytics <spark-pod> -- \
  kinit -kt /etc/security/keytabs/spark.keytab spark/worker1.local@STARDATADBLABS.LOCAL

# Verify the ticket
kubectl exec -n analytics <spark-pod> -- klist

# Renew a ticket
kubectl exec -n analytics <spark-pod> -- kinit -R

# Destroy the ticket
kubectl exec -n analytics <spark-pod> -- kdestroy
```

### 2.8 KDC Monitoring
```bash
# View KDC logs
kubectl logs -n kerberos deploy/kerberos-kdc -f

# Check database size
kubectl exec -n kerberos deploy/kerberos-kdc -- \
  du -sh /var/kerberos/krb5kdc/

# List all principals (with last login time)
kubectl exec -n kerberos deploy/kerberos-kdc -- \
  kadmin.local -q "listprincs" | while read p; do
    kubectl exec -n kerberos deploy/kerberos-kdc -- \
      kadmin.local -q "getprinc $p" 2>/dev/null | grep -E "Principal|Last pwchange"
  done
```

---

## 3. Apache Ranger — Fine-Grained Authorization

### 3.1 What Is Apache Ranger?
Apache Ranger provides **centralized, policy-based authorization** across the entire data platform. While Kerberos answers "who are you?", Ranger answers "what are you allowed to do?". It enforces:

| Feature | Description |
|---|---|
| **Resource-based policies** | Allow/deny access to specific topics, tables, columns, indices |
| **Row-level filtering** | Return only rows matching a filter expression per user/group |
| **Column masking** | Auto-mask sensitive columns (e.g., show `****-XXXX` instead of SSN) |
| **Audit logging** | Every access attempt is logged to HDFS, Solr, or Elasticsearch |
| **User sync** | Sync users/groups from LDAP/AD |
| **Tag-based policies** | Apply policies via metadata tags rather than explicit paths |

### 3.2 Prerequisites
```bash
# 1. PostgreSQL must be running with the ranger database
psql -U postgres -c "CREATE DATABASE ranger;"
psql -U postgres -c "CREATE USER ranger WITH PASSWORD '<password>';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE ranger TO ranger;"

# 2. Build and push the Ranger Docker image
docker pull apache/ranger:2.4.0
docker tag apache/ranger:2.4.0 192.168.1.50:30500/apache-ranger:2.7.0
docker push 192.168.1.50:30500/apache-ranger:2.7.0

# 3. Seed secrets
sudo bash scripts/master/12-seed-openbao-secrets.sh
```

### 3.3 Deploy
```bash
# Via ArgoCD
kubectl apply -f argocd-apps/app-ranger.yaml

# Or manually
kubectl apply -f manifests/ranger/ranger-deployment.yaml
kubectl rollout status deployment/ranger-admin -n security
```

### 3.4 Access the Admin UI
```bash
# Get admin password
RANGER_PASS=$(kubectl get secret ranger-db-credentials -n security \
  -o jsonpath='{.data.admin-password}' | base64 -d)
echo "Password: ${RANGER_PASS}"

# Open in browser
open http://192.168.1.50:30680

# Login: admin / <password-above>
```

### 3.5 Ranger REST API
```bash
RANGER_PASS=$(kubectl get secret ranger-db-credentials -n security \
  -o jsonpath='{.data.admin-password}' | base64 -d)

# Check health
curl -u admin:"${RANGER_PASS}" http://192.168.1.50:30680/service/public/v2/api/service

# List all services (registered plugins)
curl -u admin:"${RANGER_PASS}" \
  http://192.168.1.50:30680/service/public/v2/api/service | python3 -m json.tool

# List all policies for a service
curl -u admin:"${RANGER_PASS}" \
  "http://192.168.1.50:30680/service/public/v2/api/policy?serviceName=kafka-service" \
  | python3 -m json.tool

# Create a new policy
curl -X POST -u admin:"${RANGER_PASS}" \
  http://192.168.1.50:30680/service/public/v2/api/policy \
  -H "Content-Type: application/json" \
  -d '{
    "name": "allow-kafka-read",
    "service": "kafka-service",
    "isEnabled": true,
    "resources": {
      "topic": { "values": ["events-*"], "isRecursive": true }
    },
    "policyItems": [
      {
        "accesses": [{"type": "consume", "isAllowed": true}],
        "users": ["kestra-user"],
        "groups": ["data-engineers"],
        "conditions": [],
        "delegateAdmin": false
      }
    ]
  }'

# Delete a policy by ID
curl -X DELETE -u admin:"${RANGER_PASS}" \
  http://192.168.1.50:30680/service/public/v2/api/policy/<policy-id>
```

### 3.6 Register a Service Plugin

To protect a new service (e.g., Kafka), register it in Ranger:

```bash
RANGER_PASS=$(kubectl get secret ranger-db-credentials -n security \
  -o jsonpath='{.data.admin-password}' | base64 -d)

# Register a Kafka service
curl -X POST -u admin:"${RANGER_PASS}" \
  http://192.168.1.50:30680/service/public/v2/api/service \
  -H "Content-Type: application/json" \
  -d '{
    "type": "kafka",
    "name": "kafka-service",
    "displayName": "Platform Kafka",
    "isEnabled": true,
    "configs": {
      "bootstrap.servers": "kafka.streaming.svc.cluster.local:9092",
      "security.protocol": "SASL_PLAINTEXT",
      "sasl.mechanism": "PLAIN",
      "username": "kafka-user",
      "password": "<kafka-password>"
    }
  }'
```

### 3.7 Configure Ranger + Kerberos Integration
```bash
# Ranger Admin requires SPNEGO for Kerberos-protected UI/API
# Add these properties to ranger-admin-site.xml:
# xasecure.audit.destination.hdfs.config.conf.dir=/etc/krb5.conf
# ranger.spnego.kerberos.principal=HTTP/ranger.security.svc.cluster.local@STARDATADBLABS.LOCAL
# ranger.spnego.kerberos.keytab=/etc/security/keytabs/spnego.keytab

# Create the SPNEGO principal
kubectl exec -n kerberos deploy/kerberos-kdc -- \
  kadmin.local -q "addprinc -randkey HTTP/ranger.security.svc.cluster.local@STARDATADBLABS.LOCAL"
kubectl exec -n kerberos deploy/kerberos-kdc -- \
  kadmin.local -q "ktadd -k /tmp/spnego.keytab HTTP/ranger.security.svc.cluster.local@STARDATADBLABS.LOCAL"
```

### 3.8 Audit Log Access
```bash
# Ranger writes audit logs to the audit-log PVC
# Access audit logs from inside the Ranger pod
RANGER_POD=$(kubectl get pod -n security -l app=ranger-admin \
  -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n security "${RANGER_POD}" -- \
  tail -f /var/log/ranger/audit/RangerAudit.log | python3 -m json.tool
```

---

## 4. Private Docker Registry

### 4.1 What Is the Private Registry?
A self-hosted OCI-compatible Docker registry deployed in the `registry` namespace. All platform-specific Docker images (Spark+Gluten+Velox, SQLMesh, MCP servers, Ranger, Polaris, etc.) are built and stored here, eliminating dependency on public image registries for production deployments.

| Property | Value |
|---|---|
| URL | `https://192.168.1.50:30500` |
| Alt URL | `https://registry.local:30500` |
| Storage | PVC 50 Gi on `local-path` |
| TLS | Self-signed, 10-year validity |
| Auth | None (internal cluster — add htpasswd for production) |

### 4.2 Deploy
```bash
sudo bash scripts/registry/06-registry-setup.sh
```

This script:
1. Generates a self-signed TLS certificate in `/etc/k8s-registry-certs/`
2. Creates the `registry` namespace and `registry-tls` K8s secret
3. Applies PVC, Deployment, Service manifests
4. Configures containerd trust on all nodes
5. Adds `registry.local → 192.168.1.50` to `/etc/hosts` on all nodes

### 4.3 Build and Push an Image
```bash
# Build locally
docker build -t 192.168.1.50:30500/myapp:1.0 ./myapp/

# Push to registry
docker push 192.168.1.50:30500/myapp:1.0

# Tag an existing image
docker tag nginx:latest 192.168.1.50:30500/nginx:latest
docker push 192.168.1.50:30500/nginx:latest
```

### 4.4 List Images in the Registry
```bash
# List all repositories
curl -sk --cacert /etc/k8s-registry-certs/registry.crt \
  https://192.168.1.50:30500/v2/_catalog

# List tags for a specific image
curl -sk --cacert /etc/k8s-registry-certs/registry.crt \
  https://192.168.1.50:30500/v2/myapp/tags/list

# Get manifest for a specific tag
curl -sk --cacert /etc/k8s-registry-certs/registry.crt \
  -H "Accept: application/vnd.docker.distribution.manifest.v2+json" \
  https://192.168.1.50:30500/v2/myapp/manifests/1.0
```

### 4.5 Delete an Image
```bash
# Step 1: Get the digest for the tag
DIGEST=$(curl -sk --cacert /etc/k8s-registry-certs/registry.crt \
  -H "Accept: application/vnd.docker.distribution.manifest.v2+json" \
  -I https://192.168.1.50:30500/v2/myapp/manifests/1.0 \
  | grep docker-content-digest | awk '{print $2}' | tr -d '\r')

# Step 2: Delete by digest
curl -sk --cacert /etc/k8s-registry-certs/registry.crt \
  -X DELETE \
  https://192.168.1.50:30500/v2/myapp/manifests/${DIGEST}

# Step 3: Run garbage collection to reclaim disk space
kubectl exec -n registry deploy/private-registry -- \
  registry garbage-collect /etc/docker/registry/config.yml
```

### 4.6 Trust the Certificate on a New Workstation
```bash
# Copy the certificate from the master
scp star_master@192.168.1.50:/etc/k8s-registry-certs/registry.crt .

# Ubuntu / Debian
sudo cp registry.crt /usr/local/share/ca-certificates/k8s-registry.crt
sudo update-ca-certificates
sudo systemctl restart docker

# RHEL / Fedora / Rocky
sudo cp registry.crt /etc/pki/ca-trust/source/anchors/k8s-registry.crt
sudo update-ca-trust
sudo systemctl restart docker

# Configure Docker directly (alternative)
sudo mkdir -p /etc/docker/certs.d/192.168.1.50:30500
sudo cp registry.crt /etc/docker/certs.d/192.168.1.50:30500/ca.crt
sudo systemctl restart docker
```

### 4.7 Configure containerd on a New Node
```bash
# Create trust directory for the registry
sudo mkdir -p /etc/containerd/certs.d/192.168.1.50:30500

# Write the hosts.toml config
sudo tee /etc/containerd/certs.d/192.168.1.50:30500/hosts.toml <<'EOF'
server = "https://192.168.1.50:30500"

[host."https://192.168.1.50:30500"]
  capabilities = ["pull", "resolve", "push"]
  ca = "/etc/k8s-registry-certs/registry.crt"
EOF

# Copy the certificate
sudo mkdir -p /etc/k8s-registry-certs
sudo scp star_master@192.168.1.50:/etc/k8s-registry-certs/registry.crt \
  /etc/k8s-registry-certs/registry.crt

# Restart containerd
sudo systemctl restart containerd
```

### 4.8 Rotate the TLS Certificate
```bash
# Regenerate the certificate (10-year validity)
openssl req -x509 -newkey rsa:4096 -nodes -days 3650 \
  -keyout /etc/k8s-registry-certs/registry.key \
  -out    /etc/k8s-registry-certs/registry.crt \
  -subj   "/CN=registry.local/O=K8sPlatformRegistry" \
  -addext "subjectAltName=DNS:registry.local,IP:192.168.1.50"

# Update the K8s TLS secret
kubectl create secret tls registry-tls \
  --cert=/etc/k8s-registry-certs/registry.crt \
  --key=/etc/k8s-registry-certs/registry.key \
  -n registry \
  --dry-run=client -o yaml | kubectl apply -f -

# Restart the registry pod
kubectl rollout restart deployment/private-registry -n registry

# Re-distribute the new cert to all nodes
for ip in 192.168.1.51 192.168.1.52 192.168.1.53 192.168.1.54; do
  idx=$((${ip##*.} - 50))
  sudo scp /etc/k8s-registry-certs/registry.crt \
    star_worker${idx}@${ip}:/tmp/registry.crt
  sudo ssh star_worker${idx}@${ip} \
    "sudo mv /tmp/registry.crt /etc/k8s-registry-certs/registry.crt && sudo systemctl restart containerd"
done

# Re-run the registry setup script to push the new cert everywhere
bash scripts/registry/06-registry-setup.sh
```

### 4.9 Check Registry Storage
```bash
# Check PVC usage
kubectl get pvc -n registry

# Check actual disk usage inside the registry
kubectl exec -n registry deploy/private-registry -- \
  du -sh /var/lib/registry/docker/registry/v2/repositories/

# Registry pod logs
kubectl logs -n registry deploy/private-registry --tail=50
```

### 4.10 Add Authentication to the Registry (Production)
```bash
# Generate htpasswd file
docker run --rm --entrypoint htpasswd \
  httpd:2 -Bbn registry_user registry_password \
  > /etc/k8s-registry-certs/htpasswd

# Create K8s secret from htpasswd file
kubectl create secret generic registry-auth \
  --from-file=htpasswd=/etc/k8s-registry-certs/htpasswd \
  -n registry

# Update the registry deployment to use the auth secret
# (mount at /auth/htpasswd and set REGISTRY_AUTH env vars)
```

---

## 5. Network Policies (Production Hardening)

### 5.1 Default Deny All Ingress (Prod Namespace)
```yaml
# Apply as manifests/namespaces/netpol-prod-default-deny.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-ingress
  namespace: prod
spec:
  podSelector: {}
  policyTypes: [Ingress]
```

### 5.2 Allow Only Specific Traffic to OpenBao
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-openbao-ingress
  namespace: prod
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: openbao
  policyTypes: [Ingress]
  ingress:
    # Allow only pods with the "inject" label to call the agent injector
    - from:
        - namespaceSelector: {}
          podSelector:
            matchLabels:
              bao-agent-inject: "true"
      ports:
        - port: 8200
    # Allow ArgoCD to reach the API
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: argocd
      ports:
        - port: 8200
```

---

## 6. RBAC Reference

### 6.1 OpenBao Role-Based Policies
All application credentials follow the **least privilege** pattern in OpenBao:

| Application | OpenBao Policy | Allowed Paths |
|---|---|---|
| Grafana | `grafana-policy` | `secret/data/grafana/*` read |
| Prometheus | `prometheus-policy` | `secret/data/prometheus/*` read |
| Kestra | `kestra-policy` | `secret/data/kestra/*` read |
| Ranger | `ranger-policy` | `secret/data/ranger/*` read |
| Debezium | `debezium-policy` | `secret/data/debezium/*` read |
| Admin | `platform-admin` | `secret/*` create/read/update/delete |

### 6.2 ArgoCD RBAC
```yaml
# In helm/argocd/values.yaml
server:
  rbacConfig:
    policy.default: role:readonly
    policy.csv: |
      p, role:org-admin, applications, *, */*, allow
      p, role:org-admin, clusters, get, *, allow
      p, role:org-admin, repositories, *, *, allow
      g, admin, role:org-admin
```

---

## 7. Troubleshooting

### 7.1 Kerberos `kinit` Fails — "Cannot find KDC"
```bash
# Check KDC DNS resolution from within the pod
kubectl exec -n analytics <pod> -- \
  nslookup kerberos-kdc.kerberos.svc.cluster.local

# Check KDC pod is running
kubectl get pods -n kerberos

# Check KDC logs for authentication failures
kubectl logs -n kerberos deploy/kerberos-kdc | grep -i "error\|fail"
```

### 7.2 Ranger Admin UI Not Loading
```bash
# Check Ranger pod status
kubectl get pods -n security
kubectl logs -n security deploy/ranger-admin --tail=50

# Check PostgreSQL connectivity from Ranger
kubectl exec -n security deploy/ranger-admin -- \
  psql -h postgresql.databases.svc.cluster.local -U ranger -d ranger -c "\l"
```

### 7.3 Registry Push Fails — "x509: certificate signed by unknown authority"
```bash
# The registry cert is not trusted on the pushing machine
# Copy and trust the certificate (see §4.6)
scp star_master@192.168.1.50:/etc/k8s-registry-certs/registry.crt .

# Quick test after trusting
docker pull 192.168.1.50:30500/hello-world:latest 2>&1 || \
  echo "Trust not configured correctly"
```

### 7.4 Registry PVC Full
```bash
# Check usage
kubectl exec -n registry deploy/private-registry -- \
  du -sh /var/lib/registry/docker/registry/v2/

# Run garbage collection to remove unreferenced layers
kubectl exec -n registry deploy/private-registry -- \
  registry garbage-collect /etc/docker/registry/config.yml --delete-untagged

# If still full, increase the PVC
kubectl patch pvc registry-pvc -n registry \
  -p '{"spec":{"resources":{"requests":{"storage":"100Gi"}}}}'
```

### 7.5 Keytab Authentication Fails — "Key version number for principal in key table is incorrect"
```bash
# The keytab is outdated — the KDC renewed the key
# Re-export the keytab
kubectl exec -n kerberos deploy/kerberos-kdc -- \
  kadmin.local -q "ktadd -k /tmp/new.keytab spark/worker1.local@STARDATADBLABS.LOCAL"

kubectl cp kerberos/$(kubectl get pod -n kerberos -l app=kerberos-kdc \
  -o jsonpath='{.items[0].metadata.name}'):/tmp/new.keytab ./spark-new.keytab

# Update the K8s Secret
kubectl create secret generic spark-keytab \
  --from-file=spark.keytab=./spark-new.keytab \
  -n analytics \
  --dry-run=client -o yaml | kubectl apply -f -

# Restart the pod to pick up the new keytab
kubectl rollout restart deployment/spark-worker -n analytics
```

---

## 8. Quick Security Reference

| Task | Command |
|---|---|
| List Kerberos principals | `kubectl exec -n kerberos deploy/kerberos-kdc -- kadmin.local -q "listprincs"` |
| Create principal | `kadmin.local -q "addprinc -randkey <name>@STARDATADBLABS.LOCAL"` |
| Export keytab | `kadmin.local -q "ktadd -k /tmp/out.keytab <principal>"` |
| Verify keytab | `klist -ekt <keytab-file>` |
| Obtain ticket | `kinit -kt <keytab> <principal>` |
| Check ticket | `klist` |
| Ranger health | `curl -u admin:<pass> http://192.168.1.50:30680/service/public/v2/api/service` |
| List Ranger policies | `curl -u admin:<pass> http://192.168.1.50:30680/service/public/v2/api/policy` |
| List registry images | `curl -sk https://192.168.1.50:30500/v2/_catalog` |
| Registry GC | `kubectl exec -n registry deploy/private-registry -- registry garbage-collect /etc/docker/registry/config.yml` |
| Rotate registry cert | `bash scripts/registry/06-registry-setup.sh` |
