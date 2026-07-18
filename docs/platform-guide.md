# Kubernetes Build Platform — Documentation Hub

> **Cluster:** 1 master + 4 workers · **OS:** Ubuntu 22.04 / RHEL 9  
> **Version targets:** Kubernetes 1.30 · containerd 1.7 · Calico v3.27 · ArgoCD 2.11 · OpenBao latest

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Cluster Preparation](#2-cluster-preparation)
3. [Storage Setup](#3-storage-setup) — §3.2 SSH key setup · §3.3 run order
4. [Private Docker Registry](#4-private-docker-registry)
5. [Staging & Testing Directories](#5-staging--testing-directories)
6. [ArgoCD – GitOps Toolchain](#6-argocd--gitops-toolchain)
7. [Helm – Application Packaging](#7-helm--application-packaging)
8. [OpenBao – Secret Manager](#8-openbao--secret-manager)
9. [Namespace Quotas](#9-namespace-quotas)
10. [ArgoCD Application Manifests](#10-argocd-application-manifests)
11. [Runbook – Day 2 Operations](#11-runbook--day-2-operations)
12. [Security Considerations](#12-security-considerations)
13. [Troubleshooting](#13-troubleshooting)
14. [RHEL 10 / kube-proxy / Calico Networking — Field Notes](#14-rhel-10--kube-proxy--calico-networking--field-notes)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        192.168.1.0/24                               │
│                                                                     │
│   ┌──────────────────────────────────────────────────────────────┐  │
│   │  master  192.168.1.50                                        │  │
│   │  • kube-apiserver  • etcd  • kube-scheduler                  │  │
│   │  • kube-controller-manager                                   │  │
│   │  • ArgoCD  (NodePort 30443)                                  │  │
│   │  • OpenBao (NodePort 30820)                                  │  │
│   │  • Private Registry (NodePort 30500)                         │  │
│   │  • /opt/k8s-builds/{staging,testing}                         │  │
│   └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌──────────┐  │
│   │ worker1     │  │ worker2     │  │ worker3     │  │ worker4  │  │
│   │ .51         │  │ .52         │  │ .53         │  │ .54      │  │
│   │ kubelet     │  │ kubelet     │  │ kubelet     │  │ kubelet  │  │
│   │ containerd  │  │ containerd  │  │ containerd  │  │ containerd│ │
│   └─────────────┘  └─────────────┘  └─────────────┘  └──────────┘  │
└─────────────────────────────────────────────────────────────────────┘

CNI: Calico (pod CIDR 10.244.0.0/16)
Storage: local-path-provisioner → /opt/local-path-provisioner on each node
GitOps: ArgoCD watches Git repo → reconciles cluster state
Secrets: OpenBao (KV v2 + Kubernetes auth)
```

### Namespace Layout

| Namespace | Purpose | Quota |
|---|---|---|
| `prod` | Production workloads + OpenBao secret manager | Full capacity |
| `test` | Development / test | 1/5 of prod |
| `staging` | Pre-prod build validation | No hard quota |
| `testing` (K8s) | Integration test jobs | No hard quota |
| `registry` | Private OCI registry | — |
| `argocd` | ArgoCD control plane | — |
| `local-path-storage` | Storage provisioner | — |

### Port Reference

| Service | Port | Protocol |
|---|---|---|
| Kubernetes API | 6443 | HTTPS |
| ArgoCD UI | 30443 | HTTPS (NodePort) |
| OpenBao UI/API | 30820 | HTTP (NodePort) |
| Private Registry | 30500 | HTTPS (NodePort) |
| Registry (internal) | 5000 | HTTPS (ClusterIP) |

---

## 2. Cluster Preparation

### 2.1 Prerequisites

All nodes require:
- Ubuntu 22.04 LTS **or** RHEL 9 (tested; other versions work with minor adjustments)
- A non-root user with passwordless `sudo` on every worker node
- Static IP addresses or DHCP reservations
- Passwordless SSH key access from the master to each worker (see §2.2)
- Minimum hardware: 2 vCPU / 4 GB RAM / 50 GB disk per node

### 2.2 Worker Inventory (`workers.conf`)

Worker node details are kept in [`scripts/workers.conf`](../scripts/workers.conf) — one line per node:

```
# IP / hostname    SSH user (non-root, must have passwordless sudo)
192.168.1.51       star_worker1
192.168.1.52       star_worker2
192.168.1.53       star_worker3
192.168.1.54       star_worker4
```

Edit this file to match your environment before running anything. Each customer's username can differ per node — the script reads the file at runtime.

### 2.3 SSH Key Setup (master → workers)

You run `deploy-workers.sh` as `star_master` with `sudo`. That means the **entire script runs as root** — and `ssh` inside the script looks for keys in `/root/.ssh/`, not in `~star_master/.ssh/`. All three key-setup commands below must therefore also be run with `sudo` so they operate on the same `/root/.ssh/` directory.

```bash
# Step 1 — Generate an SSH key for root (once only).
# Must use sudo: the key must land in /root/.ssh/, not ~star_master/.ssh/.
# -N "" = empty passphrase so the script can use it non-interactively.
sudo ssh-keygen -t ed25519 -f /root/.ssh/id_ed25519 -N ""

# Step 2 — Push root's public key to every worker in workers.conf.
# Must use sudo: so ssh-copy-id reads the key from /root/.ssh/.
# IP and username are read directly from the inventory — no hardcoding.
# You will be prompted for each worker user's password once.
# ssh-copy-id is not affected by the stdin problem so the pipe form is fine here.
grep -v '^\s*#' scripts/workers.conf | grep -v '^\s*$' | while read -r ip user; do
  sudo ssh-copy-id "${user}@${ip}"
done

# Step 3 — Verify passwordless access for every worker in workers.conf.
# Must use sudo: so ssh uses root's key in /root/.ssh/.
# <&3 keeps stdin as your terminal; the worker list is read from fd 3.
while read -r ip user <&3; do
  sudo ssh -n -o BatchMode=yes "${user}@${ip}" hostname && echo "${ip}: OK" || echo "${ip}: FAILED"
done 3< <(grep -v '^\s*#' scripts/workers.conf | grep -v '^\s*$')
```

> **Why `sudo` on every step?**
> `sudo bash deploy-workers.sh` runs the script as `root`. Inside the script, `ssh` and `scp` run as `root` and look for keys in `/root/.ssh/`. If you generate or push the key without `sudo` (as `star_master`), the key ends up in `~star_master/.ssh/` and the script will fail with a `BatchMode` authentication error.

> **Why not root SSH to workers?**
> Most enterprise-hardened images set `PermitRootLogin no`. Connecting as a named non-root user with `sudo` satisfies security policy and keeps privileged actions traceable to a real user account.

### 2.4 Passwordless sudo on workers

> ⚠️ **Required before running `deploy-workers.sh`.** The script SSHes into each worker over a non-interactive session and runs commands with `sudo`. Without a `NOPASSWD` rule, sudo will fail with:
> `sudo: a terminal is required to read the password`

Run this from the **master** once SSH key access is working (§2.3). It reads usernames and IPs directly from `workers.conf`:

```bash
# From the project root on the master.
# <&3 keeps stdin as your terminal so -t can allocate a pseudo-terminal for
# the remote sudo password prompt; the worker list is read from fd 3.
while read -r ip user <&3; do
  echo "Setting NOPASSWD sudo on ${ip} for ${user}..."
  sudo ssh -t "${user}@${ip}" \
    "echo '${user} ALL=(ALL) NOPASSWD: ALL' | sudo tee /etc/sudoers.d/${user} && sudo chmod 0440 /etc/sudoers.d/${user}"
done 3< <(grep -v '^\s*#' scripts/workers.conf | grep -v '^\s*$')
```

Expected output per worker:
```
Setting NOPASSWD sudo on 192.168.1.51 for star_worker1...
[sudo] password for star_worker1:        ← enter the worker user's password once
star_worker1 ALL=(ALL) NOPASSWD: ALL     ← confirms the rule was written
Connection to 192.168.1.51 closed.
```

Verify that passwordless sudo works from the master before proceeding:

```bash
while read -r ip user <&3; do
  sudo ssh -n "${user}@${ip}" sudo whoami && echo "${ip}: OK" || echo "${ip}: FAILED"
done 3< <(grep -v '^\s*#' scripts/workers.conf | grep -v '^\s*$')
```

Expected output:
```
root
192.168.1.51: OK
root
192.168.1.52: OK
root
192.168.1.53: OK
root
192.168.1.54: OK
```

> **Note:** Plain `ssh star_worker1@192.168.1.51` (without `sudo`) will still prompt for a password — that is normal and expected. The deploy script always uses `sudo ssh` which reads root's key from `/root/.ssh/`. Plain `ssh` as `star_master` looks in `~star_master/.ssh/` where no key was installed.

### 2.5 Run Order

```bash
# ── Step 1: prerequisites on the MASTER ──────────────────────────────
sudo bash scripts/workers/00-common-prereqs.sh

# ── Step 2: initialise the cluster on the MASTER ─────────────────────
sudo bash scripts/master/01-kubeadm-init.sh

# ── Step 3: bootstrap ALL workers from the master ────────────────────
# Reads scripts/workers.conf and SSHes into each worker as the named user,
# copying and running 00-common-prereqs.sh + 02-worker-join.sh with sudo.
sudo bash scripts/master/deploy-workers.sh

# Use a different inventory file:
# sudo bash scripts/master/deploy-workers.sh --inventory /path/to/workers.conf

# Or target specific workers inline without a file (IP:USER format):
# sudo bash scripts/master/deploy-workers.sh 192.168.1.51:star_worker1

# ── Step 4: label workers and verify ─────────────────────────────────
sudo bash scripts/master/03-label-workers.sh
```

> **Manual alternative** (run directly on each worker instead of step 3):
> ```bash
> # On each worker node
> sudo bash scripts/workers/00-common-prereqs.sh
> sudo bash scripts/workers/02-worker-join.sh 192.168.1.50
> ```

### 2.6 What `deploy-workers.sh` Does

[`scripts/master/deploy-workers.sh`](../scripts/master/deploy-workers.sh) is an orchestrator that runs entirely on the master. It reads [`scripts/workers.conf`](../scripts/workers.conf) for node IPs and usernames, then for each worker:

| Step | Action |
|---|---|
| Connectivity check | `ssh -o BatchMode=yes <user>@<worker>` — skips the node gracefully if unreachable |
| Copy scripts | `scp` as the worker user copies scripts to `/tmp/k8s-scripts/` (no sudo needed — `/tmp` is world-writable) |
| Run prerequisites | `sudo bash` executes `00-common-prereqs.sh` remotely (swap, kernel modules, containerd, kubeadm…) |
| Run join | `sudo bash` executes `02-worker-join.sh <MASTER_IP>` — fetches a fresh token from the master via SSH |

One worker failure does **not** abort the rest; per-worker errors are logged and execution continues.

**Runtime overrides:**
```bash
# Change master IP
MASTER_IP=10.0.0.1 sudo bash scripts/master/deploy-workers.sh

# Use a custom inventory file
sudo bash scripts/master/deploy-workers.sh --inventory /etc/k8s/workers.conf

# Pass workers inline (no inventory file needed)
sudo bash scripts/master/deploy-workers.sh 192.168.1.51:star_worker1 192.168.1.52:star_worker2
```

### 2.7 What `00-common-prereqs.sh` Does

| Step | Action |
|---|---|
| Swap | `swapoff -a` + removes swap from `/etc/fstab` |
| Kernel modules | `overlay`, `br_netfilter` loaded at boot |
| sysctl | `net.bridge.*`, `ip_forward`, inotify limits |
| Firewall | firewalld → trusted zone; ufw disabled |
| containerd | Installed from Docker repo, configured with `SystemdCgroup = true` |
| kubeadm/kubelet/kubectl | Installed from pkgs.k8s.io, version-pinned |
| crictl | Configured for containerd socket |

### 2.8 What `01-kubeadm-init.sh` Does

- Pre-pulls control-plane images
- Runs `kubeadm init` with pod CIDR `10.244.0.0/16` and service CIDR `10.96.0.0/12`
- Writes kubeconfig to `/root/.kube/config` and `$SUDO_USER`'s home
- Installs **Calico CNI** (`v3.27.3`)
- Waits for master Ready, then prints and saves the worker join command

### 2.9 Verifying the Cluster

```bash
kubectl get nodes -o wide
kubectl get pods -A
kubectl cluster-info
```

Expected output:
```
NAME       STATUS   ROLES           AGE   VERSION
master     Ready    control-plane   5m    v1.30.x
worker1    Ready    worker          3m    v1.30.x
worker2    Ready    worker          3m    v1.30.x
worker3    Ready    worker          3m    v1.30.x
worker4    Ready    worker          3m    v1.30.x
```

---

## 3. Storage Setup

### 3.1 Strategy

The platform uses **Rancher local-path-provisioner** (`v0.0.28`). It dynamically provisions `hostPath` volumes on whichever node the pod is scheduled to, using the path `/opt/local-path-provisioner` by default.

The `04-discover-storage.sh` script runs on each node to:
1. Find the filesystem with the most free space (excluding tmpfs/overlay/squashfs)
2. Create the provisioner directory there
3. Write the result to `/etc/k8s-storage-path`

### 3.2 SSH Key Setup for Storage Discovery (`star_master` → workers)

`04-discover-storage.sh --all-nodes` runs under `sudo bash`, which means `$SUDO_USER` is `star_master` and SSH must use `star_master`'s key. Unlike `deploy-workers.sh` (which runs as root), this script calls `sudo -u star_master ssh` — so the key must live in `~star_master/.ssh/`, **not** `/root/.ssh/`.

```bash
# Step 1 — Generate an SSH key for star_master (once only, run as star_master — no sudo).
# The key must land in /home/star_master/.ssh/, not /root/.ssh/.
ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519

# Step 2 — Copy the public key to every worker (run as star_master — no sudo).
# You will be prompted for each worker user's password once.
ssh-copy-id -i ~/.ssh/id_ed25519.pub star_worker1@192.168.1.51
ssh-copy-id -i ~/.ssh/id_ed25519.pub star_worker2@192.168.1.52
ssh-copy-id -i ~/.ssh/id_ed25519.pub star_worker3@192.168.1.53
ssh-copy-id -i ~/.ssh/id_ed25519.pub star_worker4@192.168.1.54

# Step 3 — Verify passwordless SSH works for every worker (run as star_master).
for ip in 192.168.1.51 192.168.1.52 192.168.1.53 192.168.1.54; do
  ssh -o BatchMode=yes -o ConnectTimeout=5 star_worker${ip##*.51}@${ip} true \
    && echo "${ip}: OK" || echo "${ip}: FAILED"
done
```

> **Why not `sudo ssh-keygen`?**
> `sudo bash 04-discover-storage.sh` sets `SSH_USER=$SUDO_USER` (i.e. `star_master`) and calls `sudo -u star_master ssh`. That process looks for keys in `/home/star_master/.ssh/`. If you generate the key with `sudo`, it lands in `/root/.ssh/` and the script will fail with `Permission denied (publickey)` for every worker.

> **Why not `su -c`?**
> The script previously used `su - star_master -c "ssh ..."`. On RHEL/Fedora, PAM requires `star_master`'s password even when called from root, so `su` exits with *Authentication failure* before SSH is ever attempted. The script now uses `sudo -u star_master ssh` which needs no password from root.

### 3.3 Run Order

```bash
# Step 1 — Run on master AND all workers in one command (from master).
# Prerequisites: SSH key setup from §3.2 must be complete first.
sudo bash scripts/storage/04-discover-storage.sh --all-nodes

# Override inventory file if needed:
# sudo bash scripts/storage/04-discover-storage.sh --all-nodes --inventory /path/to/workers.conf

# Step 2 — Install the provisioner on MASTER only.
sudo bash scripts/storage/05-install-local-path-provisioner.sh
```

> **Manual alternative (one node at a time):**
> ```bash
> # Run directly on each node
> sudo bash scripts/storage/04-discover-storage.sh
> ```

### 3.4 StorageClass

```yaml
# manifests/storage/storageclass.yaml
kind: StorageClass
metadata:
  name: local-path
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"
provisioner: rancher.io/local-path
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
```

`WaitForFirstConsumer` ensures volumes are only provisioned after a pod is scheduled, so the volume is created on the same node as the pod.

### 3.5 Testing Storage

```bash
# Apply test PVC
kubectl apply -f manifests/storage/storageclass.yaml

# Check PVC (will be Pending until a pod consumes it)
kubectl get pvc storage-test-pvc

# Deploy a test pod
kubectl run storage-test --image=busybox \
  --overrides='{"spec":{"volumes":[{"name":"v","persistentVolumeClaim":{"claimName":"storage-test-pvc"}}],"containers":[{"name":"c","image":"busybox","command":["sleep","3600"],"volumeMounts":[{"name":"v","mountPath":"/data"}]}]}}' \
  --restart=Never

kubectl exec storage-test -- df -h /data
kubectl delete pod storage-test
kubectl delete pvc storage-test-pvc
```

### 3.6 Node Path Customization

If a node has a dedicated data disk (e.g., `/data`), edit the `local-path-config` ConfigMap:

```bash
kubectl edit configmap local-path-config -n local-path-storage
```

Set per-node paths in `nodePathMap`:
```json
{
  "nodePathMap": [
    { "node": "worker1", "paths": ["/data/local-path"] },
    { "node": "DEFAULT_PATH_FOR_NON_LISTED_NODES", "paths": ["/opt/local-path-provisioner"] }
  ]
}
```

---

## 4. Private Docker Registry

### 4.1 Overview

A private OCI-compatible registry is deployed as a Kubernetes `Deployment` in the `registry` namespace, backed by a 50 Gi PVC, exposed via `NodePort 30500`. TLS is self-signed with a 10-year validity covering both `registry.local` (DNS) and `192.168.1.50` (IP SAN).

### 4.2 Deployment

```bash
# On MASTER
sudo bash scripts/registry/06-registry-setup.sh
```

This script:
1. Generates a self-signed certificate in `/etc/k8s-registry-certs/`
2. Creates the `registry` namespace and a `registry-tls` TLS secret
3. Applies `manifests/registry/` (PVC, Deployment, Service)
4. Configures containerd trust on master and all workers
5. Adds `registry.local` to `/etc/hosts` on all nodes
6. Smoke-tests the registry with `curl`

### 4.3 Manifest Summary

| File | Purpose |
|---|---|
| `registry-pvc.yaml` | 50 Gi PVC for image storage |
| `registry-deployment.yaml` | `registry:2` deployment with TLS, mounted certs |
| `registry-service.yaml` | NodePort 30500 → container 5000 |

### 4.4 Trusting the Certificate

The script auto-configures containerd on all nodes. For Docker-based clients on workstations:

```bash
# Copy cert from master
scp root@192.168.1.50:/etc/k8s-registry-certs/registry.crt .

# Linux (Ubuntu)
sudo cp registry.crt /usr/local/share/ca-certificates/registry.crt
sudo update-ca-certificates

# Add to Docker (alternative)
sudo mkdir -p /etc/docker/certs.d/192.168.1.50:30500
sudo cp registry.crt /etc/docker/certs.d/192.168.1.50:30500/ca.crt
sudo systemctl restart docker
```

### 4.5 Usage

```bash
# Tag and push from a workstation
docker tag myapp:1.0 192.168.1.50:30500/myapp:1.0
docker push 192.168.1.50:30500/myapp:1.0

# Or via registry.local (requires /etc/hosts entry on workstation)
docker tag myapp:1.0 registry.local:30500/myapp:1.0
docker push registry.local:30500/myapp:1.0

# List images in registry
curl -sk --cacert registry.crt \
  https://192.168.1.50:30500/v2/_catalog

# List tags for an image
curl -sk --cacert registry.crt \
  https://192.168.1.50:30500/v2/myapp/tags/list
```

### 4.6 Referencing Registry in Pod Specs

```yaml
spec:
  containers:
    - name: myapp
      image: 192.168.1.50:30500/myapp:1.0
```

No `imagePullSecrets` needed — containerd trusts the registry certificate cluster-wide.

---

## 5. Staging & Testing Directories

### 5.1 Purpose

| Area | Path | Kubernetes NS | Use |
|---|---|---|---|
| **Testing** | `/opt/k8s-builds/testing` | `testing` | Active dev; build and test images here |
| **Staging** | `/opt/k8s-builds/staging` | `staging` | Validated builds ready to push to registry |
| **Registry** | `192.168.1.50:30500` | `registry` | Final production-ready images |

### 5.2 Setup

```bash
sudo bash scripts/master/07-staging-testing-setup.sh
```

### 5.3 Directory Tree

```
/opt/k8s-builds/
├── testing/
│   ├── workspace/       ← git clone repos here
│   ├── images/          ← built .tar images
│   ├── manifests/       ← k8s YAML for test deployments
│   ├── logs/            ← build logs
│   └── promote-to-staging.sh
└── staging/
    ├── artifacts/       ← binaries, charts, etc.
    ├── images/          ← validated image .tar files
    ├── manifests/       ← k8s YAML for staging deployments
    ├── logs/            ← staging logs
    └── promote-to-registry.sh
```

### 5.4 Build → Staging → Registry Workflow

```bash
# 1. Build image in testing workspace
cd /opt/k8s-builds/testing/workspace/myapp
docker build -t myapp:1.0 .

# 2. Run tests
kubectl apply -f manifests/ -n testing
kubectl wait pod -l app=myapp -n testing --for=condition=Ready --timeout=60s
# ... run tests ...
kubectl delete -f manifests/ -n testing

# 3. Promote to staging
/opt/k8s-builds/testing/promote-to-staging.sh myapp 1.0

# 4. Validate in staging
kubectl apply -f /opt/k8s-builds/staging/manifests/ -n staging

# 5. Promote to private registry
/opt/k8s-builds/staging/promote-to-registry.sh myapp 1.0 \
  /opt/k8s-builds/staging/images/myapp-1.0.tar
```

---

## 6. ArgoCD – GitOps Toolchain

### 6.1 Overview

ArgoCD monitors a Git repository and continuously reconciles the cluster state with the desired state declared in Git. Any push to the configured branch triggers a sync.

### 6.2 Installation

```bash
# Install Helm first
sudo bash scripts/master/08-install-helm.sh

# Deploy ArgoCD
sudo bash scripts/master/09-deploy-argocd.sh
```

### 6.3 Access

| Method | URL |
|---|---|
| Web UI | `https://192.168.1.50:30443` |
| CLI | `argocd login 192.168.1.50:30443 --username admin` |

**Retrieve initial password:**
```bash
kubectl -n argocd get secret argocd-initial-admin-secret \
  -o jsonpath='{.data.password}' | base64 -d; echo
```

**Change password:**
```bash
argocd account update-password \
  --current-password <initial> \
  --new-password <new>
```

### 6.4 Registering Your Git Repository

```bash
argocd repo add https://github.com/stardatadblabs/k8s-platform.git \
  --username git \
  --password <token>
```

For SSH:
```bash
argocd repo add git@github.com:stardatadblabs/k8s-platform.git \
  --ssh-private-key-path ~/.ssh/id_ed25519
```

### 6.5 Deploying Applications via ArgoCD CLI

```bash
# Apply the platform AppProject first
kubectl apply -f argocd-apps/app-project-platform.yaml

# Apply all Application manifests
kubectl apply -f argocd-apps/

# Check sync status
argocd app list
argocd app get private-registry
argocd app sync private-registry
```

### 6.6 Helm Values for ArgoCD

Key settings in [`helm/argocd/values.yaml`](../helm/argocd/values.yaml):

| Key | Value | Reason |
|---|---|---|
| `server.extraArgs` | `--insecure` | No ingress TLS in this setup |
| `server.service.type` | NodePort | Direct access without ingress |
| `server.service.nodePortHttps` | 30443 | Fixed port |
| `dex.enabled` | false | SSO disabled for simplicity |
| `notifications.enabled` | true | Enables Slack/webhook notifications |

### 6.7 ArgoCD Application Sync Policies

All platform apps use:
- `automated.prune: true` — removes resources deleted from Git
- `automated.selfHeal: true` — reverts manual cluster changes
- `CreateNamespace=true` — auto-creates namespace if missing

> **Exception:** `app-storage.yaml` uses `prune: false` to prevent accidental deletion of the StorageClass.

---

## 7. Helm – Application Packaging

### 7.1 Installation

```bash
sudo bash scripts/master/08-install-helm.sh
```

Installs Helm v3.15.2 and adds repos:
- `argo` → `https://argoproj.github.io/argo-helm`
- `openbao` → `https://openbao.github.io/openbao-helm`

### 7.2 Common Helm Commands

```bash
# List installed releases
helm list -A

# Check for upgrades
helm outdated -A 2>/dev/null || helm list -A

# Upgrade ArgoCD to a new chart version
helm upgrade argocd argo/argo-cd \
  -n argocd \
  --version 6.12.0 \
  -f helm/argocd/values.yaml

# Upgrade OpenBao
helm upgrade openbao openbao/openbao \
  -n openbao \
  -f helm/openbao/values.yaml

# Render templates without installing (for debugging)
helm template argocd argo/argo-cd \
  -n argocd \
  -f helm/argocd/values.yaml | less

# Show default values for a chart
helm show values openbao/openbao
```

### 7.3 Packaging Your Own Applications

```bash
# Create a new chart skeleton
helm create myapp

# Lint the chart
helm lint myapp/

# Package it
helm package myapp/

# Push to OCI registry (requires Helm ≥ 3.8)
helm push myapp-0.1.0.tgz oci://192.168.1.50:30500/charts

# Install from OCI registry
helm install myapp \
  oci://192.168.1.50:30500/charts/myapp \
  --version 0.1.0 \
  -n prod
```

---

## 8. OpenBao – Secret Manager

### 8.1 Overview

OpenBao is the community-maintained open-source fork of HashiCorp Vault (BSL relicense split). It provides:
- KV v2 secrets engine
- Dynamic credentials (database, cloud)
- Kubernetes authentication (pods authenticate via ServiceAccount JWT)
- Agent sidecar injector

### 8.2 Installation

#### Prerequisites

Before running the deploy script, ensure:

1. **Worker internet is working** — pods may schedule on any worker; all workers must be able
   to pull images from `quay.io` and `docker.io`. See [§14.5](#145-worker-nodes-cannot-pull-images--reach-internet)
   and [§14.12](#1412-openbao-deployment-failures) if workers have no internet.

2. **Pre-pull images on master** — the `values.yaml` pins OpenBao to `master.local` via
   `nodeSelector`, but the agent-injector pod schedules on workers. Pre-pull on master to
   guarantee a fast start:
   ```bash
   sudo crictl pull quay.io/openbao/openbao:2.6.0
   sudo crictl pull docker.io/hashicorp/vault-k8s:1.7.2
   ```

3. **No leftover release** — if a previous failed install exists, clean it up first:
   ```bash
   helm uninstall openbao -n prod 2>/dev/null || true
   # If it was accidentally installed in the openbao namespace, clean that too
   helm uninstall openbao -n openbao 2>/dev/null || true
   kubectl delete namespace openbao --ignore-not-found
   # Confirm port 30820 is free
   kubectl get svc --all-namespaces | grep 30820
   ```

#### Deploy

```bash
cd ~/k8s-platform/scripts/master
sudo bash 10-deploy-openbao.sh
```

The script:
1. Adds the `openbao` Helm repo and runs `helm repo update`
2. Creates the `prod` namespace (if not already present)
3. Deploys via Helm with `helm/openbao/values.yaml`
4. Waits for `openbao-0` pod to reach `Ready`
5. Initializes with 5 key shares / threshold 3
6. Saves unseal keys to `/root/openbao-init-keys.json` (chmod 600)
7. Unseals automatically using 3 of the 5 keys
8. Enables KV v2 secrets engine at `secret/`
9. Enables and configures Kubernetes auth method

#### Deployed resources (namespace: `prod`)

| Resource | Type | Details |
|---|---|---|
| `openbao-0` | StatefulSet pod | Server pinned to `master.local` |
| `openbao-agent-injector-*` | Deployment pod | Sidecar injector, schedules on workers |
| `service/openbao` | NodePort | `8200:30820` — UI/API |
| `service/openbao-agent-injector-svc` | ClusterIP | Webhook, port 443 |
| `service/openbao-internal` | Headless | Cluster-internal, ports 8200/8201 |
| PVC `data-openbao-0` | 10 Gi | `local-path` StorageClass |
| PVC `audit-openbao-0` | 5 Gi | `local-path` StorageClass |

#### Credentials (from this deployment)

| | |
|---|---|
| **UI** | `http://192.168.1.50:30820/ui` |
| **Root Token** | `s.5zxPMHtZ25EfkiwULIgWjPRQ` |
| **Keys file** | `/root/openbao-init-keys.json` |

> ⚠️ Back up `/root/openbao-init-keys.json` to a secure offline location and
> `shred -u` it from the server immediately. See [§8.7](#87-unseal-keys-management).

### 8.3 Access

| Method | URL |
|---|---|
| Web UI | `http://192.168.1.50:30820/ui` |
| CLI | `bao login -address=http://192.168.1.50:30820` |

```bash
# Set address for CLI use
export BAO_ADDR="http://192.168.1.50:30820"

# Login with root token
bao login <root-token>

# Or via Kubernetes auth from within a pod
bao login -method=kubernetes role=my-role
```

### 8.4 Writing and Reading Secrets

```bash
# Write a secret
bao kv put secret/myapp/config \
  db_password="s3cr3t" \
  api_key="abcdef123"

# Read a secret
bao kv get secret/myapp/config

# Read a specific field
bao kv get -field=db_password secret/myapp/config
```

### 8.5 Kubernetes Authentication Setup

After initial deployment, configure a role for your application's ServiceAccount:

```bash
export BAO_ADDR="http://192.168.1.50:30820"
export BAO_TOKEN="<root-token>"

# Create a policy
bao policy write myapp-policy - <<'EOF'
path "secret/data/myapp/*" {
  capabilities = ["read", "list"]
}
EOF

# Create a role mapping namespace + serviceaccount → policy
bao write auth/kubernetes/role/myapp \
  bound_service_account_names=myapp-sa \
  bound_service_account_namespaces=prod,staging \
  policies=myapp-policy \
  ttl=1h
```

### 8.6 Injecting Secrets via Sidecar

Annotate your pod to have the OpenBao agent inject secrets automatically:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: myapp
  namespace: prod
  annotations:
    bao-agent-inject: "true"
    bao-agent-inject-secret-config: "secret/data/myapp/config"
    bao-agent-inject-template-config: |
      {{- with secret "secret/data/myapp/config" -}}
      export DB_PASSWORD="{{ .Data.data.db_password }}"
      export API_KEY="{{ .Data.data.api_key }}"
      {{- end }}
    bao-role: "myapp"
spec:
  serviceAccountName: myapp-sa
  containers:
    - name: myapp
      image: 192.168.1.50:30500/myapp:1.0
```

The injected file appears at `/bao/secrets/config` inside the container.

### 8.7 Unseal Keys Management

**Critical:** The unseal keys in `/root/openbao-init-keys.json` must be:
1. Copied to a secure offline location immediately
2. Split among trusted team members (Shamir's Secret Sharing)
3. **Deleted from the server** after backup

```bash
# Back up keys (example: copy to secure workstation)
scp root@192.168.1.50:/root/openbao-init-keys.json ./openbao-keys-BACKUP.json

# After secure backup, delete from server
ssh root@192.168.1.50 "shred -u /root/openbao-init-keys.json"
```

---

## 9. Namespace Quotas

### 9.1 Quota Design

The `prod` namespace is given generous limits suited to full production workloads. The `test` namespace is set to exactly 1/5 of each `prod` limit to prevent test workloads from consuming cluster resources.

### 9.2 Quota Table

| Resource | prod | test |
|---|---|---|
| `requests.cpu` | 20 | 4 |
| `limits.cpu` | 40 | 8 |
| `requests.memory` | 40 Gi | 8 Gi |
| `limits.memory` | 80 Gi | 16 Gi |
| `requests.storage` | 500 Gi | 100 Gi |
| `persistentvolumeclaims` | 20 | 4 |
| `pods` | 100 | 20 |
| `services` | 50 | 10 |
| `configmaps` | 100 | 20 |
| `secrets` | 100 | 20 |

### 9.3 LimitRange Defaults

When a container does not specify resource requests/limits, the LimitRange injects defaults:

| | prod default | test default |
|---|---|---|
| CPU request | 100m | 50m |
| CPU limit | 500m | 200m |
| Memory request | 128 Mi | 64 Mi |
| Memory limit | 512 Mi | 256 Mi |

### 9.4 Applying Quotas

```bash
sudo bash scripts/master/11-namespaces-quotas.sh
```

Or apply directly:
```bash
kubectl apply -f manifests/namespaces/prod-namespace.yaml
kubectl apply -f manifests/namespaces/test-namespace.yaml
```

### 9.5 Checking Quota Usage

```bash
# Summary across all namespaces
kubectl get resourcequota -A

# Detailed usage for prod
kubectl describe resourcequota prod-quota -n prod

# LimitRange details
kubectl describe limitrange prod-limits -n prod
```

---

## 10. ArgoCD Application Manifests

### 10.1 Summary

| File | App Name | Manages |
|---|---|---|
| `app-project-platform.yaml` | AppProject: `platform` | Project permissions |
| `app-registry.yaml` | `private-registry` | `manifests/registry/` |
| `app-storage.yaml` | `storage` | `manifests/storage/` |
| `app-namespaces.yaml` | `namespaces` | `manifests/namespaces/` |
| `app-openbao.yaml` | `openbao` | OpenBao Helm chart |

### 10.2 Updating the Git Repo URL

All app manifests contain:
```yaml
repoURL: https://github.com/stardatadblabs/k8s-platform.git
```

The repository is already set to `stardatadblabs`. No substitution needed.

```bash
# Bulk replace
find argocd-apps/ -name "*.yaml" -exec \
  sed -i 's/stardatadblabs/your-fork-org/g' {} \;
```

### 10.3 Applying All ArgoCD Apps

```bash
# Register the project first
kubectl apply -f argocd-apps/app-project-platform.yaml

# Apply all apps
kubectl apply -f argocd-apps/

# Monitor sync
watch kubectl get applications -n argocd
```

---

## 11. Runbook – Day 2 Operations

### Upgrading Kubernetes

```bash
# On master — upgrade kubeadm first
apt-get update && apt-get install -y kubeadm=1.31.x-*
kubeadm upgrade plan
kubeadm upgrade apply v1.31.x

# Drain master
kubectl drain master --ignore-daemonsets --delete-emptydir-data
apt-get install -y kubelet=1.31.x-* kubectl=1.31.x-*
systemctl restart kubelet
kubectl uncordon master

# Repeat drain/upgrade/uncordon for each worker
```

### Adding a New Worker

**Option A — From the master (recommended):**
```bash
# Add the new worker IP to the deploy-workers.sh WORKERS array, then:
sudo bash scripts/master/deploy-workers.sh 192.168.1.55

# On the new worker — run storage discovery
ssh root@192.168.1.55 "bash /tmp/k8s-scripts/storage/04-discover-storage.sh"
# OR log into the worker and run it directly:
# sudo bash scripts/storage/04-discover-storage.sh
```

**Option B — Log into the worker directly:**
```bash
# On the new worker node
sudo bash scripts/workers/00-common-prereqs.sh
sudo bash scripts/workers/02-worker-join.sh 192.168.1.50
sudo bash scripts/storage/04-discover-storage.sh
```

```bash
# On master — label and verify
kubectl label node <new-node> node-role.kubernetes.io/worker=worker
kubectl get nodes
```

### Rotating the Registry TLS Certificate

```bash
# Regenerate cert on master
openssl req -x509 -newkey rsa:4096 -nodes -days 3650 \
  -keyout /etc/k8s-registry-certs/registry.key \
  -out    /etc/k8s-registry-certs/registry.crt \
  -subj   "/CN=registry.local/O=K8sRegistry" \
  -addext "subjectAltName=DNS:registry.local,IP:192.168.1.50"

# Update the Kubernetes TLS secret
kubectl create secret tls registry-tls \
  --cert=/etc/k8s-registry-certs/registry.crt \
  --key=/etc/k8s-registry-certs/registry.key \
  -n registry --dry-run=client -o yaml | kubectl apply -f -

# Restart registry pod
kubectl rollout restart deployment/private-registry -n registry

# Re-run trust configuration on all nodes
bash scripts/registry/06-registry-setup.sh
```

### Unsealing OpenBao After a Restart

OpenBao starts sealed after every pod restart. You must unseal it with 3 of 5 keys:

```bash
export BAO_ADDR="http://192.168.1.50:30820"
BAO_POD=$(kubectl get pod -n openbao -l app.kubernetes.io/name=openbao \
  -o jsonpath='{.items[0].metadata.name}')

# Unseal (run 3 times with different keys from the backup file)
kubectl exec -n openbao "${BAO_POD}" -- bao operator unseal <key1>
kubectl exec -n openbao "${BAO_POD}" -- bao operator unseal <key2>
kubectl exec -n openbao "${BAO_POD}" -- bao operator unseal <key3>

# Verify
kubectl exec -n openbao "${BAO_POD}" -- bao status
```

### Resetting the Cluster (Nuclear Option)

```bash
# On all workers first
kubeadm reset -f
iptables -F && iptables -t nat -F && iptables -t mangle -F
ipvsadm --clear 2>/dev/null || true
rm -rf /etc/cni/net.d /var/lib/etcd

# Then on master
kubeadm reset -f
rm -rf /root/.kube $HOME/.kube /etc/kubernetes
```

---

## 13. Troubleshooting

### 13.1 SSH Connectivity Issues

#### `kex_exchange_identification: read: Connection reset by peer`

The TCP connection reached the worker but `sshd` dropped it immediately. Not a firewall block.

```bash
# 1. Check verbose SSH output
sudo ssh -v star_worker1@192.168.1.51 2>&1 | head -20

# 2. Check if sshd is running — use another worker as a jump host
sudo ssh -t star_worker2@192.168.1.52 "ssh star_worker1@192.168.1.51 sudo systemctl status sshd"

# 3. Check sshd logs via the jump host
sudo ssh -t star_worker2@192.168.1.52 "ssh star_worker1@192.168.1.51 sudo journalctl -u sshd -n 30 --no-pager"

# 4. Restart sshd via the jump host
sudo ssh -t star_worker2@192.168.1.52 "ssh star_worker1@192.168.1.51 sudo systemctl restart sshd"

# 5. Re-verify after restart
sudo ssh -n star_worker1@192.168.1.51 sudo whoami && echo "OK"
```

#### `sudo: a terminal is required to read the password` in a loop

Caused by `ssh` reading from the pipe's stdin instead of your terminal. Use `<&3` to keep stdin as the terminal:

```bash
# WRONG — ssh steals the pipe's stdin, loop stops after first node
grep ... workers.conf | while read -r ip user; do
  sudo ssh -t "${user}@${ip}" "sudo somecommand"
done

# CORRECT — worker list read from fd 3; stdin stays as your terminal
while read -r ip user <&3; do
  sudo ssh -t "${user}@${ip}" "sudo somecommand"
done 3< <(grep -v '^\s*#' scripts/workers.conf | grep -v '^\s*$')
```

#### Plain `ssh star_worker1@192.168.1.51` prompts for a password

This is **expected and not an error**. `deploy-workers.sh` runs via `sudo` and uses root's key in `/root/.ssh/`. Plain `ssh` as `star_master` looks in `~star_master/.ssh/` where no key was installed. Always use `sudo ssh` to test connectivity.

---


### 13.2 `04-discover-storage.sh --all-nodes` Skips All Workers

All workers log `SKIP <ip>: SSH unreachable` even though the nodes are online.

**Root cause A — `su -c` Authentication failure (RHEL/Fedora)**

The script used `su - star_master -c "ssh ..."` to run SSH as the non-root user. On RHEL/Fedora, PAM requires `star_master`'s password even when `su` is called from root, so `su` exits with *Authentication failure* before SSH is ever attempted.

Fixed in the current script: all remote calls now use `sudo -u star_master ssh` which needs no password from root.

**Root cause B — No SSH key in `~star_master/.ssh/`**

`sudo bash 04-discover-storage.sh` sets `SSH_USER=$SUDO_USER` (`star_master`) and calls `sudo -u star_master ssh`. That process looks for keys in `/home/star_master/.ssh/`. If no key exists there (or if the key was generated with `sudo` and landed in `/root/.ssh/`), every worker fails with `Permission denied (publickey)`.

**Diagnose:**
```bash
# Check what keys star_master has
ls -la /home/star_master/.ssh/
# Must contain id_ed25519 and id_ed25519.pub

# Test SSH directly as star_master (no sudo)
sudo -u star_master ssh -o BatchMode=yes -o ConnectTimeout=5 star_worker1@192.168.1.51 true \
  && echo "OK" || echo "FAILED"
```

**Fix — generate and distribute the key as `star_master` (no sudo):**
```bash
# Run as star_master in a normal (non-sudo) terminal
ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519

ssh-copy-id -i ~/.ssh/id_ed25519.pub star_worker1@192.168.1.51
ssh-copy-id -i ~/.ssh/id_ed25519.pub star_worker2@192.168.1.52
ssh-copy-id -i ~/.ssh/id_ed25519.pub star_worker3@192.168.1.53
ssh-copy-id -i ~/.ssh/id_ed25519.pub star_worker4@192.168.1.54
```

> ⚠️ Do **not** use `sudo ssh-copy-id` — that writes to `/root/.ssh/` instead of `/home/star_master/.ssh/` and the script will still fail.

**Root cause C — SSH stdin consuming the inventory file**

If workers are processed but the loop stops after the first one (workers 52–54 silently missing), it means the `ssh` command inside the loop is reading from the same stdin as the `while read` loop, consuming the remaining lines of `workers.conf`.

Fixed in the current script: the inventory file is opened on file descriptor 3 (`done 3< "${INVENTORY_FILE}"`) and `read` pulls from `<&3`, so SSH can never consume it.

---


### 13.3 RHEL Subscription / Repository Issues

#### `Failed to download metadata for repo: Cannot download repomd.xml`

The worker cannot reach the Red Hat CDN. Usually caused by missing or inactive subscription.

**Step 1 — Check subscription status on all workers:**
```bash
while read -r ip user <&3; do
  echo "--- ${ip} ---"
  sudo ssh -n "${user}@${ip}" "sudo subscription-manager status"
done 3< <(grep -v '^\s*#' scripts/workers.conf | grep -v '^\s*$')
```

**Step 2 — Register workers that are not registered:**
```bash
# Read password silently — never type it in plain text
read -rs RH_PASS

while read -r ip user <&3; do
  echo "Registering ${ip}..."
  sudo ssh -t "${user}@${ip}" \
    "sudo subscription-manager register --username <rh-username> --password '${RH_PASS}'"
done 3< <(grep -v '^\s*#' scripts/workers.conf | grep -v '^\s*$')
```

> **Note:** `--auto-attach` was removed in RHEL 10. Registration alone enables repos for Developer subscriptions.

**Step 3 — Verify repos are available:**
```bash
# This may take 30–60 seconds per node on first run after registration
# while Red Hat CDN metadata is refreshed — this is normal, just wait.
while read -r ip user <&3; do
  echo "--- ${ip} ---"
  sudo ssh -n "${user}@${ip}" "sudo dnf repolist"
done 3< <(grep -v '^\s*#' scripts/workers.conf | grep -v '^\s*$')
```

Expected output per worker:
```
--- 192.168.1.51 ---
Updating Subscription Management repositories.
repo id                                    repo name
rhel-10-for-x86_64-baseos-rpms            Red Hat Enterprise Linux 10 for x86_64 - BaseOS
rhel-10-for-x86_64-appstream-rpms         Red Hat Enterprise Linux 10 for x86_64 - AppStream
```

**Step 4 — If repos are still empty, force a refresh:**
```bash
while read -r ip user <&3; do
  sudo ssh -n "${user}@${ip}" "sudo subscription-manager refresh && sudo dnf repolist"
done 3< <(grep -v '^\s*#' scripts/workers.conf | grep -v '^\s*$')
```

**Step 5 — If the node is a VM clone with a duplicate identity:**
```bash
while read -r ip user <&3; do
  echo "Re-registering ${ip}..."
  sudo ssh -t "${user}@${ip}" \
    "sudo subscription-manager clean && \
     sudo subscription-manager register --username <rh-username> --password '${RH_PASS}'"
done 3< <(grep -v '^\s*#' scripts/workers.conf | grep -v '^\s*$')
```

Once all workers show repos in `dnf repolist`, re-run the deploy script:
```bash
sudo bash scripts/master/deploy-workers.sh
```

---

## 12. Security Considerations

### Network Policies

No NetworkPolicies are defined in this setup. In production, add:

```yaml
# Deny all ingress to prod namespace by default
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-ingress
  namespace: prod
spec:
  podSelector: {}
  policyTypes: [Ingress]
```

### RBAC

- ArgoCD is configured with `role:readonly` as default; admins get full access
- OpenBao policies should follow least-privilege: grant only the paths each app needs
- Create per-team ServiceAccounts in each namespace rather than sharing

### Secret Hygiene

| ❌ Don't | ✅ Do |
|---|---|
| Store secrets in Git (even encrypted without proper tooling) | Use OpenBao KV + sidecar injector |
| Use the OpenBao root token for applications | Create scoped policies and roles |
| Leave `/root/openbao-init-keys.json` on the server | Back up offline and `shred` the file |
| Use `--insecure` ArgoCD in production | Add a proper ingress with cert-manager |

### TLS

- Registry: self-signed 10-year cert (acceptable for internal lab; use a proper CA in production)
- ArgoCD: currently `--insecure` (terminates TLS at NodePort); add an Ingress + cert-manager for production
- OpenBao: TLS disabled (`tlsDisable: true`) in this config; enable for production with proper certs

### Container Runtime Security

- `SystemdCgroup = true` ensures proper cgroup v2 integration
- containerd is configured to trust the internal registry via `certs.d` host configuration (not `insecure-registries`)


---

## 14. RHEL 10 / kube-proxy / Calico Networking — Field Notes

> These issues were encountered on **RHEL 10.2 + Kubernetes 1.30 + Calico v3.27 + OpenBao 2.6.0** during initial platform deployment. Each section describes the symptom, root cause, and fix so it can be reproduced reliably on future installs.

**Sections:**
14.1 ArgoCD / NodePort Services Unreachable · 14.2 `nft flush ruleset` Destroys Networking ·
14.3 Calico Wrong IP · 14.4 Pod-to-ClusterIP Blocked · 14.5 Workers No Internet ·
14.6 `rp_filter` Breaks ipvs · 14.7 Helm Not Found Under sudo · 14.8 ArgoCD Connection Refused ·
14.9 Registry Stuck Pending · 14.10 ArgoCD Pre-upgrade Hook Timeout · 14.11 Health Check ·
14.12 OpenBao Deployment Failures

---

### 14.1 ArgoCD / NodePort Services Unreachable (`connection timed out`)

**Symptom:** `https://192.168.1.50:30443` times out in browser. `curl -sk https://192.168.1.50:30443` never responds.

**Root cause:** RHEL 10 ships `nf_tables` as the native kernel filter. The iptables compatibility modules (`iptable_filter`, `iptable_nat`) exist on disk but are **not loaded by default**. Without them, kube-proxy (iptables mode) and Calico Felix both fail:
- kube-proxy crashes: `No iptables support for family IPv4`
- Calico Felix panics: `iptables-save failed: incompatible nft rules`

> ⚠️ **Do NOT switch kube-proxy to `nftables` mode** — it is alpha in K8s 1.30 and conflicts with Calico Felix which uses `iptables-nft` to write its own `cali-*` chains. Running both kube-proxy nftables AND Calico simultaneously causes Calico to continuously panic.
> ⚠️ **Do NOT switch to `ipvs` mode** — ipvs mode also requires iptables for some rules and crashes the same way.

**The correct fix is: load the missing iptables modules.**

**Diagnosis:**
```bash
# Check which modules are loaded
lsmod | grep -E "iptable_filter|iptable_nat|ip_tables"
# If iptable_filter and iptable_nat are missing, this is the problem

# Check kube-proxy error
kubectl logs -n kube-system -l k8s-app=kube-proxy | grep "No iptables"
```

**Fix — run the dedicated script:**
```bash
sudo bash scripts/master/fix-iptables-modules.sh
```

The script loads the modules, persists them to `/etc/modules-load.d/iptables.conf`, restarts kube-proxy and calico-node on master, auto-unseals OpenBao if needed, and verifies all three paths (pod IP, ClusterIP, NodePort).

**Or apply manually:**
```bash
sudo modprobe ip_tables iptable_filter iptable_nat iptable_mangle nf_nat nf_conntrack

# Persist across reboots
echo -e "ip_tables\niptable_filter\niptable_nat\niptable_mangle\nnf_nat\nnf_conntrack" | \
  sudo tee /etc/modules-load.d/iptables.conf

kubectl -n kube-system rollout restart daemonset/kube-proxy
CALICO=$(kubectl get pod -n kube-system -l k8s-app=calico-node -o wide --no-headers | grep master.local | awk '{print $1}')
kubectl delete pod ${CALICO} -n kube-system
```

**Verify:**
```bash
kubectl -n kube-system get pods -l k8s-app=kube-proxy -o wide
kubectl -n kube-system get pods -l k8s-app=calico-node -o wide
curl -s http://192.168.1.50:30820/v1/sys/health | python3 -m json.tool
```

---

### 14.2 `nft flush ruleset` Breaks Cluster Networking

**Symptom:** After running `sudo nft flush ruleset` (attempted as a fix), all pod-to-pod and pod-to-ClusterIP traffic stops working. Pods show `no route to host` or `Host is unreachable`. CoreDNS unreachable from worker pods. ArgoCD pods crash with `dial tcp 10.96.0.1:443: connect: no route to host`.

**Root cause:** `nft flush ruleset` wipes **all** nftables rules including Calico's pod routing rules, kube-proxy's service rules, and firewalld's rules. Calico and kube-proxy do not automatically re-apply all rules on the next sync cycle — a full restart is required.

> ⚠️ **Never run `nft flush ruleset` on a running Kubernetes node.** It is destructive and difficult to recover from without reboots.

**Recovery procedure:**
```bash
# Step 1 — Reboot all worker nodes (cleanest recovery)
for ip in 192.168.1.51 192.168.1.52 192.168.1.53 192.168.1.54; do
  idx=$((${ip##*.} - 50))
  ssh star_worker${idx}@${ip} "sudo reboot" || true
done

# Step 2 — Wait for workers to come back
sleep 90
kubectl get nodes

# Step 3 — Restart kube-proxy to repopulate ipvs/iptables rules
kubectl -n kube-system rollout restart daemonset/kube-proxy
sleep 15

# Step 4 — Restart Calico to repopulate routing rules
kubectl -n kube-system rollout restart daemonset/calico-node
kubectl rollout status daemonset/calico-node -n kube-system --timeout=120s

# Step 5 — Re-add firewall rules for pod/service CIDRs (see §14.4)
# Step 6 — Re-add masquerade for worker internet (see §14.5)
```

---

### 14.3 Calico Picks Up Wrong IP (WiFi Interface Instead of `eno1`)

**Symptom:** Pod-to-pod traffic across nodes fails. `ping` from master to worker pod IPs shows 100% loss. Calico BGP peers show wrong IP (`192.168.1.132` instead of `192.168.1.50` for master). Worker nodes route pod traffic to the wrong gateway.

**Diagnosis:**
```bash
# Check what IP Calico has registered for each node
kubectl get nodes -o custom-columns=\
'NAME:.metadata.name,CALICO-IP:.metadata.annotations.projectcalico\.org/IPv4Address'

# Check Calico logs for which interface it detected
kubectl logs -n kube-system -l k8s-app=calico-node -c calico-node --tail=20 \
  | grep "autodetect\|IPv4"

# Check BGP peers on a worker
kubectl exec -n kube-system <calico-node-pod> -- birdcl show protocols
```

**Root cause:** Calico's IP autodetection defaults to `first-found` which picks the first non-loopback interface. On master, `wlp2s0` (WiFi, `192.168.1.132`) was detected before `eno1` (`192.168.1.50`). This causes all workers to BGP-peer with the wrong IP and route pod traffic to the wrong destination.

**Fix — pin Calico to the correct interface:**
```bash
# Patch calico-node daemonset to use eno1 for IP autodetection
kubectl patch daemonset calico-node -n kube-system --type=json -p='[
  {
    "op": "add",
    "path": "/spec/template/spec/containers/0/env/-",
    "value": {
      "name": "IP_AUTODETECTION_METHOD",
      "value": "interface=eno1"
    }
  }
]'

# Restart to apply
kubectl rollout restart daemonset/calico-node -n kube-system
kubectl rollout status daemonset/calico-node -n kube-system --timeout=120s

# Verify correct IP is now registered
kubectl get node master.local \
  -o jsonpath='{.metadata.annotations.projectcalico\.org/IPv4Address}'; echo
# Expected: 192.168.1.50/24

# Verify worker BGP peers with correct master IP
kubectl exec -n kube-system <calico-node-on-worker> -- birdcl show protocols | grep Mesh
# Expected: Mesh_192_168_1_50 ... Established
```

> This fix should be applied **before** the cluster is used in production. Add `IP_AUTODETECTION_METHOD: interface=eno1` to the Calico manifest at install time if the node has multiple network interfaces.

---

### 14.4 Pod-to-ClusterIP Traffic Blocked by Firewall (firewalld)

**Symptom (workers):** Pods on workers cannot reach Kubernetes ClusterIPs (`10.96.0.1`, `10.96.0.10`). DNS resolution fails inside pods. ArgoCD components crash with `dial tcp 10.96.0.1:443: connect: no route to host`.

**Symptom (master itself):** `curl http://192.168.1.50:<NodePort>` times out with exit code 28. Direct pod IP (`10.244.233.x`) and ClusterIP (`10.96.x.x` / `10.100.x.x`) also time out. But `kubectl exec <pod> -- wget http://127.0.0.1:<port>` works fine — meaning the pod is healthy, firewalld is the barrier.

**Root cause:** firewalld does not trust pod CIDR (`10.244.0.0/16`) or service CIDR (`10.96.0.0/12`). This affects:
- Worker pods reaching master services (CoreDNS, API server)
- The master itself reaching its own NodePorts and pod IPs via kube-proxy DNAT

**Diagnosis:**
```bash
sudo firewall-cmd --get-active-zones
sudo firewall-cmd --zone=trusted --list-sources
# If 10.244.0.0/16 and 10.96.0.0/12 are not listed, this is the problem

# Quick test from master — if all three timeout, firewalld is blocking:
curl -sf --max-time 3 http://192.168.1.50:30820/v1/sys/health  # NodePort
curl -sf --max-time 3 http://$(kubectl get svc openbao -n prod -o jsonpath='{.spec.clusterIP}'):8200/v1/sys/health  # ClusterIP
curl -sf --max-time 3 http://$(kubectl get pod openbao-0 -n prod -o jsonpath='{.status.podIP}'):8200/v1/sys/health  # Pod IP
```

**Fix — use the dedicated script (covers master firewall + rp_filter together):**
```bash
sudo bash scripts/master/fix-master-firewall.sh
```

**Or apply manually:**
```bash
# Master — trust pod and service CIDRs
sudo firewall-cmd --permanent --add-source=10.244.0.0/16 --zone=trusted
sudo firewall-cmd --permanent --add-source=10.96.0.0/12 --zone=trusted
sudo firewall-cmd --reload
sudo firewall-cmd --zone=trusted --list-sources
# Expected: 10.244.0.0/16 10.96.0.0/12
```

**Apply the same fix on all workers** (pod-to-pod traffic also needs this):
```bash
for ip in 192.168.1.51 192.168.1.52 192.168.1.53 192.168.1.54; do
  idx=$((${ip##*.} - 50))
  user="star_worker${idx}"
  ssh -o BatchMode=yes ${user}@${ip} "
    sudo firewall-cmd --permanent --add-source=10.244.0.0/16 --zone=trusted
    sudo firewall-cmd --permanent --add-source=10.96.0.0/12 --zone=trusted
    sudo firewall-cmd --reload
    echo done on \$(hostname)
  "
done
```

> This is a **required post-install step** on RHEL 10 with firewalld. It should be added to `00-common-prereqs.sh` for future cluster builds.

---

### 14.5 Worker Nodes Cannot Pull Images / Reach Internet

**Symptom:** Worker pods stuck in `ImagePullBackOff` or `ErrImagePull`. DNS resolution of
external hostnames (`quay.io`, `docker.io`) times out. Workers can ping master and reach
internal cluster services but not the public internet.

**Root cause:** Workers use master (`192.168.1.50`) as their default gateway. Master has two
interfaces — `eno1` (LAN, `192.168.1.50`) and `wlp2s0` (WiFi, `192.168.1.132`). Worker
traffic arriving on `eno1` needs NAT masquerade and cross-zone forwarding to leave via
`wlp2s0`. On RHEL 10, **firewalld owns the nft tables** — raw `nft add rule` commands work
briefly but are silently wiped on the next `firewall-cmd --reload`. All masquerade config
must go through firewalld exclusively.

**Key lesson:** Do NOT use raw `nft add rule` for masquerade on systems running firewalld.
firewalld regenerates its nft tables on every reload and discards any externally added rules.

**Diagnosis:**
```bash
# 1. On a worker — basic reachability
ping -c2 8.8.8.8                          # should succeed if routing is OK
curl -o /dev/null -sw '%{http_code}' https://quay.io/v2/   # 401 = OK, 000 = no internet

# 2. On master — check which zone each interface is in
sudo firewall-cmd --get-active-zones
# eno1  should be in: internal
# wlp2s0 should be in: public (or external)

# 3. On master — check masquerade is on the zone that wlp2s0 is in
sudo firewall-cmd --zone=public --query-masquerade

# 4. On master — check forwarding policies exist
sudo firewall-cmd --get-policies | grep -E "InternalToExternal|ExternalToInternal"

# 5. On master — verify ip_forward is enabled
cat /proc/sys/net/ipv4/ip_forward    # must be 1
```

**Fix — run the dedicated script (idempotent, safe to re-run):**
```bash
sudo bash ~/k8s-platform/scripts/master/fix-masquerade.sh
```

The script (`scripts/master/fix-masquerade.sh`) performs these steps using firewalld exclusively:

1. Enables `net.ipv4.ip_forward` and persists it to `/etc/sysctl.d/99-ip-forward.conf`
2. Detects which firewalld zone `wlp2s0` is currently in (NM controls this — may be
   `public` or `external`; script adapts automatically)
3. Enables `masquerade` on that zone permanently (`--permanent`) **and** at runtime immediately
4. Deletes any stale forwarding policies and recreates them targeting the correct detected zone:
   - `InternalToExternal` (target=ACCEPT): `internal` → detected ext zone
   - `ExternalToInternal` (target=ACCEPT): detected ext zone → `internal`
5. Ensures `eno1` is in the `internal` zone
6. Ensures Kubernetes pod/service CIDRs (`10.244.0.0/16`, `10.96.0.0/12`) are in `trusted` zone
7. Runs `firewall-cmd --reload` to apply all permanent config
8. Bounces the NM connection for `wlp2s0` so NM reports the correct zone to firewalld immediately
9. Updates `/etc/NetworkManager/dispatcher.d/99-masquerade.sh` to call `firewall-cmd --reload`
   on `wlp2s0 up` (instead of raw nft rules)

**Why the policy egress zone matters:**
NM may place `wlp2s0` in `public` rather than `external` after a firewalld reload. If the
forwarding policy says `egress-zone=external` but the interface is actually in `public`, packets
are forwarded to an empty zone and silently dropped. The script detects the runtime zone and
creates the policy against it.

**Verify after fix:**
```bash
# Confirm zones, masquerade, and policies
sudo firewall-cmd --get-active-zones
sudo firewall-cmd --zone=public --query-masquerade   # or --zone=external if wlp2s0 is there
sudo firewall-cmd --policy=InternalToExternal --list-all

# Test from each worker
for w in star_worker1@192.168.1.51 star_worker2@192.168.1.52 \
          star_worker3@192.168.1.53 star_worker4@192.168.1.54; do
  printf "%-25s" "${w}"
  ssh -i ~/.ssh/id_ed25519 -o BatchMode=yes "${w}" \
    "curl -so /dev/null --connect-timeout 6 -w 'quay=%{http_code}' https://quay.io/v2/"
  echo ""
done
# Expected: quay=401 for all workers (401 = unauthenticated, connection succeeded)
```

---

### 14.6 `rp_filter=1` on Calico Interfaces Breaks ipvs Pod-to-Service Routing

**Symptom:** In ipvs kube-proxy mode, pods cannot reach ClusterIPs (`Host is unreachable`) even though `kube-ipvs0` has the correct IPs bound and ipvs virtual server table is populated correctly.

**Root cause:** When a new Calico veth interface (`caliXXXX`) is created for a pod, it inherits `rp_filter=1` from the kernel's `default` interface template. With ipvs mode, return traffic from ClusterIPs arrives via a different path than the original packet (asymmetric routing), and `rp_filter=1` (strict reverse path filtering) drops these packets silently.

**Diagnosis:**
```bash
# Check rp_filter on cali interfaces
ssh star_worker1@192.168.1.51 "
  ls /proc/sys/net/ipv4/conf/ | grep cali | while read iface; do
    echo \${iface}: \$(cat /proc/sys/net/ipv4/conf/\${iface}/rp_filter)
  done
"
# If any show rp_filter: 1, this is the problem

# Check default template
ssh star_worker1@192.168.1.51 "cat /proc/sys/net/ipv4/conf/default/rp_filter"
```

**Fix — set rp_filter=0 on all nodes permanently:**
```bash
for ip in 192.168.1.51 192.168.1.52 192.168.1.53 192.168.1.54; do
  idx=$((${ip##*.} - 50))
  user="star_worker${idx}"
  ssh -o BatchMode=yes ${user}@${ip} "
    sudo sysctl -w net.ipv4.conf.all.rp_filter=0
    sudo sysctl -w net.ipv4.conf.default.rp_filter=0
    sudo tee /etc/sysctl.d/99-calico-ipvs.conf <<EOF
net.ipv4.conf.all.rp_filter = 0
net.ipv4.conf.default.rp_filter = 0
net.ipv4.conf.tunl0.rp_filter = 0
net.ipv4.conf.all.accept_local = 1
EOF
    sudo sysctl -p /etc/sysctl.d/99-calico-ipvs.conf
    echo done on \$(hostname)
  "
done
```

**Install a systemd watcher to fix rp_filter on new cali interfaces as they are created** (udev rules don't fire reliably for CNI-created veth pairs on RHEL 10):
```bash
for ip in 192.168.1.51 192.168.1.52 192.168.1.53 192.168.1.54; do
  idx=$((${ip##*.} - 50))
  user="star_worker${idx}"
  ssh -o BatchMode=yes ${user}@${ip} 'sudo tee /usr/local/bin/fix-cali-rp-filter.sh > /dev/null << '"'"'SCRIPT'"'"'
#!/bin/bash
while true; do
  for iface in $(ls /proc/sys/net/ipv4/conf/ | grep cali); do
    val=$(cat /proc/sys/net/ipv4/conf/${iface}/rp_filter 2>/dev/null)
    if [ "$val" = "1" ]; then
      echo 0 > /proc/sys/net/ipv4/conf/${iface}/rp_filter
    fi
  done
  sleep 2
done
SCRIPT
sudo chmod +x /usr/local/bin/fix-cali-rp-filter.sh
sudo tee /etc/systemd/system/fix-cali-rp-filter.service > /dev/null <<EOF
[Unit]
Description=Fix rp_filter for Calico interfaces
After=network.target
[Service]
Type=simple
ExecStart=/usr/local/bin/fix-cali-rp-filter.sh
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable --now fix-cali-rp-filter.service
echo done' 2>&1
done
```

---

### 14.7 Helm Scripts Fail with `helm: command not found` Under `sudo`

**Symptom:** Scripts called via `sudo bash` fail with `helm: command not found` even though `helm` is installed at `/usr/local/bin/helm`.

**Root cause:** `sudo` on RHEL uses a restricted `PATH` that does not include `/usr/local/bin`. Helm is installed there by the `get-helm-3` installer.

**Fix:** Add `export PATH="/usr/local/bin:${PATH}"` at the top of every script that calls `helm`:
```bash
set -euo pipefail
export PATH="/usr/local/bin:${PATH}"   # ← add this line
```

Affected scripts: [`08-install-helm.sh`](../scripts/master/08-install-helm.sh), [`09-deploy-argocd.sh`](../scripts/master/09-deploy-argocd.sh), [`10-deploy-openbao.sh`](../scripts/master/10-deploy-openbao.sh).

---

### 14.8 ArgoCD NodePort Accessible but Browser Shows Connection Refused

**Symptom:** `curl -sk https://192.168.1.50:30443` returns `ok` but browser shows a connection error or ArgoCD serves HTTP instead of HTTPS.

**Root cause:** ArgoCD chart ≥ 6.x uses `configs.params.server.insecure` to control TLS. Older values files used `server.extraArgs: [--insecure]`. If `--insecure` is set, ArgoCD serves plain HTTP on port 8080 for both the HTTP and HTTPS NodePorts — the HTTPS NodePort (30443) maps to container port 443 but ArgoCD isn't listening on 443, only 8080.

**Correct `helm/argocd/values.yaml` configuration:**
```yaml
server:
  service:
    type: NodePort
    nodePortHttp: 30080
    nodePortHttps: 30443

configs:
  params:
    server.insecure: "false"   # ArgoCD serves HTTPS on 8080 internally
```

- Use `http://192.168.1.50:30080` if `server.insecure: "true"` (HTTP mode)
- Use `https://192.168.1.50:30443` if `server.insecure: "false"` (HTTPS mode, self-signed cert)

---

### 14.9 Registry Deployment Stuck Pending (`nodeSelector` Empty String)

**Symptom:** `private-registry` pod stuck in `Pending` indefinitely. PVC also stuck `Pending`. Events show `0/5 nodes available: 5 node(s) didn't match Pod's node affinity/selector`.

**Root cause:** [`manifests/registry/registry-deployment.yaml`](../manifests/registry/registry-deployment.yaml) had `nodeSelector: kubernetes.io/hostname: ""` — an empty string matches no node.

**Fix:**
```bash
# Check your node names
kubectl get nodes -o wide

# Patch the deployment with the correct master hostname
kubectl patch deployment private-registry -n registry \
  --type merge \
  -p '{"spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":"master.local"}}}}}'

# Or edit the manifest directly
# manifests/registry/registry-deployment.yaml:
#   nodeSelector:
#     kubernetes.io/hostname: master.local
```

---

### 14.10 Helm Pre-upgrade Hook Timeout (`argocd-redis-secret-init`)

**Symptom:** `helm upgrade argocd` fails with `pre-upgrade hooks failed: timed out waiting for the condition`. The `argocd-redis-secret-init` job keeps failing.

**Root cause:** The hook job runs `argocd admin redis-initial-password` which needs to reach the Kubernetes API server (`10.96.0.1:443`). If pod-to-ClusterIP networking is broken (see §14.4), the job can never complete.

**Fix:**
1. Fix pod-to-ClusterIP networking first (§14.4)
2. Delete the stale job and retry:
```bash
kubectl delete job argocd-redis-secret-init -n argocd 2>/dev/null || true
kubectl delete pod -n argocd -l app.kubernetes.io/name=argocd-server 2>/dev/null || true

export PATH="/usr/local/bin:$PATH"
helm upgrade argocd argo/argo-cd \
  --namespace argocd \
  --version 6.11.1 \
  --values helm/argocd/values.yaml \
  --wait --timeout 5m
```

---

### 14.11 Quick Networking Health Check

Run this after any networking change to verify the full stack is working:

```bash
# 1. All nodes Ready
kubectl get nodes

# 2. All kube-proxy pods Running
kubectl -n kube-system get pods -l k8s-app=kube-proxy

# 3. All Calico pods Running with correct BGP peers
kubectl -n kube-system get pods -l k8s-app=calico-node
kubectl exec -n kube-system <calico-node-on-worker> -- birdcl show protocols | grep Mesh
# All peers should show: Established

# 4. Pod-to-ClusterIP connectivity from a worker
kubectl run nettest --image=public.ecr.aws/docker/library/redis:7.2.4-alpine \
  --restart=Never --command \
  --overrides='{"spec":{"nodeSelector":{"kubernetes.io/hostname":"worker1.local"}}}' \
  -- sh -c "wget -qO- --timeout=5 --no-check-certificate https://10.96.0.1/healthz && echo ClusterIP_OK; nslookup kubernetes.default.svc.cluster.local && echo DNS_OK"
sleep 10; kubectl logs nettest; kubectl delete pod nettest --grace-period=0

# 5. NodePort reachable
curl -sk https://192.168.1.50:30443/healthz && echo ArgoCD_OK
curl -sk https://192.168.1.50:30500/v2/ && echo Registry_OK
curl -s http://192.168.1.50:30820/v1/sys/health | python3 -m json.tool | grep -E "initialized|sealed" && echo OpenBao_OK

# 6. Worker internet access
for w in star_worker1@192.168.1.51 star_worker2@192.168.1.52 \
          star_worker3@192.168.1.53 star_worker4@192.168.1.54; do
  printf "%-25s" "${w}"
  ssh -i ~/.ssh/id_ed25519 -o BatchMode=yes "${w}" \
    "curl -so /dev/null --connect-timeout 6 -w 'quay=%{http_code}' https://quay.io/v2/"
  echo ""
done
# Expected: quay=401 for all workers
```

---

### 14.12 OpenBao Deployment Failures

#### 14.12.1 `nodePort: 30820 already allocated` on Install

**Symptom:**
```
Error: failed to create resource: Service "openbao-ui" is invalid:
  spec.ports[0].nodePort: Invalid value: 30820: provided port is already allocated
```

**Root cause:** The `values.yaml` had port 30820 defined in **two places**:
- `server.service.nodePort: 30820` → creates the main `openbao` NodePort service
- `ui.serviceNodePort: 30820` → creates a **separate** `openbao-ui` NodePort service

Both try to claim port 30820, which Kubernetes rejects. This also means a failed install
leaves the `openbao` service behind even after `helm uninstall`, blocking future installs.

**Fix (already applied in `helm/openbao/values.yaml`):**
```yaml
# WRONG — two services competing for the same NodePort
ui:
  serviceType: NodePort
  serviceNodePort: 30820   # ← remove these two lines

# CORRECT — only server.service gets the NodePort
ui:
  enabled: true
  serviceType: ClusterIP   # server.service already exposes NodePort 30820
```

**Cleanup after a failed partial install:**
```bash
helm uninstall openbao -n prod 2>/dev/null || true
# Clean up any stale openbao namespace from an old install attempt
helm uninstall openbao -n openbao 2>/dev/null || true
kubectl delete namespace openbao --ignore-not-found
# Wait for namespace to terminate, then confirm port is free
kubectl get svc --all-namespaces | grep 30820
```

---

#### 14.12.2 `ErrImagePull` — Doubled Registry Prefix

**Symptom:** Pod events show:
```
Failed to pull image "quay.io/quay.io/openbao/openbao:2.6.0": not found
```

**Root cause:** The openbao chart has a separate `server.image.registry` field (defaults to
`quay.io`). Setting `repository: quay.io/openbao/openbao` in values causes the registry to
be prepended again, resulting in `quay.io/quay.io/openbao/openbao`.

**Fix (already applied in `helm/openbao/values.yaml`):**
```yaml
server:
  image:
    repository: openbao/openbao   # NO registry prefix — chart adds quay.io automatically
    tag: "2.6.0"                  # quay.io tags have no 'v' prefix
```

---

#### 14.12.3 `ErrImagePull` — Wrong Image Tag (`v2.6.0` not found)

**Symptom:**
```
failed to resolve image: quay.io/openbao/openbao:v2.6.0: not found
```

**Root cause:** quay.io tags for openbao use no `v` prefix. The tag is `2.6.0`, not `v2.6.0`.

**Fix:** Pin the tag explicitly without the `v` prefix:
```yaml
server:
  image:
    tag: "2.6.0"
```

Verify available tags:
```bash
curl -s "https://quay.io/api/v1/repository/openbao/openbao/tag/?limit=10&onlyActiveTags=true" \
  | python3 -c "import sys,json; [print(t['name']) for t in json.load(sys.stdin)['tags']]"
```

---

#### 14.12.4 Pod Scheduled on Worker with No Internet (ImagePullBackOff)

**Symptom:** Pod is scheduled on a worker node and enters `ImagePullBackOff` because the
worker cannot reach `quay.io` (internet not yet configured through master).

**Root cause:** The Helm chart places the OpenBao server pod on any available node. Workers
route internet through master (`192.168.1.50`) and fail to pull if masquerade is not set up.

**Fix (already applied in `helm/openbao/values.yaml`):** Pin the server pod to master:
```yaml
server:
  nodeSelector:
    kubernetes.io/hostname: master.local
  tolerations:
    - key: "node-role.kubernetes.io/control-plane"
      operator: "Exists"
      effect: "NoSchedule"
```

Pre-pull both images on master before deploying:
```bash
sudo crictl pull quay.io/openbao/openbao:2.6.0
sudo crictl pull docker.io/hashicorp/vault-k8s:1.7.2
```

For workers to pull images independently (agent-injector and future workloads), fix the
masquerade first — see [§14.5](#145-worker-nodes-cannot-pull-images--reach-internet).

---

### 14.13 NodePort / ClusterIP / Pod IP Unreachable from Master — Calico Interfaces Not in firewalld Zone

**Symptom:** After fixing `rp_filter`, trusted CIDRs, and kube-proxy mode, `curl http://192.168.1.50:<NodePort>` still times out (exit 28). `kubectl exec <pod> -- wget http://127.0.0.1:<port>` works. `curl http://<podIP>:<port>` also times out.

**Root cause:** Calico creates veth pairs (`caliXXXX`) for each pod on the master node. These interfaces are **not assigned to any firewalld zone**. The `inet firewalld` nftables table's `filter_INPUT_POLICIES` chain ends with:
```
iifname "eno1" reject with icmpx admin-prohibited
iifname "wlp2s0" reject with icmpx admin-prohibited
jump filter_IN_public
reject with icmpx admin-prohibited   ← cali interfaces fall through to here
```
Return packets from pods (source `10.244.233.x`) arrive via the cali veth, hit `filter_INPUT_POLICIES`, find no matching zone rule, and are **rejected**. The `ip saddr 10.244.0.0/16 accept` rule in `filter_INPUT_POLICIES` matches the pod CIDR as a **source**, which is correct for pods-to-master. But for master-to-pod traffic, the **return** packet arrives via the cali interface — the interface check runs before the source-IP check and rejects it.

**This is the definitive fix for "NodePort not reachable from master" on RHEL 10 + Calico + nftables kube-proxy + firewalld.**

**Diagnosis:**
```bash
# Confirm cali interfaces are not in any firewalld zone
sudo firewall-cmd --get-active-zones
# cali interfaces will not appear under any zone

# Inspect the reject rule in filter_INPUT_POLICIES
sudo nft list chain inet firewalld filter_INPUT_POLICIES | grep -E "cali|reject"
```

**Fix — run the dedicated script:**
```bash
sudo bash scripts/master/fix-calico-firewalld.sh
```

The script:
1. Adds all existing `cali*` interfaces and `tunl0` to the `trusted` zone (permanent + runtime)
2. Reloads firewalld
3. Installs a systemd watcher (`fix-cali-firewalld.service`) that adds every new `cali*` interface to the trusted zone and sets `rp_filter=0` as pods are created
4. Tests Pod IP, ClusterIP, and NodePort connectivity

**Or apply manually:**
```bash
# Add all current cali interfaces to trusted zone
for iface in $(ip link show | grep -oE 'cali[a-z0-9]+' | sort -u); do
  sudo firewall-cmd --permanent --zone=trusted --add-interface="${iface}"
  sudo firewall-cmd --zone=trusted --add-interface="${iface}"
done
sudo firewall-cmd --permanent --zone=trusted --add-interface=tunl0
sudo firewall-cmd --reload
```

**Verify:**
```bash
curl -s http://192.168.1.50:30820/v1/sys/health | python3 -m json.tool
# Expected: {"initialized": true, "sealed": false, ...}
```

> **Required on every RHEL 10 master with Calico + firewalld.** New `cali*` interfaces are created on every pod start — the watchdog service ensures new pods are always accessible. Add `fix-calico-firewalld.sh` to the post-install runbook.

---
