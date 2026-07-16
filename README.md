# Kubernetes Build Platform

## Repository Structure

```
k8s-platform/
├── deploy-all.sh                         ← Master deployment script (run last)
├── scripts/
│   ├── workers.conf                      ← Worker inventory: IP + SSH user per line
│   ├── workers/
│   │   ├── 00-common-prereqs.sh          ← Run on EVERY node first
│   │   └── 02-worker-join.sh             ← Run on each worker after master init
│   ├── master/
│   │   ├── 01-kubeadm-init.sh            ← Master only: init cluster + Calico
│   │   ├── deploy-workers.sh             ← Master only: push + run worker scripts over SSH
│   │   ├── 03-label-workers.sh           ← Label workers, verify health
│   │   ├── 07-staging-testing-setup.sh   ← Create /opt/k8s-builds tree
│   │   ├── 08-install-helm.sh            ← Install Helm v3
│   │   ├── 09-deploy-argocd.sh           ← Deploy ArgoCD via Helm
│   │   ├── 10-deploy-openbao.sh          ← Deploy OpenBao via Helm + init
│   │   └── 11-namespaces-quotas.sh       ← Apply prod/test namespace quotas
│   ├── storage/
│   │   ├── 04-discover-storage.sh        ← Run on EVERY node: find best disk
│   │   └── 05-install-local-path-provisioner.sh  ← Master: install provisioner
│   └── registry/
│       └── 06-registry-setup.sh          ← Master: TLS cert + registry deploy
├── manifests/
│   ├── storage/
│   │   └── storageclass.yaml             ← local-path StorageClass (default)
│   ├── registry/
│   │   ├── registry-pvc.yaml             ← 50 Gi PVC for image storage
│   │   ├── registry-deployment.yaml      ← registry:2 with TLS
│   │   └── registry-service.yaml         ← NodePort 30500
│   └── namespaces/
│       ├── prod-namespace.yaml           ← prod NS + ResourceQuota + LimitRange
│       └── test-namespace.yaml           ← test NS (1/5 of prod quota)
├── helm/
│   ├── argocd/
│   │   └── values.yaml                   ← ArgoCD Helm values
│   └── openbao/
│       └── values.yaml                   ← OpenBao Helm values
├── argocd-apps/
│   ├── app-project-platform.yaml         ← ArgoCD AppProject
│   ├── app-registry.yaml                 ← ArgoCD App: registry
│   ├── app-storage.yaml                  ← ArgoCD App: storage
│   ├── app-namespaces.yaml               ← ArgoCD App: namespaces
│   └── app-openbao.yaml                  ← ArgoCD App: openbao
├── staging/                              ← Git-tracked staging manifests
├── testing/                              ← Git-tracked testing manifests
└── docs/
    └── platform-guide.md                 ← Full platform documentation
```

## Quick Start

> ⚠️ **Required order — do not skip steps or run out of sequence:**
> ```
> Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 6
> ```
> Worker bootstrap (Phase 3) will fail with _"failed to load admin kubeconfig"_ if
> master init (Phase 2) has not completed first.

### Phase 1 — Master node prerequisites
```bash
# On the MASTER only
sudo bash scripts/workers/00-common-prereqs.sh
```

### Phase 1b — Open firewall ports (REQUIRED on ALL nodes before cluster init)

Workers connect to the master API server from `192.168.1.51-54` over the wired interface.
firewalld on RHEL/Fedora blocks this by default — **no ports are open until you add them**.
Skipping this step causes the worker join to silently time out after 5 minutes with:
`rate: Wait(n=1) would exceed context deadline`

**Run on MASTER (`192.168.1.50`):**
```bash
sudo firewall-cmd --permanent --add-port=6443/tcp        # Kubernetes API server
sudo firewall-cmd --permanent --add-port=2379-2380/tcp   # etcd
sudo firewall-cmd --permanent --add-port=10250/tcp       # kubelet API
sudo firewall-cmd --permanent --add-port=10257/tcp       # kube-controller-manager
sudo firewall-cmd --permanent --add-port=10259/tcp       # kube-scheduler
sudo firewall-cmd --permanent --add-port=4789/udp        # Calico VXLAN overlay
sudo firewall-cmd --permanent --add-port=179/tcp         # Calico BGP
sudo firewall-cmd --reload

# Verify
sudo firewall-cmd --list-ports
# Expected: 179/tcp 2379-2380/tcp 4789/udp 6443/tcp 10250/tcp 10257/tcp 10259/tcp
```

**Run on EVERY WORKER (`192.168.1.51-54`):**
```bash
sudo firewall-cmd --permanent --add-port=10250/tcp        # kubelet API
sudo firewall-cmd --permanent --add-port=30000-32767/tcp  # NodePort services
sudo firewall-cmd --permanent --add-port=4789/udp         # Calico VXLAN overlay
sudo firewall-cmd --permanent --add-port=179/tcp          # Calico BGP
sudo firewall-cmd --reload

# Verify
sudo firewall-cmd --list-ports
# Expected: 179/tcp 4789/udp 10250/tcp 30000-32767/tcp
```

> ℹ️ `curl -sk https://192.168.1.50:6443/healthz` returns `ok` from master itself because
> master talks to `127.0.0.1` (loopback bypasses firewalld). Workers connect via `192.168.1.x`
> which goes through firewalld on `eno1` — port 6443 must be explicitly opened.

### Phase 2 — Master cluster init
```bash
sudo bash scripts/master/01-kubeadm-init.sh
```

This sets up `/etc/kubernetes/admin.conf` and the kubeconfig that all subsequent steps depend on.

```bash
# Wait for Calico CNI and CoreDNS to be fully Ready before touching workers.
# Calico image pull takes 1-3 min on first boot — do NOT skip this step.
kubectl wait --for=condition=Ready pod -l k8s-app=calico-node -n kube-system --timeout=300s
kubectl wait --for=condition=Ready pod -l k8s-app=kube-dns   -n kube-system --timeout=300s

# Verify all control-plane pods are Running before continuing.
kubectl get pods -n kube-system
kubectl get nodes
# Expected: master   Ready   control-plane
```

> ⚠️ Skipping the `kubectl wait` step and running worker bootstrap too early causes:
> `rate: Wait(n=1) would exceed context deadline` — see Troubleshooting below.

### Phase 3 — Bootstrap all workers from the master (recommended)
```bash
# Step 1 — Edit the worker inventory with your node IPs and SSH usernames.
# Each worker can have a different non-root user with sudo access.
vi scripts/workers.conf

# Step 2 — Generate an SSH key for root on the master (once only).
# "sudo bash deploy-workers.sh" runs the entire script as root, so ssh
# inside the script looks for keys in /root/.ssh/ — not ~/.ssh/.
# All three key commands below must use sudo for the same reason.
sudo ssh-keygen -t ed25519 -f /root/.ssh/id_ed25519 -N ""

# Step 3 — Push root's public key to every worker listed in workers.conf.
# Reads IP and username directly from the inventory — no hardcoding needed.
# You will be prompted for each worker user's password once.
# ssh-copy-id is not affected by the stdin problem so the pipe form is fine here.
grep -v '^\s*#' scripts/workers.conf | grep -v '^\s*$' | while read -r ip user; do
  sudo ssh-copy-id "${user}@${ip}"
done

# Step 4 — Verify passwordless SSH access for every worker.
# <&3 keeps stdin as your terminal so -n works; the worker list comes from fd 3.
while read -r ip user <&3; do
  sudo ssh -n -o BatchMode=yes "${user}@${ip}" hostname && echo "${ip}: OK" || echo "${ip}: FAILED"
done 3< <(grep -v '^\s*#' scripts/workers.conf | grep -v '^\s*$')

# Step 5 — Enable passwordless sudo on every worker (REQUIRED).
# deploy-workers.sh runs sudo over a non-interactive SSH session — without this
# it fails with: "sudo: a terminal is required to read the password"
# <&3 keeps stdin as your terminal so -t can allocate a pseudo-terminal for
# the remote sudo password prompt; the worker list comes from fd 3.
while read -r ip user <&3; do
  echo "Setting NOPASSWD sudo on ${ip} for ${user}..."
  sudo ssh -t "${user}@${ip}" \
    "echo '${user} ALL=(ALL) NOPASSWD: ALL' | sudo tee /etc/sudoers.d/${user} && sudo chmod 0440 /etc/sudoers.d/${user}"
done 3< <(grep -v '^\s*#' scripts/workers.conf | grep -v '^\s*$')
# Expected output per worker:
#   Setting NOPASSWD sudo on 192.168.1.51 for star_worker1...
#   [sudo] password for star_worker1:        ← enter the worker user's password
#   star_worker1 ALL=(ALL) NOPASSWD: ALL     ← confirms the rule was written
#   Connection to 192.168.1.51 closed.

# Step 6 — Verify passwordless sudo works for every worker.
while read -r ip user <&3; do
  sudo ssh -n "${user}@${ip}" sudo whoami && echo "${ip}: OK" || echo "${ip}: FAILED"
done 3< <(grep -v '^\s*#' scripts/workers.conf | grep -v '^\s*$')
# Expected output per worker:
#   root
#   192.168.1.51: OK

# NOTE: plain "ssh star_worker1@192.168.1.51" (without sudo) will still ask for a
# password — that is normal. The deploy script always uses "sudo ssh" which reads
# from /root/.ssh/ where the key was installed. Plain ssh uses ~star_master/.ssh/.

# Step 7 — Run the orchestrator; it reads workers.conf automatically.
sudo bash scripts/master/deploy-workers.sh

# Override inventory file location if needed:
# sudo bash scripts/master/deploy-workers.sh --inventory /path/to/workers.conf

# Or target specific workers inline (IP:USER format):
# sudo bash scripts/master/deploy-workers.sh 192.168.1.51:star_worker1 192.168.1.52:star_worker2
```

> **Manual alternative (one worker at a time):**
> ```bash
> # On each worker node directly
> sudo bash scripts/workers/00-common-prereqs.sh
> sudo bash scripts/workers/02-worker-join.sh 192.168.1.50
> ```

### Troubleshooting — Step 7 Join Failure: "rate: Wait(n=1) would exceed context deadline" (firewall blocking port 6443)

This same error also occurs when the **firewall on master is blocking workers from reaching port 6443**.
Unlike the Calico timing issue, this error repeats on every retry and never self-resolves.

**How to tell the difference:**

| Cause | Behaviour |
|---|---|
| Calico not ready yet | Fails first time, succeeds after waiting a few minutes and retrying |
| Firewall blocking 6443 | Fails every time, no matter how long you wait |

**Diagnose — run on each worker:**
```bash
# Should return "ok" — if empty or times out, firewall is the cause
curl -sk --max-time 5 https://192.168.1.50:6443/healthz
```

**Fix — open the required ports (see Phase 1b above):**
```bash
# On MASTER
sudo firewall-cmd --permanent --add-port=6443/tcp
sudo firewall-cmd --permanent --add-port=2379-2380/tcp
sudo firewall-cmd --permanent --add-port=10250/tcp
sudo firewall-cmd --permanent --add-port=10257/tcp
sudo firewall-cmd --permanent --add-port=10259/tcp
sudo firewall-cmd --permanent --add-port=4789/udp
sudo firewall-cmd --permanent --add-port=179/tcp
sudo firewall-cmd --reload

# On EVERY WORKER
sudo firewall-cmd --permanent --add-port=10250/tcp
sudo firewall-cmd --permanent --add-port=30000-32767/tcp
sudo firewall-cmd --permanent --add-port=4789/udp
sudo firewall-cmd --permanent --add-port=179/tcp
sudo firewall-cmd --reload
```

Then confirm from each worker:
```bash
curl -sk https://192.168.1.50:6443/healthz   # must return: ok
```

Then re-run:
```bash
sudo bash scripts/master/deploy-workers.sh
```

---

### Troubleshooting — Step 7 Join Failure: "client rate limiter Wait returned an error: rate: Wait(n=1) would exceed context deadline"

[`deploy-workers.sh`](scripts/master/deploy-workers.sh) runs worker bootstrap immediately after master init. If
Calico's `calico-node` pod is still pulling its image and has not yet written `/var/lib/calico/nodename`,
any worker that tries to join will fail with a 5-minute timeout:

```
[preflight] Running pre-flight checks
error execution phase preflight: couldn't validate the identity of the API Server:
failed to request the cluster-info ConfigMap: client rate limiter Wait returned an error:
rate: Wait(n=1) would exceed context deadline
[19:20:56] [192.168.1.52] Join: FAILED
```

**What happens internally:**
```
worker joins → API server sets up pod network → Calico CNI plugin called
→ stat /var/lib/calico/nodename: no such file or directory  (Calico still starting)
→ API server retries hit rate limiter → context deadline exceeded after 5 min
```

**Fix — wait for all kube-system pods to be Ready before running deploy-workers.sh:**

```bash
# On master — run these after 01-kubeadm-init.sh, before deploy-workers.sh
kubectl wait --for=condition=Ready pod -l k8s-app=calico-node -n kube-system --timeout=300s
kubectl wait --for=condition=Ready pod -l k8s-app=kube-dns   -n kube-system --timeout=300s

# Verify everything is Running before proceeding
kubectl get pods -n kube-system
# All pods must show 1/1 Running — especially calico-node and coredns

# Only then bootstrap the workers
sudo bash scripts/master/deploy-workers.sh
```

> ℹ️ Calico's image pull typically takes **1–3 minutes** on first boot depending on internet speed.
> The `kubectl wait` commands will block until Ready or timeout — safe to run immediately after init.

---

### Troubleshooting — Step 7 Join Failure: "No SSH access and no /tmp/worker-join-cmd.sh found"

[`02-worker-join.sh`](scripts/workers/02-worker-join.sh) needs to fetch a join token by SSHing from each
worker back to the master as `root`. If root SSH keys are not set up between the nodes, the
script falls back to `/tmp/worker-join-cmd.sh` on the worker. If neither is available you will
see:

```
[ERROR] No SSH access and no /tmp/worker-join-cmd.sh found.
    Copy /root/worker-join-cmd.sh from master to /tmp/worker-join-cmd.sh on this node.
[18:16:13] [192.168.1.54] Join: FAILED
```

> ⚠️ **Prerequisite:** `kubeadm token create` requires the cluster to be initialised first.
> If you see _"failed to load admin kubeconfig: open /root/.kube/config: no such file or directory"_
> it means Phase 2 (`01-kubeadm-init.sh`) has not been run yet. Complete Phase 2 before
> attempting any of the fixes below.

**Quickest fix — push the join command from master to every worker:**

```bash
# On master — generate a fresh join token (valid for 24 h) and save it.
# KUBECONFIG must point to the admin config so kubeadm can reach the cluster.
sudo KUBECONFIG=/etc/kubernetes/admin.conf kubeadm token create --print-join-command \
  | sudo tee /root/worker-join-cmd.sh
sudo chmod 600 /root/worker-join-cmd.sh

# Verify the file looks correct (should start with "kubeadm join ...").
sudo cat /root/worker-join-cmd.sh

# Push it to every worker — master already has SSH access to each worker user.
for worker_ip in 192.168.1.51 192.168.1.52 192.168.1.53 192.168.1.54; do
  echo "Copying join command to ${worker_ip}..."
  sudo scp /root/worker-join-cmd.sh "$(grep "^${worker_ip}" scripts/workers.conf | awk '{print $2}')@${worker_ip}:/tmp/worker-join-cmd.sh"
done

# Re-run the orchestrator — it will now find the file on each worker.
sudo bash scripts/master/deploy-workers.sh
```

**Permanent fix — set up root SSH keys so the script works automatically:**

```bash
# On master — create root's SSH key (skip if /root/.ssh/id_ed25519 already exists).
sudo ssh-keygen -t ed25519 -f /root/.ssh/id_ed25519 -N ""

# Push root's public key to each worker (you will be prompted for the worker root
# password once per node — or use the worker sudo user if root login is disabled).
for worker_ip in 192.168.1.51 192.168.1.52 192.168.1.53 192.168.1.54; do
  sudo ssh-copy-id -i /root/.ssh/id_ed25519.pub "root@${worker_ip}"
done

# Test — each should print the worker hostname with no password prompt.
for worker_ip in 192.168.1.51 192.168.1.52 192.168.1.53 192.168.1.54; do
  sudo ssh -o BatchMode=yes "root@${worker_ip}" hostname && echo "${worker_ip}: OK" || echo "${worker_ip}: FAILED"
done

# Re-run the orchestrator — it will now SSH as root to generate tokens directly.
sudo bash scripts/master/deploy-workers.sh
```

### Phase 4 — Label Workers
```bash
sudo bash scripts/master/03-label-workers.sh
```

### Phase 5 — Storage (all nodes, then master)

> **Prerequisite — SSH key for `star_master`** (different from Phase 3 which uses root's key):
> `04-discover-storage.sh` SSHes to workers as `star_master`, so a key must exist in `~star_master/.ssh/`.
> Run these **as `star_master` (no sudo)** before this phase if not already done:
> ```bash
> ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
> ssh-copy-id -i ~/.ssh/id_ed25519.pub star_worker1@192.168.1.51
> ssh-copy-id -i ~/.ssh/id_ed25519.pub star_worker2@192.168.1.52
> ssh-copy-id -i ~/.ssh/id_ed25519.pub star_worker3@192.168.1.53
> ssh-copy-id -i ~/.ssh/id_ed25519.pub star_worker4@192.168.1.54
> ```
> See [`docs/platform-guide.md` §3.2](docs/platform-guide.md) for full details.

```bash
# Step 1 — Run on master AND all workers in one command (recommended).
# Reads workers.conf automatically — same inventory used by deploy-workers.sh.
sudo bash scripts/storage/04-discover-storage.sh --all-nodes

# Override inventory file if needed:
# sudo bash scripts/storage/04-discover-storage.sh --all-nodes --inventory /path/to/workers.conf

# Step 2 — Run on MASTER only
sudo bash scripts/storage/05-install-local-path-provisioner.sh
```

> **Manual alternative (one node at a time):**
> ```bash
> # Run individually on each node that needs storage discovery
> sudo bash scripts/storage/04-discover-storage.sh
> ```

### Phase 6 — Full Platform (master only)
```bash
sudo bash deploy-all.sh
```

## Node Layout

| Role | IP | Joined |
|---|---|---|
| master | 192.168.1.50 | control-plane |
| worker1 | 192.168.1.51 | worker |
| worker2 | 192.168.1.52 | worker |
| worker3 | 192.168.1.53 | worker |
| worker4 | 192.168.1.54 | worker |

## Service Endpoints

| Service | URL |
|---|---|
| ArgoCD UI | https://192.168.1.50:30443 |
| OpenBao UI | http://192.168.1.50:30820 |
| Private Registry | https://192.168.1.50:30500 |

## Documentation

See [`docs/platform-guide.md`](docs/platform-guide.md) for full documentation covering every component.
