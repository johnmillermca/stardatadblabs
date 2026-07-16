#!/usr/bin/env bash
# =============================================================================
# 00-common-prereqs.sh
# Run on EVERY node (master + all workers) before kubeadm init/join.
# Tested on Ubuntu 22.04 / RHEL 9.
# =============================================================================
set -euo pipefail

KUBE_VERSION="1.30"
CONTAINERD_VERSION="1.7"
CALICO_VERSION="v3.27.3"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
die()  { echo "[ERROR] $*" >&2; exit 1; }

# ── Detect OS ────────────────────────────────────────────────────────────────
if   [ -f /etc/os-release ]; then source /etc/os-release; OS_ID="${ID,,}"; OS_VER="${VERSION_ID}";
else die "Cannot detect OS"; fi
log "Detected OS: ${OS_ID} ${OS_VER}"

# ── 1. Disable swap ───────────────────────────────────────────────────────────
log "Disabling swap..."
swapoff -a
sed -i '/\bswap\b/d' /etc/fstab

# ── 2. Kernel modules ─────────────────────────────────────────────────────────
log "Loading kernel modules..."
cat >/etc/modules-load.d/k8s.conf <<'EOF'
overlay
br_netfilter
EOF
modprobe overlay
modprobe br_netfilter

# ── 3. Sysctl settings ────────────────────────────────────────────────────────
log "Applying sysctl settings..."
cat >/etc/sysctl.d/99-k8s.conf <<'EOF'
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward                 = 1
fs.inotify.max_user_watches         = 524288
fs.inotify.max_user_instances       = 512
EOF
sysctl --system

# ── 4. Firewall tweaks (permissive for lab; tighten in prod) ─────────────────
if systemctl is-active --quiet firewalld 2>/dev/null; then
  log "firewalld detected – setting to permissive"
  firewall-cmd --set-default-zone=trusted --permanent || true
  firewall-cmd --reload || true
fi
if command -v ufw &>/dev/null && ufw status | grep -q "Status: active"; then
  log "ufw detected – disabling for Kubernetes"
  ufw disable
fi

# ── 5. Install containerd ─────────────────────────────────────────────────────
log "Installing containerd..."
case "${OS_ID}" in
  ubuntu|debian)
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y -qq curl gnupg2 ca-certificates apt-transport-https lsb-release
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
      | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
      > /etc/apt/sources.list.d/docker.list
    apt-get update -qq
    apt-get install -y -qq containerd.io
    ;;
  rhel|centos|rocky|almalinux|fedora)
    dnf install -y -q curl gnupg2 ca-certificates
    dnf config-manager --add-repo \
      https://download.docker.com/linux/centos/docker-ce.repo
    dnf install -y -q containerd.io
    ;;
  *) die "Unsupported OS: ${OS_ID}" ;;
esac

# ── 6. Configure containerd with SystemdCgroup ────────────────────────────────
log "Configuring containerd..."
mkdir -p /etc/containerd
containerd config default > /etc/containerd/config.toml
sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
systemctl enable --now containerd
systemctl restart containerd

# ── 7. Install kubeadm / kubelet / kubectl ────────────────────────────────────
log "Installing Kubernetes ${KUBE_VERSION} tooling..."
case "${OS_ID}" in
  ubuntu|debian)
    curl -fsSL "https://pkgs.k8s.io/core:/stable:/v${KUBE_VERSION}/deb/Release.key" \
      | gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
    echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] \
https://pkgs.k8s.io/core:/stable:/v${KUBE_VERSION}/deb/ /" \
      > /etc/apt/sources.list.d/kubernetes.list
    apt-get update -qq
    apt-get install -y -qq kubelet kubeadm kubectl
    apt-mark hold kubelet kubeadm kubectl
    ;;
  rhel|centos|rocky|almalinux|fedora)
    cat >/etc/yum.repos.d/kubernetes.repo <<EOF
[kubernetes]
name=Kubernetes
baseurl=https://pkgs.k8s.io/core:/stable:/v${KUBE_VERSION}/rpm/
enabled=1
gpgcheck=1
gpgkey=https://pkgs.k8s.io/core:/stable:/v${KUBE_VERSION}/rpm/repodata/repomd.xml.key
EOF
    dnf install -y -q kubelet kubeadm kubectl --disableexcludes=kubernetes
    ;;
esac
systemctl enable --now kubelet

# ── 8. crictl config ──────────────────────────────────────────────────────────
cat >/etc/crictl.yaml <<'EOF'
runtime-endpoint: unix:///run/containerd/containerd.sock
image-endpoint:   unix:///run/containerd/containerd.sock
timeout: 10
debug: false
EOF

log "Node prerequisites complete. Proceed with master init or worker join."
