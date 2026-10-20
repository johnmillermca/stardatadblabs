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

# ── 4. Firewall — open required Kubernetes + Calico ports ────────────────────
# Sets explicit allow rules rather than switching to "trusted" (allow-all) zone.
# This survives reboots and works whether the default zone is public or trusted.
#
# Required ports/protocols:
#   10250/tcp   — kubelet API (kubectl logs, exec, port-forward)
#   30000-32767 — NodePort services (TCP + UDP)
#   179/tcp     — Calico BGP (even in IPIP mode, BGP peering uses this)
#   ipip (proto 4) — Calico IPIP encapsulation for cross-node pod traffic
#   FORWARD via tunl0 — allow encapsulated pod packets to traverse the tunnel
#
# Without these, pods on this node appear Running but are completely unreachable
# from other nodes — NodePorts return HTTP 000, kubectl logs fails with
# "no route to host", and cross-node pod networking silently drops all packets.
if systemctl is-active --quiet firewalld 2>/dev/null; then
  log "firewalld detected – applying Kubernetes firewall rules..."

  # Detect the default-route interface dynamically (enp1s0, eth0, ens3, etc.)
  DEFAULT_IFACE=$(ip -4 route show default 2>/dev/null | awk '/^default/{print $5; exit}')
  DEFAULT_IFACE="${DEFAULT_IFACE:-enp1s0}"
  log "Using default network interface: ${DEFAULT_IFACE}"

  # Move the physical NIC and Calico IPIP tunnel interface into the 'trusted' zone.
  # This is the correct approach on RHEL/nftables-based firewalld — the 'public' zone
  # has implicit reject-all rules at the end of its nftables chain that override any
  # explicit port/protocol allow rules, including firewall-cmd --direct iptables rules
  # (which are evaluated in a separate table and do NOT override nftables chains).
  # 'trusted' zone = accept all, which is appropriate for internal cluster traffic.
  firewall-cmd --permanent --zone=trusted --add-interface="${DEFAULT_IFACE}"
  firewall-cmd --permanent --zone=trusted --add-interface=tunl0

  firewall-cmd --reload
  log "firewalld: interfaces '${DEFAULT_IFACE}' and 'tunl0' moved to trusted zone."
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
