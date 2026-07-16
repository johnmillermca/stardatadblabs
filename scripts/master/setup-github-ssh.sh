#!/usr/bin/env bash
# =============================================================================
# setup-github-ssh.sh
#
# Sets up SSH key authentication for GitHub so git push works reliably
# even when HTTPS is disrupted by iptables/Calico rules.
#
# USAGE
#   bash scripts/master/setup-github-ssh.sh
#
# WHAT IT DOES
#   1. Generates an ed25519 SSH key (if not already present)
#   2. Writes an SSH config entry for github.com
#   3. Switches the git remote from HTTPS to SSH
#   4. Prints the public key for you to add to GitHub Settings → SSH Keys
#
# After running:
#   - Copy the printed public key
#   - Go to: https://github.com/settings/keys → New SSH key → paste it
#   - Then run: ssh -T git@github.com   (should print "Hi johnmillermca!")
#   - Then run: bash scripts/git-sync-github.sh "your message"
# =============================================================================
set -euo pipefail

GITHUB_USER="johnmillermca"
REPO="stardatadblabs"
KEY_FILE="${HOME}/.ssh/github_stardatadblabs"
SSH_CONFIG="${HOME}/.ssh/config"
REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

log() { echo "[$(date '+%H:%M:%S')] $*"; }
ok()  { echo "  ✓ $*"; }

mkdir -p "${HOME}/.ssh"
chmod 700 "${HOME}/.ssh"

# ── 1. Generate SSH key ────────────────────────────────────────────────────────
if [[ -f "${KEY_FILE}" ]]; then
  log "SSH key already exists at ${KEY_FILE} — skipping generation"
else
  log "Generating ed25519 SSH key for GitHub..."
  ssh-keygen -t ed25519 \
    -C "platform@stardatadblabs.local" \
    -f "${KEY_FILE}" \
    -N ""
  ok "Key generated: ${KEY_FILE}"
fi

# ── 2. Write SSH config entry ──────────────────────────────────────────────────
log "Configuring SSH client for github.com..."
# Remove any existing github.com block to avoid duplicates
if [[ -f "${SSH_CONFIG}" ]]; then
  # Remove existing stardatadblabs host block if present
  sed -i '/^# github-stardatadblabs$/,/^$/d' "${SSH_CONFIG}" 2>/dev/null || true
fi

cat >> "${SSH_CONFIG}" <<EOF

# github-stardatadblabs
Host github.com
  HostName github.com
  User git
  IdentityFile ${KEY_FILE}
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new
  ServerAliveInterval 30
  ServerAliveCountMax 3
EOF
chmod 600 "${SSH_CONFIG}"
ok "SSH config written: ${SSH_CONFIG}"

# ── 3. Switch git remote from HTTPS to SSH ────────────────────────────────────
log "Switching git remote to SSH..."
cd "${REPO_DIR}"
CURRENT_REMOTE=$(git remote get-url origin 2>/dev/null || echo "none")
NEW_REMOTE="git@github.com:${GITHUB_USER}/${REPO}.git"

if [[ "${CURRENT_REMOTE}" == "${NEW_REMOTE}" ]]; then
  ok "Remote already uses SSH: ${NEW_REMOTE}"
else
  git remote set-url origin "${NEW_REMOTE}"
  ok "Remote changed: ${CURRENT_REMOTE} → ${NEW_REMOTE}"
fi

# ── 4. Update git-sync-github.sh REMOTE_URL to SSH ────────────────────────────
SYNC_SCRIPT="${REPO_DIR}/scripts/git-sync-github.sh"
if grep -q 'REMOTE_URL="https://' "${SYNC_SCRIPT}" 2>/dev/null; then
  sed -i "s|REMOTE_URL=\"https://github.com/${GITHUB_USER}/${REPO}.git\"|REMOTE_URL=\"git@github.com:${GITHUB_USER}/${REPO}.git\"|" "${SYNC_SCRIPT}"
  ok "Updated REMOTE_URL in git-sync-github.sh to SSH"
fi

# ── 5. Print public key and next steps ─────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  ACTION REQUIRED — Add this public key to GitHub                  ║"
echo "╠══════════════════════════════════════════════════════════════════╣"
echo ""
cat "${KEY_FILE}.pub"
echo ""
echo "╠══════════════════════════════════════════════════════════════════╣"
echo "║  Steps:                                                            ║"
echo "║  1. Copy the key above                                             ║"
echo "║  2. Go to https://github.com/settings/keys                         ║"
echo "║  3. Click 'New SSH key'                                             ║"
echo "║  4. Title: 'k8s-platform master node'                              ║"
echo "║  5. Paste the key → Save                                           ║"
echo "║  6. Run: ssh -T git@github.com                                     ║"
echo "║     Expected: 'Hi ${GITHUB_USER}! You have authenticated...'     ║"
echo "║  7. Run: bash scripts/git-sync-github.sh \"chore: switch to SSH\"  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
