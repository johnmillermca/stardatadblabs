#!/usr/bin/env bash
# =============================================================================
# git-sync-github.sh
# Push the entire k8s-platform to GitHub
#   Remote  : git@github.com:johnmillermca/stardatadblabs.git  (SSH — preferred)
#             https://github.com/johnmillermca/stardatadblabs.git (HTTPS fallback)
#   Branch  : main
#
# Usage: bash scripts/git-sync-github.sh [commit-message]
# Safe to run repeatedly — creates git repo if not initialised.
#
# First-time SSH setup:
#   bash scripts/master/setup-github-ssh.sh
# =============================================================================
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
GITHUB_USER="johnmillermca"
REPO_NAME="stardatadblabs"
SSH_REMOTE="git@github.com:${GITHUB_USER}/${REPO_NAME}.git"
HTTPS_REMOTE="https://github.com/${GITHUB_USER}/${REPO_NAME}.git"
BRANCH="main"
COMMIT_MSG="${1:-"chore: auto-sync k8s-platform $(date '+%Y-%m-%d %H:%M:%S')"}"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "  ✓ $*"; }
warn() { echo "  ⚠ $*"; }
die()  { echo "[ERROR] $*" >&2; exit 1; }

cd "${REPO_DIR}"

# ── Detect which transport to use ─────────────────────────────────────────────
# Prefer SSH if the key exists and ssh-agent/key is loadable
SSH_KEY="${HOME}/.ssh/github_stardatadblabs"
if [[ -f "${SSH_KEY}" ]]; then
  REMOTE_URL="${SSH_REMOTE}"
  log "Using SSH remote (key: ${SSH_KEY})"
else
  REMOTE_URL="${HTTPS_REMOTE}"
  warn "SSH key not found at ${SSH_KEY} — using HTTPS remote"
  warn "For reliable pushes run: bash scripts/master/setup-github-ssh.sh"
fi

# ── Ensure git is initialised ─────────────────────────────────────────────────
if [[ ! -d .git ]]; then
  log "Initialising git repository in ${REPO_DIR}..."
  git init -b "${BRANCH}"
  ok "git init"
fi

# ── Configure user identity (needed for commit) ───────────────────────────────
git config user.email "platform@stardatadblabs.local"
git config user.name  "k8s-platform bot"

# ── Ensure remote is configured to the right URL ─────────────────────────────
if ! git remote get-url origin &>/dev/null; then
  log "Adding remote 'origin' → ${REMOTE_URL}"
  git remote add origin "${REMOTE_URL}"
  ok "remote added"
else
  CURRENT=$(git remote get-url origin)
  # Accept either SSH or HTTPS as-is if already pointing at the right repo;
  # only override if pointing somewhere else entirely
  if [[ "${CURRENT}" != "${SSH_REMOTE}" && "${CURRENT}" != "${HTTPS_REMOTE}" ]]; then
    log "Updating remote 'origin': ${CURRENT} → ${REMOTE_URL}"
    git remote set-url origin "${REMOTE_URL}"
    ok "remote updated"
  else
    # Always prefer SSH when key exists
    if [[ -f "${SSH_KEY}" && "${CURRENT}" == "${HTTPS_REMOTE}" ]]; then
      git remote set-url origin "${SSH_REMOTE}"
      ok "Switched remote from HTTPS → SSH"
    else
      ok "Remote already configured: ${CURRENT}"
    fi
  fi
fi

# ── Ensure .gitignore is present ─────────────────────────────────────────────
if [[ ! -f .gitignore ]]; then
cat > .gitignore <<'GITIGNORE'
# OpenBao / Vault init key (contains unseal keys + root token — NEVER commit)
openbao-init-keys.json
/root/openbao-init-keys.json

# Kubernetes secret dumps (contain base64-encoded secrets)
all-secrets.yaml
**/all-secrets.yaml

# Downloaded JARs (large binaries — tracked by version in Dockerfile)
jars/*.jar

# SSH keys — never commit private keys
*.pem
*.key
**/*_rsa
**/*_ed25519
!**/*_ed25519.pub

# OS / editor
.DS_Store
*.swp
*.swo
.idea/
.vscode/

# Helm dependency downloads (re-fetchable via helm dep update)
**/charts/
**/Chart.lock

# iptables save file (contains host-specific rules)
/etc/iptables-gateway.rules
GITIGNORE
  ok ".gitignore created"
fi

# ── Stage all changes ─────────────────────────────────────────────────────────
log "Staging all changes..."
git add -A
STATUS=$(git status --short)
if [[ -z "${STATUS}" ]]; then
  log "Nothing to commit — working tree clean."
  CURRENT_REMOTE=$(git remote get-url origin)
  echo ""
  echo "  Repository is up to date."
  echo "  Remote : ${CURRENT_REMOTE}"
  echo "  Branch : ${BRANCH}"
  echo "  Tip    : $(git log --oneline -1 2>/dev/null || echo 'no commits')"
  exit 0
fi

log "Changed files:"
echo "${STATUS}"

# ── Commit ───────────────────────────────────────────────────────────────────
log "Committing: ${COMMIT_MSG}"
git commit -m "${COMMIT_MSG}"
ok "committed: $(git log --oneline -1)"

# ── Push with retry ──────────────────────────────────────────────────────────
PUSH_REMOTE=$(git remote get-url origin)
log "Pushing to ${PUSH_REMOTE} (branch: ${BRANCH})..."

push_attempt() {
  git push -u origin "${BRANCH}" 2>&1
}

if push_attempt; then
  ok "pushed to GitHub"
else
  warn "First push attempt failed — retrying in 5 seconds..."
  sleep 5
  if push_attempt; then
    ok "pushed to GitHub (2nd attempt)"
  else
    warn "Second attempt failed — trying with --force (safe for first-time setup)..."
    git push -u origin "${BRANCH}" --force 2>&1 && ok "force-pushed to GitHub" || \
      die "Push failed after 3 attempts. Check network: curl -v https://github.com"
  fi
fi

FINAL_REMOTE=$(git remote get-url origin)
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
printf "║  Remote : %-50s║\n" "${FINAL_REMOTE:0:48}"
printf "║  Branch : %-50s║\n" "${BRANCH}"
printf "║  Commit : %-50s║\n" "${COMMIT_MSG:0:48}"
echo "╚══════════════════════════════════════════════════════════════╝"
log "=== GitHub sync complete ==="
