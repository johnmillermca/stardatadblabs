#!/usr/bin/env bash
# =============================================================================
# git-sync-github.sh
# Push the entire k8s-platform to GitHub
#   Remote  : https://github.com/johnmillermca/stardatadblabs
#   Branch  : main
#
# Usage: bash scripts/git-sync-github.sh [commit-message]
# Safe to run repeatedly — creates git repo if not initialised.
# Requires: git, ssh key or personal access token configured for the remote.
# =============================================================================
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REMOTE_URL="https://github.com/johnmillermca/stardatadblabs.git"
BRANCH="main"
COMMIT_MSG="${1:-"chore: auto-sync k8s-platform $(date '+%Y-%m-%d %H:%M:%S')"}"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "  ✓ $*"; }
die()  { echo "[ERROR] $*" >&2; exit 1; }

cd "${REPO_DIR}"

# ── Ensure git is initialised ──────────────────────────────────────────────────
if [[ ! -d .git ]]; then
  log "Initialising git repository in ${REPO_DIR}..."
  git init -b "${BRANCH}"
  ok "git init"
fi

# ── Ensure remote is configured ───────────────────────────────────────────────
if ! git remote get-url origin &>/dev/null; then
  log "Adding remote 'origin' → ${REMOTE_URL}"
  git remote add origin "${REMOTE_URL}"
  ok "remote added"
else
  CURRENT=$(git remote get-url origin)
  if [[ "${CURRENT}" != "${REMOTE_URL}" ]]; then
    log "Updating remote 'origin': ${CURRENT} → ${REMOTE_URL}"
    git remote set-url origin "${REMOTE_URL}"
    ok "remote updated"
  fi
fi

# ── Ensure .gitignore is present ──────────────────────────────────────────────
if [[ ! -f .gitignore ]]; then
cat > .gitignore <<'EOF'
# OpenBao / Vault init key (contains unseal keys + root token — NEVER commit)
openbao-init-keys.json
/root/openbao-init-keys.json

# Kubernetes secret dumps (contain base64-encoded secrets)
all-secrets.yaml
**/all-secrets.yaml

# Downloaded JARs (large binaries — tracked by filename in Dockerfile)
jars/*.jar

# OS / editor
.DS_Store
*.swp
*.swo
.idea/
.vscode/

# Helm dependency lock caches
**/charts/
**/Chart.lock
EOF
  ok ".gitignore created"
fi

# ── Stage all changes ──────────────────────────────────────────────────────────
log "Staging all changes..."
git add -A
STATUS=$(git status --short)
if [[ -z "${STATUS}" ]]; then
  log "Nothing to commit — working tree clean."
  exit 0
fi

log "Changed files:"
echo "${STATUS}"

# ── Commit ────────────────────────────────────────────────────────────────────
log "Committing: ${COMMIT_MSG}"
git -c user.email="platform@stardatadblabs.local" \
    -c user.name="k8s-platform bot" \
    commit -m "${COMMIT_MSG}"
ok "committed"

# ── Push ──────────────────────────────────────────────────────────────────────
log "Pushing to ${REMOTE_URL} (branch: ${BRANCH})..."
git push -u origin "${BRANCH}" 2>&1 || {
  log "Regular push failed — attempting force push on first-time setup..."
  git push -u origin "${BRANCH}" --force
}
ok "pushed to GitHub"

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
printf "║  Remote : %-50s║\n" "${REMOTE_URL}"
printf "║  Branch : %-50s║\n" "${BRANCH}"
printf "║  Commit : %-50s║\n" "${COMMIT_MSG:0:48}"
echo "╚══════════════════════════════════════════════════════════════╝"
log "=== GitHub sync complete ==="
