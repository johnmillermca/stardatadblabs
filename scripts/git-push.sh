#!/usr/bin/env bash
# =============================================================================
# git-push.sh
# Commits and pushes the entire k8s-platform to GitHub.
#
# Usage:
#   bash scripts/git-push.sh                        # auto commit message
#   bash scripts/git-push.sh "feat: add Kafka"      # custom message
#   bash scripts/git-push.sh --dry-run              # preview only
# =============================================================================
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REMOTE_URL="https://github.com/johnmillermca/stardatadblabs"
GIT_USER_NAME="StarDataDB Labs"
GIT_USER_EMAIL="admin@stardatadblabs.local"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
warn() { echo "[WARN]  $*"; }
die()  { echo "[ERROR] $*" >&2; exit 1; }

# ── Parse args ────────────────────────────────────────────────────────────────
DRY_RUN=false
COMMIT_MSG=""
for arg in "$@"; do
  case "${arg}" in
    --dry-run) DRY_RUN=true ;;
    --*)       die "Unknown flag: ${arg}  (valid: --dry-run)" ;;
    *)         COMMIT_MSG="${arg}" ;;
  esac
done
[[ -z "${COMMIT_MSG}" ]] && COMMIT_MSG="Platform update $(date '+%Y-%m-%d %H:%M')"

cd "${REPO_DIR}"

# ── Init git if needed ────────────────────────────────────────────────────────
if [[ ! -d .git ]]; then
  log "Initialising git repository..."
  git init
  git checkout -b main 2>/dev/null || true
fi

# ── Configure user identity ───────────────────────────────────────────────────
git config user.name  "${GIT_USER_NAME}"
git config user.email "${GIT_USER_EMAIL}"

# ── Set remote ────────────────────────────────────────────────────────────────
if git remote get-url origin &>/dev/null; then
  CURRENT_REMOTE=$(git remote get-url origin)
  if [[ "${CURRENT_REMOTE}" != "${REMOTE_URL}" ]]; then
    warn "Remote 'origin' points to ${CURRENT_REMOTE}, updating to ${REMOTE_URL}"
    git remote set-url origin "${REMOTE_URL}"
  fi
else
  log "Adding remote origin: ${REMOTE_URL}"
  git remote add origin "${REMOTE_URL}"
fi

# ── Security guard: never commit secret files ─────────────────────────────────
DANGEROUS_PATTERNS=("openbao-init-keys.json" "init-keys.json" ".env" "credentials.yaml")
STAGED_DANGEROUS=()
for pat in "${DANGEROUS_PATTERNS[@]}"; do
  while IFS= read -r f; do
    [[ -n "${f}" ]] && STAGED_DANGEROUS+=("${f}")
  done < <(git ls-files --others --exclude-standard 2>/dev/null | grep -i "${pat}" || true)
done

if [[ ${#STAGED_DANGEROUS[@]} -gt 0 ]]; then
  echo ""
  echo "  ⛔ SECURITY STOP — The following sensitive files are untracked and"
  echo "     would be committed. They should be in .gitignore."
  for f in "${STAGED_DANGEROUS[@]}"; do echo "     - ${f}"; done
  die "Add them to .gitignore before pushing."
fi

# ── Write .gitignore if missing ───────────────────────────────────────────────
if [[ ! -f .gitignore ]]; then
  log "Creating .gitignore..."
  cat > .gitignore << 'GITIGNORE'
# Secrets — never commit
*-init-keys.json
openbao-init-keys.json
*.key
*.pem
*-credentials.yaml
secrets/
.env
*.env
# Backups
/opt/k8s-backups/
# OS / editor
.DS_Store
*.swp
*~
# Temp
tmp/
.tmp/
GITIGNORE
fi

# ── Stage all changes ─────────────────────────────────────────────────────────
git add -A

# ── Dry-run: show what would be committed ─────────────────────────────────────
if [[ "${DRY_RUN}" == true ]]; then
  echo ""
  log "DRY RUN — would commit with message: \"${COMMIT_MSG}\""
  echo ""
  git diff --cached --stat || echo "  (nothing staged)"
  echo ""
  log "Run without --dry-run to actually push."
  exit 0
fi

# ── Commit (skip if nothing staged) ──────────────────────────────────────────
if git diff --cached --quiet; then
  log "Nothing to commit — working tree clean."
else
  git commit -m "${COMMIT_MSG}"
  log "Committed: ${COMMIT_MSG}"
fi

# ── Detect branch ─────────────────────────────────────────────────────────────
BRANCH=$(git branch --show-current 2>/dev/null || echo "main")
[[ -z "${BRANCH}" ]] && BRANCH="main"

# ── Push ──────────────────────────────────────────────────────────────────────
log "Pushing to origin/${BRANCH}..."
git push origin "${BRANCH}" && log "Push successful: ${REMOTE_URL}" \
  || die "Push failed. Check credentials (use a GitHub PAT via HTTPS or SSH key)."
