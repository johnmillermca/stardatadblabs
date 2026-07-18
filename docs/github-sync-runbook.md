# GitHub Sync Runbook — k8s-platform

> **Repo:** `https://github.com/johnmillermca/stardatadblabs`  
> **Branch:** `main`  
> **Path inside repo:** `k8s-platform/`  
> **ArgoCD watches:** all `argocd-apps/`, `helm/`, `manifests/` changes

---

## 1. One-time Setup (already done)

```bash
# Verify remote is set correctly
git remote -v
# origin  git@github.com:johnmillermca/stardatadblabs.git (fetch)
# origin  git@github.com:johnmillermca/stardatadblabs.git (push)

# Verify SSH key is registered with GitHub
ssh -T git@github.com
# Hi johnmillermca! You've successfully authenticated...
```

---

## 2. Day-to-Day Sync Workflow

### Push changes to GitHub (triggers ArgoCD auto-sync)

```bash
cd ~/k8s-platform

# 1. Check what changed
git status
git diff

# 2. Stage all changes
git add -A

# 3. Commit with a descriptive message
git commit -m "feat: deploy kafka HA 3-broker + private registry images"

# 4. Push to main — ArgoCD detects within 3 minutes (default poll interval)
git push origin main
```

### Force ArgoCD to sync immediately (without waiting for poll)

```bash
# Sync a single app
argocd app sync <app-name> --server 192.168.1.50:30443 --insecure

# Sync all apps in the platform project
argocd app sync -l app.kubernetes.io/part-of=platform \
  --server 192.168.1.50:30443 --insecure

# Or via kubectl
kubectl patch application <app-name> -n argocd \
  --type merge -p '{"operation":{"sync":{"revision":"HEAD"}}}'
```

---

## 3. Recommended Commit Message Format

```
<type>(<scope>): <summary>

type  : feat | fix | chore | refactor | docs
scope : kafka | spark | monitoring | all | helm/<name>

Examples:
  feat(kafka): enable 3-broker HA with private registry images
  fix(opensearch): increase startup probe failureThreshold to 40
  chore(helm): bump bitnami/kafka chart to 32.5.0
  docs: update github-sync-runbook
```

---

## 4. Branching Strategy

| Branch | Purpose |
|---|---|
| `main` | Production — ArgoCD watches this |
| `feature/<name>` | New features / app additions |
| `fix/<name>` | Bug fixes |

```bash
# Create a feature branch
git checkout -b feature/add-ranger-deployment

# ... make changes ...
git add -A && git commit -m "feat(ranger): add Apache Ranger manifests"
git push origin feature/add-ranger-deployment

# Merge to main when ready (via PR or directly)
git checkout main
git merge feature/add-ranger-deployment
git push origin main
```

---

## 5. Pulling Latest Changes (before editing)

Always pull before editing to avoid conflicts:

```bash
cd ~/k8s-platform
git pull origin main
```

---

## 6. Undoing a Bad Deployment

### Revert last commit (keeps history clean)

```bash
git revert HEAD --no-edit
git push origin main
# ArgoCD auto-syncs and rolls back the cluster state
```

### Hard reset to a specific revision

```bash
# Find the good commit hash
git log --oneline -10

# Reset (DESTRUCTIVE — discards commits after <hash>)
git reset --hard <hash>
git push origin main --force-with-lease
```

### ArgoCD rollback to a previous revision (without touching Git)

```bash
argocd app rollback <app-name> <revision-number> \
  --server 192.168.1.50:30443 --insecure
```

---

## 7. Checking Sync Status

```bash
# All apps
argocd app list --server 192.168.1.50:30443 --insecure

# Detailed status for one app
argocd app get <app-name> --server 192.168.1.50:30443 --insecure

# Watch live
watch -n5 "kubectl get applications -n argocd \
  -o custom-columns='NAME:.metadata.name,SYNC:.status.sync.status,HEALTH:.status.health.status'"
```

---

## 8. ArgoCD Webhook (optional — instant sync on push)

Instead of polling every 3 minutes, configure a GitHub webhook so ArgoCD syncs
the moment you push:

1. In GitHub → repo Settings → Webhooks → Add webhook:
   - **Payload URL:** `https://192.168.1.50:30443/api/webhook`
   - **Content type:** `application/json`
   - **Events:** `push`

2. In ArgoCD, add the secret (optional, for HMAC validation):
   ```bash
   kubectl patch secret argocd-secret -n argocd \
     --type merge \
     -p '{"stringData":{"webhook.github.secret":"<your-webhook-secret>"}}'
   ```

---

## 9. File Map — What ArgoCD Watches

| Path | Used by |
|---|---|
| `argocd-apps/app-prod.yaml` | All prod workloads (wave -20 → +9) |
| `argocd-apps/app-monitoring.yaml` | Prometheus, Grafana, MCP servers |
| `argocd-apps/app-project-platform.yaml` | AppProject permissions |
| `helm/*/values.yaml` | Referenced by ArgoCD `sources[].helm.valueFiles` |
| `manifests/*/` | Raw YAML for apps without Helm charts |

> **Do NOT apply legacy files** in `argocd-apps/` (app-kafka.yaml, app-spark.yaml, etc.) —
> see `argocd-apps/LEGACY-README.md`. All apps are managed by `app-prod.yaml`.
