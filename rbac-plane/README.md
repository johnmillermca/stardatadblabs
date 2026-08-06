# RBAC Control Plane

Centralized access-control management for **Apache Doris**, **Apache Kafka** (Strimzi), **Apache OpenSearch**, and **Apache Spark** — deployed as a FastAPI service in the `prod` namespace.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                     RBAC Control Plane                               │
│                                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
│  │  rbacctl CLI  │  │  REST API    │  │  Swagger UI              │  │
│  │  (Python)     │  │  FastAPI     │  │  http://…:30850/docs     │  │
│  └──────┬────────┘  └──────┬───────┘  └──────────────────────────┘  │
│         │                  │                                          │
│         └─────────── HTTPS / Bearer JWT ──────────────────────────── │
│                            │                                          │
│  ┌─────────────────────────▼───────────────────────────────────┐     │
│  │              Sync Engine                                     │     │
│  │  hash-delta check → only call adapters when state changed   │     │
│  └────┬──────────┬──────────┬──────────────┬───────────────────┘     │
│       │          │          │              │                           │
│  Doris │    Kafka │  OpenSearch │      Spark │                        │
│  GRANT/ │  KafkaUser │  Security  │  allowlist│                       │
│  REVOKE │  CR + ACLs │  REST API  │  ConfigMap│                       │
│         │          │          │              │                        │
│  ┌──────▼──────────▼──────────▼──────────────▼───────────────┐       │
│  │         Two-layer role cache (zero bottleneck)             │       │
│  │   In-process LRU (10s TTL)  →  Redis (30s TTL)  →  PgSQL  │       │
│  └────────────────────────────────────────────────────────────┘       │
└──────────────────────────────────────────────────────────────────────┘
```

### Why this doesn't bottleneck at scale

| Layer | Latency | Hit rate |
|---|---|---|
| In-process LRU (per replica) | < 1 µs | ~80% of hot users |
| Redis (shared across replicas) | < 1 ms | ~18% of remaining |
| PostgreSQL (source of truth) | ~5 ms | < 2% (cache miss only) |

Role lookups (`GET /api/v1/users/{username}/roles`) serve from cache. The cache is invalidated only when a binding changes — not on every read. With HPA scaling (2–8 replicas), the system handles **thousands of concurrent role checks per second** without touching the database.

---

## Quick Start

### 1. Build and push the image

```bash
bash rbac-plane/scripts/build-and-push.sh
```

### 2. Seed credentials and apply schema

```bash
sudo bash rbac-plane/scripts/seed-rbac-credentials.sh
```

This:
- Generates a master token + JWT secret
- Creates the `rbac` PostgreSQL database and user
- Applies the schema migrations
- Creates the `rbac-plane-credentials` K8s Secret

### 3. Deploy via ArgoCD

```bash
kubectl apply -f argocd-apps/app-rbac-plane.yaml
```

Or deploy Redis + the control plane manually:

```bash
kubectl apply -f rbac-plane/manifests/redis-deployment.yaml
kubectl apply -f rbac-plane/manifests/rbac-plane-deployment.yaml
```

### 4. Install the CLI

```bash
pip install httpx typer rich
alias rbacctl="python3 $(pwd)/rbac-plane/cli/rbacctl.py"
export RBAC_URL=http://192.168.1.50:30850
export RBAC_TOKEN=<master-token-from-seed-output>
```

---

## API Reference (auto-generated)

Open `http://192.168.1.50:30850/docs` for the full interactive Swagger UI.

All endpoints require a `Bearer` token in the `Authorization` header.

| Endpoint | Method | Description |
|---|---|---|
| `/api/v1/auth/token` | POST | Exchange raw token for JWT |
| `/api/v1/auth/tokens` | POST | Create a new API token |
| `/api/v1/users` | GET/POST | List / create users |
| `/api/v1/users/{username}` | GET/PATCH/DELETE | Get / update / delete user |
| `/api/v1/users/{username}/bindings` | GET/POST | List / create role bindings |
| `/api/v1/users/{username}/bindings/{id}` | DELETE | Remove a binding |
| `/api/v1/users/{username}/roles` | GET | Fast lookup (cache-accelerated) |
| `/api/v1/roles` | GET/POST | List / create roles |
| `/api/v1/roles/{id}` | GET/PATCH/DELETE | Get / update / delete role |
| `/api/v1/roles/{id}/permissions/{pid}` | POST/DELETE | Add / remove permission from role |
| `/api/v1/services` | GET | List registered services |
| `/api/v1/services/{svc}/permissions` | GET | List permissions for a service |
| `/api/v1/sync` | POST | Push RBAC state to services |
| `/api/v1/audit` | GET | Query audit log |

---

## CLI Reference

```bash
# ── Users ──────────────────────────────────────────────────
rbacctl user list                          # list all users
rbacctl user create alice --name "Alice"   # create user
rbacctl user bind alice analyst            # bind role (all services)
rbacctl user bind alice etl_writer --service kafka   # service-scoped binding
rbacctl user bind alice spark_user --expires-days 90 # time-limited binding
rbacctl user bindings alice                # show bindings
rbacctl user roles alice                   # effective permissions (from cache)
rbacctl user disable alice                 # disable (cache evicted)
rbacctl user delete alice --yes            # delete user

# ── Roles ──────────────────────────────────────────────────
rbacctl role list                          # list roles
rbacctl role get 1                         # show role permissions
rbacctl role create myrole --display-name "My Role"
rbacctl role perms --service doris         # list available permissions

# ── Sync ───────────────────────────────────────────────────
rbacctl sync run                           # full platform sync
rbacctl sync run --user alice              # sync one user
rbacctl sync run --service doris           # sync all users to Doris
rbacctl sync run --dry-run                 # preview changes

# ── Audit ──────────────────────────────────────────────────
rbacctl audit log                          # last 50 events
rbacctl audit log --actor admin --limit 20

# ── Tokens ─────────────────────────────────────────────────
rbacctl token create ci-pipeline --scopes read,write
rbacctl token revoke ci-pipeline
```

---

## Built-in Roles

| Role | Services | What it allows |
|---|---|---|
| `analyst` | Doris, Kafka, OpenSearch, Spark | Read-only: SELECT, CONSUME, INDEX_READ, VIEW_UI |
| `etl_writer` | Doris, Kafka | Write: SELECT+INSERT+UPDATE+LOAD, PRODUCE+CONSUME |
| `spark_user` | Spark | Submit and kill own jobs, view UI |
| `kafka_consumer` | Kafka | CONSUME + DESCRIBE topics |
| `data_admin` | All | Full admin across all services |

---

## How Sync Works Per Service

### Apache Doris
- Creates the user if missing (`CREATE USER IF NOT EXISTS`)
- Issues `REVOKE ALL ON *.* FROM user` then re-grants from the computed permission set
- Uses `aiomysql` to connect to the FE MySQL port (9030)

### Apache Kafka (Strimzi)
- Creates / updates a `KafkaUser` custom resource (CR)
- Strimzi User Operator reconciles the CR into SCRAM credentials + ACL rules
- ACL rules are generated from the permission set (PRODUCE → Write ACL, CONSUME → Read+Describe ACL, etc.)

### Apache OpenSearch
- Creates the user via the Security REST API (`/_plugins/_security/api/internalusers/`)
- Creates/updates rbac_* roles (`/_plugins/_security/api/roles/`)
- Updates role mappings (`/_plugins/_security/api/rolesmapping/`)
- Auth is via Kerberos SPNEGO (password set to random, irrelevant)

### Apache Spark
- Updates `spark-rbac-allowlist` ConfigMap in the `prod` namespace
- The `krb-spark-guard` sidecar reads this ConfigMap and enforces it on the RPC proxy

---

## Zero-Bottleneck Design

The `GET /api/v1/users/{username}/roles` endpoint is designed for use on the **hot path** by sidecars (e.g. a future Kerberos guard or service interceptor):

1. **In-process LRU** (10 s TTL, 10k entries per replica) — zero I/O, sub-microsecond
2. **Redis** (30 s TTL) — shared across replicas, ~0.5 ms
3. **PostgreSQL** — only on cache miss (~2% of requests)

Cache is invalidated immediately on any binding change or user disable. This means:
- A role grant takes effect within 30 seconds platform-wide (Redis TTL)
- Within 10 seconds on the replica that processed the change
- Via `rbacctl sync run` — immediately pushed to services

---

## Files

```
rbac-plane/
├── api/
│   ├── main.py                # FastAPI app
│   ├── config.py              # Settings (env vars)
│   ├── models.py              # SQLAlchemy ORM models
│   ├── schemas.py             # Pydantic request/response models
│   ├── database.py            # Engine, sessions, TTL-LRU+Redis cache
│   ├── middleware/
│   │   ├── auth.py            # JWT + API token auth
│   │   └── audit.py           # Async audit log writer
│   ├── routers/
│   │   ├── auth.py            # /auth — token exchange + management
│   │   ├── users.py           # /users — CRUD + bindings + role lookup
│   │   ├── roles.py           # /roles — CRUD + permission management
│   │   ├── services.py        # /services — read-only registry
│   │   ├── sync.py            # /sync — push state to services
│   │   └── audit.py           # /audit — log query
│   └── adapters/
│       ├── doris.py           # GRANT/REVOKE via aiomysql
│       ├── kafka.py           # KafkaUser CRs via kubernetes_asyncio
│       ├── opensearch.py      # Security REST API via httpx
│       └── spark.py           # allowlist ConfigMap via kubernetes_asyncio
├── cli/
│   └── rbacctl.py             # Typer CLI
├── migrations/
│   ├── 001_initial_schema.sql # Tables + seed data
│   └── 002_seed_role_permissions.sql
├── manifests/
│   ├── rbac-plane-deployment.yaml
│   └── redis-deployment.yaml
├── docker/
│   └── Dockerfile
├── scripts/
│   ├── seed-rbac-credentials.sh
│   └── build-and-push.sh
└── README.md
```
