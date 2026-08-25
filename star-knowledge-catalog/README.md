# Star Knowledge Catalog

**IBM Knowledge Catalog-inspired data governance for the k8s-platform.**

Star Knowledge Catalog is a standalone FastAPI microservice that provides:

| Capability | Description |
|---|---|
| **Data Classifications** | Sensitivity tiers — PII, PCI, PHI, CONFIDENTIAL, PUBLIC |
| **Business Glossary** | Curated terms with keyword patterns for auto-detection |
| **Masking Algorithms** | Named Doris-native SQL masking expressions |
| **Masking Policies** | Bind classifications / glossary terms → algorithms |
| **Column Tags** | Auto-scan or manually tag every Doris column |
| **Masked Views** | Auto-generated Doris views with masking at query time |
| **Role Routing** | Integrates with RBAC Control Plane — analyst → masked view, admin → base table |

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│               Star Knowledge Catalog API                 │
│           FastAPI  ·  PostgreSQL  ·  Redis               │
│                                                          │
│  ┌───────────────┐   ┌───────────────┐                   │
│  │ Classifications│   │    Glossary   │                   │
│  └───────┬───────┘   └───────┬───────┘                   │
│          │                   │                           │
│  ┌───────▼───────────────────▼───────┐                   │
│  │          Masking Policies          │  ← priority rules│
│  └───────────────────┬───────────────┘                   │
│                      │                                   │
│  ┌───────────────────▼──────────────────────────────┐    │
│  │               Column Tags                         │    │
│  │  Auto-classify (patterns) + Manual override       │    │
│  └───────────────────┬──────────────────────────────┘    │
│                      │                                   │
│  ┌───────────────────▼──────────────────────────────┐    │
│  │           Masking Engine                          │    │
│  │  Generates CREATE OR REPLACE VIEW DDL in Doris    │    │
│  │  Doris vectorised engine handles masking natively │    │
│  └───────────────────┬──────────────────────────────┘    │
│                      │                                   │
│  ┌───────────────────▼──────────────────────────────┐    │
│  │           RBAC Client                             │    │
│  │  Calls rbac-plane /users/{username}/roles         │    │
│  │  Routes analyst → masked_view                     │    │
│  │  Routes admin  → base_table                       │    │
│  └──────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
         ↕                           ↕
  PostgreSQL                      Apache Doris
  (star_catalog DB)               (governance_demo DB)
```

---

## Quick Start

### 1. Bootstrap PostgreSQL

```bash
# Create database and user
psql -h 192.168.1.50 -p 30532 -U postgres <<'SQL'
CREATE DATABASE star_catalog;
CREATE USER star_catalog WITH PASSWORD 'changeme';
GRANT ALL PRIVILEGES ON DATABASE star_catalog TO star_catalog;
SQL

# Run schema + seed migration
psql -h 192.168.1.50 -p 30532 -U star_catalog -d star_catalog \
  -f migrations/001_schema_and_seed.sql
```

### 2. Bootstrap Doris

```bash
mysql -h 192.168.1.50 -P 30090 -u root \
  < doris/001_create_schema.sql

mysql -h 192.168.1.50 -P 30090 -u root \
  < doris/002_seed_data.sql

# Optional — pre-built views (or let the API generate them)
mysql -h 192.168.1.50 -P 30090 -u root \
  < doris/003_masked_views.sql
```

### 3. Deploy to Kubernetes

```bash
# Build and push image
bash docker/build-and-push.sh 1.0.0

# Create credentials secret
kubectl create secret generic star-catalog-credentials \
  --namespace prod \
  --from-literal=PG_PASSWORD=changeme \
  --from-literal=DORIS_ADMIN_PASSWORD='' \
  --from-literal=MASTER_TOKEN=changeme-catalog-master-token \
  --from-literal=JWT_SECRET=changeme-catalog-jwt-secret-min-32-chars! \
  --from-literal=RBAC_PLANE_TOKEN=changeme-rbac-master-token

kubectl apply -f manifests/star-catalog-deployment.yaml
```

API available at: `http://192.168.1.50:30860/docs`

---

## Core Workflows

### Auto-classify a Doris database

```bash
# Get a JWT
TOKEN=$(curl -s -X POST http://192.168.1.50:30860/api/v1/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"token":"changeme-catalog-master-token"}' | jq -r .access_token)

# Run auto-classification scan
curl -s -X POST http://192.168.1.50:30860/api/v1/columns/scan \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","dry_run":false}' | jq .
```

### Apply masked views

```bash
curl -s -X POST http://192.168.1.50:30860/api/v1/masking/apply \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"doris_database":"governance_demo","dry_run":false}' | jq .
```

### Get role-aware query for a user

```bash
curl -s -X POST http://192.168.1.50:30860/api/v1/masking/query \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "username": "alice",
    "doris_database": "governance_demo",
    "doris_table": "customers",
    "limit": 100
  }' | jq .
```

Response for an `analyst` role user:
```json
{
  "username": "alice",
  "role": "analyst",
  "target": "masked_view",
  "sql": "SELECT *\nFROM `governance_demo`.`customers_masked`\nLIMIT 100;",
  "columns_masked": ["full_name","email","phone_number","date_of_birth","national_id","street_address","ip_address","salary"],
  "columns_clear": [],
  "note": "Masked access: role 'analyst' is routed to view 'customers_masked'."
}
```

---

## File Layout

```
star-knowledge-catalog/
├── catalog/
│   ├── main.py              FastAPI application entrypoint
│   ├── config.py            Pydantic settings (env vars)
│   ├── database.py          PostgreSQL engine + 3-layer cache
│   ├── models.py            SQLAlchemy ORM models
│   ├── schemas.py           Pydantic request/response schemas
│   ├── middleware/
│   │   └── auth.py          JWT bearer middleware
│   ├── routers/
│   │   ├── auth.py          POST /auth/token
│   │   ├── classifications.py  CRUD /classifications
│   │   ├── glossary.py         CRUD /glossary
│   │   ├── algorithms.py       CRUD /algorithms
│   │   ├── policies.py         CRUD /policies
│   │   ├── columns.py          CRUD /columns + POST /columns/scan
│   │   ├── masking.py          POST /masking/apply · GET /masking/views · POST /masking/query
│   │   └── exceptions.py       CRUD /exceptions
│   └── engine/
│       ├── classifier.py    Auto column classification (pattern matching)
│       ├── masking.py       Doris DDL generator + view applicator
│       └── rbac_client.py   RBAC Control Plane HTTP client
├── migrations/
│   └── 001_schema_and_seed.sql  PostgreSQL schema + all seed data
├── doris/
│   ├── 001_create_schema.sql   governance_demo database + tables
│   ├── 002_seed_data.sql       20 customers, 40 orders, 40 payments, 10 products
│   └── 003_masked_views.sql    Pre-built masked views (bootstrap reference)
├── manifests/
│   └── star-catalog-deployment.yaml  K8s Deployment + Service + HPA
├── docker/
│   ├── Dockerfile
│   └── build-and-push.sh
└── requirements.txt
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PG_HOST` | `postgresql.prod.svc.cluster.local` | PostgreSQL host |
| `PG_DB` | `star_catalog` | PostgreSQL database |
| `PG_USER` | `star_catalog` | PostgreSQL user |
| `PG_PASSWORD` | *(secret)* | PostgreSQL password |
| `REDIS_URL` | `redis://redis.prod.svc.cluster.local:6379/1` | Redis (DB index 1) |
| `CACHE_TTL` | `60` | Cache TTL in seconds |
| `DORIS_HOST` | `doris-fe.prod.svc.cluster.local` | Doris FE host |
| `DORIS_PORT` | `9030` | Doris MySQL port |
| `DORIS_ADMIN_USER` | `root` | Doris admin user |
| `DORIS_ADMIN_PASSWORD` | *(secret)* | Doris admin password |
| `DORIS_DEMO_DATABASE` | `governance_demo` | Doris database to govern |
| `MASKED_VIEW_SUFFIX` | `_masked` | Suffix for masked view names |
| `RBAC_PLANE_URL` | `http://rbac-plane.prod.svc.cluster.local:8080` | RBAC Control Plane base URL |
| `RBAC_PLANE_TOKEN` | *(secret)* | Token for RBAC API calls |
| `MASTER_TOKEN` | *(secret)* | Bootstrap admin token |
| `JWT_SECRET` | *(secret)* | JWT signing secret |
| `AUTO_CLASSIFY_THRESHOLD` | `0.70` | Minimum score (0–1) to auto-tag a column |

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/auth/token` | Exchange API token for JWT |
| `GET/POST/PATCH/DELETE` | `/api/v1/classifications` | Data classification CRUD |
| `GET/POST/PATCH/DELETE` | `/api/v1/glossary` | Glossary term CRUD |
| `GET/POST/PATCH/DELETE` | `/api/v1/algorithms` | Masking algorithm CRUD |
| `GET/POST/PATCH/DELETE` | `/api/v1/policies` | Masking policy CRUD |
| `GET/POST/DELETE` | `/api/v1/columns` | Column tag CRUD |
| `POST` | `/api/v1/columns/scan` | Auto-classify columns in a Doris database |
| `POST` | `/api/v1/masking/apply` | Generate and apply masked Doris views |
| `GET` | `/api/v1/masking/views` | List applied view manifests |
| `POST` | `/api/v1/masking/query` | Role-aware SELECT planner |
| `GET/POST/DELETE` | `/api/v1/exceptions` | Role masking exception CRUD |
| `GET` | `/health` | Health probe |
| `GET` | `/docs` | Swagger UI |

See runbook: [`docs/runbooks/runbook-16-data-governance-masking.md`](../docs/runbooks/runbook-16-data-governance-masking.md)
