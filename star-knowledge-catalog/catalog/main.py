"""
Star Knowledge Catalog — FastAPI application entrypoint.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .config import get_settings
from .database import get_engine
from .models import Base
from .routers import auth, classifications, glossary, algorithms, policies, columns, masking, exceptions, governance_switch

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create all catalog tables on startup (idempotent)."""
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        log.info("Star Knowledge Catalog: schema ready")
    except Exception as exc:
        log.error("Star Knowledge Catalog: schema init failed: %s", exc)
    yield
    engine = get_engine()
    await engine.dispose()


def create_app() -> FastAPI:
    s = get_settings()

    app = FastAPI(
        title=s.app_title,
        version=s.app_version,
        description=(
            "**Star Knowledge Catalog** — IBM Knowledge Catalog-inspired data "
            "governance for the k8s-platform.\n\n"
            "Provides:\n"
            "- **Data Classifications** — PII, PCI, PHI, CONFIDENTIAL sensitivity tiers\n"
            "- **Business Glossary** — curated terms with auto-detection patterns\n"
            "- **Masking Algorithms** — Doris-native SQL masking expressions\n"
            "- **Masking Policies** — bind classifications/terms to algorithms\n"
            "- **Column Tags** — auto-scan or manually tag Doris columns\n"
            "- **Masked Views** — pre-computed Doris views that apply masking at query time\n"
            "- **Role Routing** — integrates with RBAC Control Plane to route analyst "
            "users to masked views and privileged roles to base tables\n\n"
            "**Quick start:**\n"
            "1. `POST /api/v1/auth/token` with your master token to get a JWT.\n"
            "2. `POST /api/v1/columns/scan` to auto-classify a Doris database.\n"
            "3. `POST /api/v1/masking/apply` to generate and deploy masked views.\n"
            "4. `POST /api/v1/masking/query` to get a role-aware SELECT statement.\n"
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
        debug=s.debug,
    )

    # ── CORS ───────────────────────────────────────────────
    origins = [o.strip() for o in s.cors_origins.split(",")]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Global error handler ───────────────────────────────
    @app.exception_handler(Exception)
    async def _global_handler(request: Request, exc: Exception):
        log.exception("Unhandled exception: %s", exc)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"},
        )

    # ── Health ─────────────────────────────────────────────
    @app.get("/health", tags=["System"], summary="Health probe")
    async def health():
        return {"status": "ok", "version": s.app_version, "service": "star-knowledge-catalog"}

    @app.get("/", tags=["System"], include_in_schema=False)
    async def root():
        return {"message": "Star Knowledge Catalog", "docs": "/docs"}

    # ── Routers ────────────────────────────────────────────
    prefix = s.api_prefix
    app.include_router(auth.router,              prefix=prefix)
    app.include_router(classifications.router,   prefix=prefix)
    app.include_router(glossary.router,          prefix=prefix)
    app.include_router(algorithms.router,        prefix=prefix)
    app.include_router(policies.router,          prefix=prefix)
    app.include_router(columns.router,           prefix=prefix)
    app.include_router(masking.router,           prefix=prefix)
    app.include_router(exceptions.router,        prefix=prefix)
    app.include_router(governance_switch.router, prefix=prefix)

    return app


app = create_app()
