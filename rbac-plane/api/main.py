"""
RBAC Control Plane — FastAPI application entrypoint.
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
from .routers import auth, audit, roles, services, sync, users

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run DB migrations on startup (idempotent)."""
    s = get_settings()
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        log.info("Database schema ready")
    except Exception as exc:
        log.error("Database init failed: %s", exc)
    yield
    # Shutdown: close engine
    engine = get_engine()
    await engine.dispose()


def create_app() -> FastAPI:
    s = get_settings()

    app = FastAPI(
        title=s.app_title,
        version=s.app_version,
        description=(
            "Centralized RBAC Control Plane for Apache Doris, Apache Kafka "
            "(Strimzi), Apache OpenSearch, and Apache Spark.\n\n"
            "**Quick start:**\n"
            "1. `POST /api/v1/auth/token` with your master token to get a JWT.\n"
            "2. `POST /api/v1/users` to register a user.\n"
            "3. `POST /api/v1/users/{username}/bindings` to assign a role.\n"
            "4. `POST /api/v1/sync` to push the role to all services.\n"
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
        debug=s.debug,
    )

    # ── CORS ──────────────────────────────────────────────
    origins = [o.strip() for o in s.cors_origins.split(",")]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Global error handler ──────────────────────────────
    @app.exception_handler(Exception)
    async def _global_handler(request: Request, exc: Exception):
        log.exception("Unhandled exception: %s", exc)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"},
        )

    # ── Health check ──────────────────────────────────────
    @app.get("/health", tags=["System"], summary="Health probe")
    async def health():
        return {"status": "ok", "version": s.app_version}

    @app.get("/", tags=["System"], include_in_schema=False)
    async def root():
        return {"message": "RBAC Control Plane", "docs": "/docs"}

    # ── Routers ───────────────────────────────────────────
    prefix = s.api_prefix
    app.include_router(auth.router,     prefix=prefix)
    app.include_router(users.router,    prefix=prefix)
    app.include_router(roles.router,    prefix=prefix)
    app.include_router(services.router, prefix=prefix)
    app.include_router(sync.router,     prefix=prefix)
    app.include_router(audit.router,    prefix=prefix)

    return app


app = create_app()
