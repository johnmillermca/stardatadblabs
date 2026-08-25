"""
Star Knowledge Catalog — central configuration.
All values come from environment variables (injected via K8s Secret / OpenBao).
"""
from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── API server ──────────────────────────────────────────
    app_title: str = "Star Knowledge Catalog"
    app_version: str = "1.0.0"
    debug: bool = False
    api_prefix: str = "/api/v1"
    cors_origins: str = "*"

    # ── PostgreSQL (catalog metadata store) ─────────────────
    pg_host: str = "postgresql.prod.svc.cluster.local"
    pg_port: int = 5432
    pg_db: str = "star_catalog"
    pg_user: str = "star_catalog"
    pg_password: str = "changeme"
    pg_pool_min: int = 2
    pg_pool_max: int = 20

    @property
    def pg_dsn(self) -> str:
        return (
            f"postgresql+asyncpg://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        )

    # ── Redis (policy & classification cache) ───────────────
    redis_url: str = "redis://redis.prod.svc.cluster.local:6379/1"
    # TTL in seconds for cached masking resolution results
    cache_ttl: int = 60
    local_cache_size: int = 5_000

    # ── JWT (issued by this service for its own API) ────────
    jwt_secret: str = "changeme-catalog-jwt-secret-min-32-chars!"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 1440  # 24 h

    # Master bootstrap token accepted as admin without a user record
    master_token: str = "changeme-catalog-master-token"

    # ── Apache Doris ────────────────────────────────────────
    doris_host: str = "doris-fe.prod.svc.cluster.local"
    doris_port: int = 9030
    doris_admin_user: str = "root"
    doris_admin_password: str = ""
    # The Doris database that contains the governance demo tables
    doris_demo_database: str = "governance_demo"
    # Suffix appended to every base table name to form the masked view name
    masked_view_suffix: str = "_masked"

    # ── RBAC Control Plane ──────────────────────────────────
    # Used to look up a user's roles and validate masking exceptions
    rbac_plane_url: str = "http://rbac-plane.prod.svc.cluster.local:8080"
    rbac_plane_token: str = "changeme-rbac-master-token"

    # ── Auto-classification scan ────────────────────────────
    # Minimum name-match score (0.0–1.0) to tag a column automatically
    auto_classify_threshold: float = 0.70


@lru_cache
def get_settings() -> Settings:
    return Settings()
