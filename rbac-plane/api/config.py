"""
RBAC Control Plane — central configuration.
All values come from environment variables (injected via K8s Secret / OpenBao).
"""
from __future__ import annotations

import os
from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # ── API server ─────────────────────────────────────────
    app_title: str = "RBAC Control Plane"
    app_version: str = "1.0.0"
    debug: bool = False
    api_prefix: str = "/api/v1"
    # Comma-separated list of allowed CORS origins ("*" for any)
    cors_origins: str = "*"

    # ── PostgreSQL (metadata store) ────────────────────────
    pg_host: str = "postgresql.prod.svc.cluster.local"
    pg_port: int = 5432
    pg_db: str = "rbac"
    pg_user: str = "rbac"
    pg_password: str = "changeme"
    pg_pool_min: int = 2
    pg_pool_max: int = 20

    @property
    def pg_dsn(self) -> str:
        return (
            f"postgresql+asyncpg://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        )

    # ── Redis (role lookup cache) ──────────────────────────
    redis_url: str = "redis://redis.prod.svc.cluster.local:6379/0"
    # How long a resolved role-set lives in cache (seconds)
    cache_ttl: int = 30
    # How many per-user checks to allow before Redis is hit again
    # (in-process LRU sits in front of Redis, TTL = cache_ttl / 3)
    local_cache_size: int = 10_000

    # ── JWT ────────────────────────────────────────────────
    jwt_secret: str = "changeme-jwt-secret-min-32-chars-long!"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 1440  # 24 h

    # ── Master admin token (bootstrap) ────────────────────
    # A raw token that is always accepted as admin. Set once at install.
    master_token: str = "changeme-master-token"

    # ── Doris adapter ─────────────────────────────────────
    doris_host: str = "doris-fe.prod.svc.cluster.local"
    doris_port: int = 9030
    doris_admin_user: str = "root"
    doris_admin_password: str = ""

    # ── Kafka adapter (Strimzi Kubernetes API) ─────────────
    # We use kubectl / Kubernetes API to create KafkaUser CRs
    # Running inside the cluster — uses in-cluster service account
    kafka_namespace: str = "prod"
    kafka_cluster_name: str = "strimzi-kafka"
    # Bootstrap address for AdminClient (to manage ACLs)
    kafka_bootstrap: str = "strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092"
    kafka_admin_scram_user: str = "kafka-app-user"
    kafka_admin_scram_password: str = ""

    # ── OpenSearch adapter ─────────────────────────────────
    opensearch_host: str = "opensearch-cluster-master.prod.svc.cluster.local"
    opensearch_port: int = 9200
    opensearch_admin_user: str = "admin"
    opensearch_admin_password: str = ""
    opensearch_verify_ssl: bool = False     # self-signed in-cluster cert

    # ── Spark adapter ──────────────────────────────────────
    # Allowlist is a K8s ConfigMap — updated via Kubernetes API
    spark_allowlist_cm: str = "spark-rbac-allowlist"
    spark_namespace: str = "prod"


@lru_cache
def get_settings() -> Settings:
    return Settings()
