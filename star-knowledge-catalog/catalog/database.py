"""
Star Knowledge Catalog — database engine, session factory, and cache layer.

Caching strategy (same three-layer approach as rbac-plane):
  1. In-process TTL-LRU  — zero network, sub-microsecond
  2. Redis               — shared across replicas, sub-millisecond, TTL = cache_ttl
  3. PostgreSQL          — source of truth, hit only on cache miss
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import OrderedDict
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .config import get_settings

log = logging.getLogger(__name__)

# ── PostgreSQL engine ──────────────────────────────────────────────────────

_engine = None
_session_factory = None


def get_engine():
    global _engine
    if _engine is None:
        s = get_settings()
        _engine = create_async_engine(
            s.pg_dsn,
            pool_size=s.pg_pool_max,
            max_overflow=0,
            pool_pre_ping=True,
            echo=s.debug,
        )
    return _engine


def get_session_factory() -> async_sessionmaker:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            get_engine(), expire_on_commit=False, class_=AsyncSession
        )
    return _session_factory


@asynccontextmanager
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


# ── Redis ──────────────────────────────────────────────────────────────────

_redis: Optional[aioredis.Redis] = None


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        s = get_settings()
        _redis = aioredis.from_url(
            s.redis_url,
            encoding="utf-8",
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=1,
        )
    return _redis


# ── In-process TTL-LRU cache ──────────────────────────────────────────────

class _TTLCache:
    """Thread-safe TTL + LRU cache backed by an OrderedDict."""

    def __init__(self, maxsize: int, ttl: float):
        self._maxsize = maxsize
        self._ttl = ttl
        self._store: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            if key not in self._store:
                return None
            expires, value = self._store[key]
            if time.monotonic() > expires:
                del self._store[key]
                return None
            self._store.move_to_end(key)
            return value

    async def set(self, key: str, value: Any) -> None:
        async with self._lock:
            expires = time.monotonic() + self._ttl
            self._store[key] = (expires, value)
            self._store.move_to_end(key)
            if len(self._store) > self._maxsize:
                self._store.popitem(last=False)

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._store.pop(key, None)

    async def clear_prefix(self, prefix: str) -> None:
        async with self._lock:
            keys = [k for k in self._store if k.startswith(prefix)]
            for k in keys:
                del self._store[k]


_settings = get_settings()
_local_cache = _TTLCache(
    maxsize=_settings.local_cache_size,
    ttl=_settings.cache_ttl / 3,
)

# ── Cache key helpers ──────────────────────────────────────────────────────

POLICY_CACHE_PREFIX = "skc:policies"
COLUMN_TAG_PREFIX   = "skc:column_tags"


async def cache_get(key: str) -> Optional[Any]:
    local = await _local_cache.get(key)
    if local is not None:
        return local
    try:
        redis = await get_redis()
        raw = await redis.get(key)
        if raw:
            data = json.loads(raw)
            await _local_cache.set(key, data)
            return data
    except Exception as exc:
        log.warning("Redis GET failed [%s]: %s", key, exc)
    return None


async def cache_set(key: str, value: Any) -> None:
    s = get_settings()
    await _local_cache.set(key, value)
    try:
        redis = await get_redis()
        await redis.set(key, json.dumps(value), ex=s.cache_ttl)
    except Exception as exc:
        log.warning("Redis SET failed [%s]: %s", key, exc)


async def cache_invalidate(key: str) -> None:
    await _local_cache.delete(key)
    try:
        redis = await get_redis()
        await redis.delete(key)
    except Exception as exc:
        log.warning("Redis DELETE failed [%s]: %s", key, exc)


async def cache_invalidate_prefix(prefix: str) -> None:
    await _local_cache.clear_prefix(prefix)
    try:
        redis = await get_redis()
        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor, match=f"{prefix}*", count=200)
            if keys:
                await redis.delete(*keys)
            if cursor == 0:
                break
    except Exception as exc:
        log.warning("Redis SCAN/DEL failed [%s]: %s", prefix, exc)
