"""
Authentication & authorization middleware.

Two auth paths:
  1. Bearer JWT  — short-lived token issued by POST /api/v1/auth/token
  2. Bearer API token — long-lived, stored as SHA-256 hash in DB / checked vs master_token

Scope enforcement:
  - "read"  → GET endpoints
  - "write" → POST / PUT / PATCH
  - "admin" → DELETE + /sync + /tokens
"""
from __future__ import annotations

import hashlib
import logging
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

import jwt
from datetime import datetime, timezone

from ..config import get_settings
from ..database import db_session

log = logging.getLogger(__name__)
bearer_scheme = HTTPBearer(auto_error=False)


def _hash_token(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


# ── JWT helpers ────────────────────────────────────────────

def create_jwt(subject: str, scopes: list[str]) -> tuple[str, int]:
    """Return (encoded_jwt, expires_in_seconds)."""
    from datetime import timedelta
    s = get_settings()
    expire = datetime.now(timezone.utc) + timedelta(minutes=s.jwt_expire_minutes)
    payload = {
        "sub": subject,
        "scopes": scopes,
        "exp": expire,
        "iat": datetime.now(timezone.utc),
    }
    token = jwt.encode(payload, s.jwt_secret, algorithm=s.jwt_algorithm)
    return token, s.jwt_expire_minutes * 60


def _decode_jwt(token: str) -> Optional[dict]:
    s = get_settings()
    try:
        return jwt.decode(token, s.jwt_secret, algorithms=[s.jwt_algorithm])
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


# ── Principal extraction ───────────────────────────────────

async def _resolve_principal(
    credentials: Optional[HTTPAuthorizationCredentials],
    request: Request,
) -> dict:
    """
    Returns {"sub": str, "scopes": list[str]} or raises 401.
    Checks in order:
      1. Master token (instant, no DB)
      2. JWT (no DB if valid signature)
      3. DB API token (DB hit — but only on cache miss; the JWT path avoids this)
    """
    s = get_settings()

    if credentials is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Authorization header required")

    raw = credentials.credentials

    # 1. Master token
    if raw == s.master_token:
        return {"sub": "master", "scopes": ["read", "write", "admin"]}

    # 2. JWT
    payload = _decode_jwt(raw)
    if payload:
        return {"sub": payload["sub"], "scopes": payload.get("scopes", [])}

    # 3. DB API token
    token_hash = _hash_token(raw)
    async with db_session() as session:
        from sqlalchemy import select, text
        from ..models import ApiToken
        result = await session.execute(
            select(ApiToken).where(
                ApiToken.token_hash == token_hash,
                ApiToken.revoked.is_(False),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                                detail="Invalid or revoked token")
        # Check expiry
        if row.expires_at and row.expires_at < datetime.now(timezone.utc):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                                detail="Token expired")
        # Update last_used_at (fire-and-forget)
        row.last_used_at = datetime.now(timezone.utc)
        return {"sub": row.name, "scopes": row.scopes}


# ── Dependency factories ───────────────────────────────────

def require_scope(*required: str):
    """FastAPI dependency: ensure principal has ALL of the required scopes."""
    async def _dep(
        request: Request,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    ) -> dict:
        principal = await _resolve_principal(credentials, request)
        missing = [s for s in required if s not in principal["scopes"]]
        if missing:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Scope(s) required: {', '.join(required)}",
            )
        return principal
    return _dep


# Convenience shortcuts
require_read  = require_scope("read")
require_write = require_scope("write")
require_admin = require_scope("admin")
