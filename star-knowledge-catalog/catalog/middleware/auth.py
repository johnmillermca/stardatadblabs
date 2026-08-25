"""
Star Knowledge Catalog — Auth middleware.
Validates JWT Bearer tokens on every protected endpoint.
"""
from __future__ import annotations

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from ..config import get_settings

_bearer = HTTPBearer(auto_error=True)


def _decode(token: str) -> dict:
    s = get_settings()
    try:
        return jwt.decode(token, s.jwt_secret, algorithms=[s.jwt_algorithm])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


async def require_read(
    creds: HTTPAuthorizationCredentials = Depends(_bearer),
) -> dict:
    payload = _decode(creds.credentials)
    scopes = payload.get("scopes", [])
    if not any(s in scopes for s in ("read", "write", "admin")):
        raise HTTPException(status_code=403, detail="Insufficient scope")
    return payload


async def require_write(
    creds: HTTPAuthorizationCredentials = Depends(_bearer),
) -> dict:
    payload = _decode(creds.credentials)
    scopes = payload.get("scopes", [])
    if not any(s in scopes for s in ("write", "admin")):
        raise HTTPException(status_code=403, detail="Insufficient scope — write required")
    return payload


async def require_admin(
    creds: HTTPAuthorizationCredentials = Depends(_bearer),
) -> dict:
    payload = _decode(creds.credentials)
    if "admin" not in payload.get("scopes", []):
        raise HTTPException(status_code=403, detail="Insufficient scope — admin required")
    return payload
