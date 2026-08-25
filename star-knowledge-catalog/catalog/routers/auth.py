"""
Star Knowledge Catalog — Auth router.
Issues short-lived JWTs from a raw API token.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import jwt
from fastapi import APIRouter, HTTPException

from ..config import get_settings
from ..schemas import TokenOut, TokenRequest

router = APIRouter(prefix="/auth", tags=["Auth"])


@router.post("/token", response_model=TokenOut, summary="Exchange API token for JWT")
async def get_token(body: TokenRequest):
    s = get_settings()
    if body.token != s.master_token:
        raise HTTPException(status_code=401, detail="Invalid token")

    expire = datetime.now(timezone.utc) + timedelta(minutes=s.jwt_expire_minutes)
    payload = {
        "sub": "admin",
        "scopes": ["read", "write", "admin"],
        "exp": expire,
    }
    token = jwt.encode(payload, s.jwt_secret, algorithm=s.jwt_algorithm)
    return TokenOut(
        access_token=token,
        token_type="bearer",
        expires_in=s.jwt_expire_minutes * 60,
    )
