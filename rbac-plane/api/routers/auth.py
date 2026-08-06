"""
/auth  — token issuance endpoint.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, status

from ..config import get_settings
from ..database import db_session
from ..middleware.auth import create_jwt, require_admin, _hash_token
from ..middleware.audit import audit
from ..models import ApiToken
from ..schemas import ApiTokenCreate, ApiTokenOut, OkResponse, TokenOut
import secrets

router = APIRouter(prefix="/auth", tags=["Auth"])


@router.post("/token", response_model=TokenOut, summary="Exchange an API token for a JWT")
async def exchange_token(raw_token: str, request: Request):
    """
    POST a raw API token in the `X-Api-Token` header (or request body).
    Returns a short-lived JWT for use in subsequent requests.
    """
    s = get_settings()

    # Master token
    if raw_token == s.master_token:
        token, expires_in = create_jwt("master", ["read", "write", "admin"])
        return TokenOut(access_token=token, expires_in=expires_in)

    # DB lookup
    token_hash = _hash_token(raw_token)
    async with db_session() as session:
        from sqlalchemy import select
        result = await session.execute(
            select(ApiToken).where(
                ApiToken.token_hash == token_hash,
                ApiToken.revoked.is_(False),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                                detail="Invalid token")
        if row.expires_at and row.expires_at < datetime.now(timezone.utc):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                                detail="Token expired")
        jwt_token, expires_in = create_jwt(row.name, list(row.scopes))
        return TokenOut(access_token=jwt_token, expires_in=expires_in)


@router.post("/tokens", response_model=ApiTokenOut, summary="Create a new API token")
async def create_api_token(
    body: ApiTokenCreate,
    request: Request,
    principal: dict = Depends(require_admin),
):
    """Create a long-lived API token. The raw token is only returned once."""
    from datetime import timedelta

    raw = secrets.token_urlsafe(40)
    token_hash = _hash_token(raw)

    expires_at = None
    if body.expires_days:
        expires_at = datetime.now(timezone.utc) + timedelta(days=body.expires_days)

    async with db_session() as session:
        row = ApiToken(
            name=body.name,
            token_hash=token_hash,
            scopes=body.scopes,
            created_by=principal["sub"],
            expires_at=expires_at,
        )
        session.add(row)
        await session.flush()
        await audit(principal["sub"], "CREATE_TOKEN", "api_token", row.name,
                    ip_address=request.client.host if request.client else None)
        out = ApiTokenOut.model_validate(row)
        out.raw_token = raw
        return out


@router.delete("/tokens/{token_name}", response_model=OkResponse, summary="Revoke an API token")
async def revoke_token(
    token_name: str,
    request: Request,
    principal: dict = Depends(require_admin),
):
    from sqlalchemy import select
    async with db_session() as session:
        result = await session.execute(
            select(ApiToken).where(ApiToken.name == token_name)
        )
        row = result.scalar_one_or_none()
        if not row:
            raise HTTPException(404, f"Token '{token_name}' not found")
        row.revoked = True
        await audit(principal["sub"], "REVOKE_TOKEN", "api_token", token_name,
                    ip_address=request.client.host if request.client else None)
    return OkResponse(message=f"Token '{token_name}' revoked")
