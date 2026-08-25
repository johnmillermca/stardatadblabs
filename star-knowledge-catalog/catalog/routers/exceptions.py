"""
Star Knowledge Catalog — Role Masking Exceptions router.
Manage which roles bypass masking for a given classification.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from ..database import db_session, cache_invalidate_prefix, POLICY_CACHE_PREFIX
from ..middleware.auth import require_read, require_admin
from ..models import DataClassification, RoleMaskingException
from ..schemas import OkResponse, RoleExceptionCreate, RoleExceptionOut

router = APIRouter(prefix="/exceptions", tags=["Role Masking Exceptions"])


def _enrich(ex: RoleMaskingException) -> RoleExceptionOut:
    out = RoleExceptionOut.model_validate(ex)
    if ex.classification:
        out.classification_name = ex.classification.name
    return out


@router.get("", response_model=list[RoleExceptionOut],
            summary="List all role masking exceptions")
async def list_exceptions(principal: dict = Depends(require_read)):
    async with db_session() as session:
        result = await session.execute(
            select(RoleMaskingException)
            .options(selectinload(RoleMaskingException.classification))
            .order_by(RoleMaskingException.role_name)
        )
        return [_enrich(e) for e in result.scalars().all()]


@router.post("", response_model=RoleExceptionOut, status_code=201,
             summary="Grant a masking exception to a role")
async def create_exception(
    body: RoleExceptionCreate,
    principal: dict = Depends(require_admin),
):
    async with db_session() as session:
        cls = await session.get(DataClassification, body.classification_id)
        if not cls:
            raise HTTPException(404, f"Classification id={body.classification_id} not found")

        existing = await session.execute(
            select(RoleMaskingException).where(
                RoleMaskingException.role_name == body.role_name,
                RoleMaskingException.classification_id == body.classification_id,
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                409,
                f"Role '{body.role_name}' already has an exception for "
                f"classification id={body.classification_id}",
            )

        ex = RoleMaskingException(
            role_name=body.role_name,
            classification_id=body.classification_id,
            granted_by=body.granted_by,
        )
        session.add(ex)
        await session.flush()
        await session.refresh(ex, ["classification"])
        await cache_invalidate_prefix(POLICY_CACHE_PREFIX)
        return _enrich(ex)


@router.delete("/{exception_id}", response_model=OkResponse,
               summary="Revoke a masking exception")
async def delete_exception(
    exception_id: int,
    principal: dict = Depends(require_admin),
):
    async with db_session() as session:
        ex = await session.get(RoleMaskingException, exception_id)
        if not ex:
            raise HTTPException(404, f"Exception id={exception_id} not found")
        await session.delete(ex)
        await cache_invalidate_prefix(POLICY_CACHE_PREFIX)
    return OkResponse(message=f"Exception {exception_id} revoked")
