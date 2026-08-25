"""
Star Knowledge Catalog — Masking Policies router.
CRUD for masking_policies.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from ..database import db_session, cache_invalidate_prefix, POLICY_CACHE_PREFIX
from ..middleware.auth import require_read, require_write, require_admin
from ..models import MaskingAlgorithm, MaskingPolicy
from ..schemas import OkResponse, PolicyCreate, PolicyOut, PolicyUpdate

router = APIRouter(prefix="/policies", tags=["Masking Policies"])


def _enrich(p: MaskingPolicy) -> PolicyOut:
    out = PolicyOut.model_validate(p)
    if p.algorithm:
        out.algorithm_name = p.algorithm.name
    return out


@router.get("", response_model=list[PolicyOut], summary="List masking policies")
async def list_policies(principal: dict = Depends(require_read)):
    async with db_session() as session:
        result = await session.execute(
            select(MaskingPolicy)
            .options(selectinload(MaskingPolicy.algorithm))
            .order_by(MaskingPolicy.priority.desc(), MaskingPolicy.name)
        )
        return [_enrich(p) for p in result.scalars().all()]


@router.get("/{name}", response_model=PolicyOut, summary="Get a policy by name")
async def get_policy(name: str, principal: dict = Depends(require_read)):
    async with db_session() as session:
        result = await session.execute(
            select(MaskingPolicy)
            .options(selectinload(MaskingPolicy.algorithm))
            .where(MaskingPolicy.name == name)
        )
        p = result.scalar_one_or_none()
        if not p:
            raise HTTPException(404, f"Policy '{name}' not found")
        return _enrich(p)


@router.post("", response_model=PolicyOut, status_code=201,
             summary="Create a masking policy")
async def create_policy(
    body: PolicyCreate,
    principal: dict = Depends(require_write),
):
    async with db_session() as session:
        existing = await session.execute(
            select(MaskingPolicy).where(MaskingPolicy.name == body.name)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(409, f"Policy '{body.name}' already exists")

        algo = await session.get(MaskingAlgorithm, body.algorithm_id)
        if not algo:
            raise HTTPException(404, f"Algorithm id={body.algorithm_id} not found")

        policy = MaskingPolicy(**body.model_dump())
        session.add(policy)
        await session.flush()
        await session.refresh(policy, ["algorithm"])
        await cache_invalidate_prefix(POLICY_CACHE_PREFIX)
        return _enrich(policy)


@router.patch("/{name}", response_model=PolicyOut, summary="Update a masking policy")
async def update_policy(
    name: str,
    body: PolicyUpdate,
    principal: dict = Depends(require_write),
):
    async with db_session() as session:
        result = await session.execute(
            select(MaskingPolicy)
            .options(selectinload(MaskingPolicy.algorithm))
            .where(MaskingPolicy.name == name)
        )
        p = result.scalar_one_or_none()
        if not p:
            raise HTTPException(404, f"Policy '{name}' not found")
        for k, v in body.model_dump(exclude_none=True).items():
            setattr(p, k, v)
        await cache_invalidate_prefix(POLICY_CACHE_PREFIX)
        return _enrich(p)


@router.delete("/{name}", response_model=OkResponse, summary="Delete a masking policy")
async def delete_policy(name: str, principal: dict = Depends(require_admin)):
    async with db_session() as session:
        result = await session.execute(
            select(MaskingPolicy).where(MaskingPolicy.name == name)
        )
        p = result.scalar_one_or_none()
        if not p:
            raise HTTPException(404, f"Policy '{name}' not found")
        await session.delete(p)
        await cache_invalidate_prefix(POLICY_CACHE_PREFIX)
    return OkResponse(message=f"Policy '{name}' deleted")
