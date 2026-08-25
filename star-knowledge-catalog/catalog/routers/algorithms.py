"""
Star Knowledge Catalog — Masking Algorithms router.
CRUD for masking_algorithms.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select

from ..database import db_session, cache_invalidate_prefix, POLICY_CACHE_PREFIX
from ..middleware.auth import require_read, require_write, require_admin
from ..models import MaskingAlgorithm
from ..schemas import AlgorithmCreate, AlgorithmOut, AlgorithmUpdate, OkResponse

router = APIRouter(prefix="/algorithms", tags=["Masking Algorithms"])


@router.get("", response_model=list[AlgorithmOut], summary="List masking algorithms")
async def list_algorithms(principal: dict = Depends(require_read)):
    async with db_session() as session:
        result = await session.execute(
            select(MaskingAlgorithm).order_by(MaskingAlgorithm.name)
        )
        return result.scalars().all()


@router.get("/{name}", response_model=AlgorithmOut, summary="Get an algorithm by name")
async def get_algorithm(name: str, principal: dict = Depends(require_read)):
    async with db_session() as session:
        result = await session.execute(
            select(MaskingAlgorithm).where(MaskingAlgorithm.name == name)
        )
        algo = result.scalar_one_or_none()
        if not algo:
            raise HTTPException(404, f"Algorithm '{name}' not found")
        return algo


@router.post("", response_model=AlgorithmOut, status_code=201,
             summary="Create a masking algorithm")
async def create_algorithm(
    body: AlgorithmCreate,
    principal: dict = Depends(require_write),
):
    async with db_session() as session:
        existing = await session.execute(
            select(MaskingAlgorithm).where(MaskingAlgorithm.name == body.name)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(409, f"Algorithm '{body.name}' already exists")
        algo = MaskingAlgorithm(**body.model_dump())
        session.add(algo)
        await session.flush()
        return algo


@router.patch("/{name}", response_model=AlgorithmOut, summary="Update an algorithm")
async def update_algorithm(
    name: str,
    body: AlgorithmUpdate,
    principal: dict = Depends(require_write),
):
    async with db_session() as session:
        result = await session.execute(
            select(MaskingAlgorithm).where(MaskingAlgorithm.name == name)
        )
        algo = result.scalar_one_or_none()
        if not algo:
            raise HTTPException(404, f"Algorithm '{name}' not found")
        for k, v in body.model_dump(exclude_none=True).items():
            setattr(algo, k, v)
        await cache_invalidate_prefix(POLICY_CACHE_PREFIX)
        return algo


@router.delete("/{name}", response_model=OkResponse, summary="Delete an algorithm")
async def delete_algorithm(name: str, principal: dict = Depends(require_admin)):
    async with db_session() as session:
        result = await session.execute(
            select(MaskingAlgorithm).where(MaskingAlgorithm.name == name)
        )
        algo = result.scalar_one_or_none()
        if not algo:
            raise HTTPException(404, f"Algorithm '{name}' not found")
        await session.delete(algo)
        await cache_invalidate_prefix(POLICY_CACHE_PREFIX)
    return OkResponse(message=f"Algorithm '{name}' deleted")
