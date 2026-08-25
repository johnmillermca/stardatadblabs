"""
Star Knowledge Catalog — Classifications router.
CRUD for data_classifications.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select

from ..database import db_session, cache_invalidate_prefix, POLICY_CACHE_PREFIX
from ..middleware.auth import require_read, require_write, require_admin
from ..models import DataClassification
from ..schemas import (
    ClassificationCreate, ClassificationOut, ClassificationUpdate, OkResponse,
)

router = APIRouter(prefix="/classifications", tags=["Classifications"])


@router.get("", response_model=list[ClassificationOut], summary="List all data classifications")
async def list_classifications(principal: dict = Depends(require_read)):
    async with db_session() as session:
        result = await session.execute(
            select(DataClassification).order_by(DataClassification.name)
        )
        return result.scalars().all()


@router.get("/{name}", response_model=ClassificationOut, summary="Get a classification by name")
async def get_classification(name: str, principal: dict = Depends(require_read)):
    async with db_session() as session:
        result = await session.execute(
            select(DataClassification).where(DataClassification.name == name)
        )
        cls = result.scalar_one_or_none()
        if not cls:
            raise HTTPException(404, f"Classification '{name}' not found")
        return cls


@router.post("", response_model=ClassificationOut, status_code=201,
             summary="Create a data classification")
async def create_classification(
    body: ClassificationCreate,
    principal: dict = Depends(require_write),
):
    async with db_session() as session:
        existing = await session.execute(
            select(DataClassification).where(DataClassification.name == body.name)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(409, f"Classification '{body.name}' already exists")
        cls = DataClassification(**body.model_dump())
        session.add(cls)
        await session.flush()
        return cls


@router.patch("/{name}", response_model=ClassificationOut, summary="Update a classification")
async def update_classification(
    name: str,
    body: ClassificationUpdate,
    principal: dict = Depends(require_write),
):
    async with db_session() as session:
        result = await session.execute(
            select(DataClassification).where(DataClassification.name == name)
        )
        cls = result.scalar_one_or_none()
        if not cls:
            raise HTTPException(404, f"Classification '{name}' not found")
        for k, v in body.model_dump(exclude_none=True).items():
            setattr(cls, k, v)
        await cache_invalidate_prefix(POLICY_CACHE_PREFIX)
        return cls


@router.delete("/{name}", response_model=OkResponse, summary="Delete a classification")
async def delete_classification(name: str, principal: dict = Depends(require_admin)):
    async with db_session() as session:
        result = await session.execute(
            select(DataClassification).where(DataClassification.name == name)
        )
        cls = result.scalar_one_or_none()
        if not cls:
            raise HTTPException(404, f"Classification '{name}' not found")
        await session.delete(cls)
        await cache_invalidate_prefix(POLICY_CACHE_PREFIX)
    return OkResponse(message=f"Classification '{name}' deleted")
