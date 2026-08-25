"""
Star Knowledge Catalog — Glossary Terms router.
CRUD for glossary_terms.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from ..database import db_session, cache_invalidate_prefix, POLICY_CACHE_PREFIX
from ..middleware.auth import require_read, require_write, require_admin
from ..models import DataClassification, GlossaryTerm
from ..schemas import (
    GlossaryTermCreate, GlossaryTermOut, GlossaryTermUpdate, OkResponse,
)

router = APIRouter(prefix="/glossary", tags=["Glossary"])


def _enrich(term: GlossaryTerm) -> GlossaryTermOut:
    out = GlossaryTermOut.model_validate(term)
    if term.classification:
        out.classification_name = term.classification.name
    return out


@router.get("", response_model=list[GlossaryTermOut], summary="List glossary terms")
async def list_terms(principal: dict = Depends(require_read)):
    async with db_session() as session:
        result = await session.execute(
            select(GlossaryTerm)
            .options(selectinload(GlossaryTerm.classification))
            .order_by(GlossaryTerm.name)
        )
        return [_enrich(t) for t in result.scalars().all()]


@router.get("/{name}", response_model=GlossaryTermOut, summary="Get a glossary term")
async def get_term(name: str, principal: dict = Depends(require_read)):
    async with db_session() as session:
        result = await session.execute(
            select(GlossaryTerm)
            .options(selectinload(GlossaryTerm.classification))
            .where(GlossaryTerm.name == name)
        )
        term = result.scalar_one_or_none()
        if not term:
            raise HTTPException(404, f"Glossary term '{name}' not found")
        return _enrich(term)


@router.post("", response_model=GlossaryTermOut, status_code=201,
             summary="Create a glossary term")
async def create_term(
    body: GlossaryTermCreate,
    principal: dict = Depends(require_write),
):
    async with db_session() as session:
        existing = await session.execute(
            select(GlossaryTerm).where(GlossaryTerm.name == body.name)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(409, f"Glossary term '{body.name}' already exists")

        if body.classification_id:
            cls = await session.get(DataClassification, body.classification_id)
            if not cls:
                raise HTTPException(404, f"Classification id={body.classification_id} not found")

        term = GlossaryTerm(**body.model_dump())
        session.add(term)
        await session.flush()
        await session.refresh(term, ["classification"])
        await cache_invalidate_prefix(POLICY_CACHE_PREFIX)
        return _enrich(term)


@router.patch("/{name}", response_model=GlossaryTermOut, summary="Update a glossary term")
async def update_term(
    name: str,
    body: GlossaryTermUpdate,
    principal: dict = Depends(require_write),
):
    async with db_session() as session:
        result = await session.execute(
            select(GlossaryTerm)
            .options(selectinload(GlossaryTerm.classification))
            .where(GlossaryTerm.name == name)
        )
        term = result.scalar_one_or_none()
        if not term:
            raise HTTPException(404, f"Glossary term '{name}' not found")
        for k, v in body.model_dump(exclude_none=True).items():
            setattr(term, k, v)
        await cache_invalidate_prefix(POLICY_CACHE_PREFIX)
        return _enrich(term)


@router.delete("/{name}", response_model=OkResponse, summary="Delete a glossary term")
async def delete_term(name: str, principal: dict = Depends(require_admin)):
    async with db_session() as session:
        result = await session.execute(
            select(GlossaryTerm).where(GlossaryTerm.name == name)
        )
        term = result.scalar_one_or_none()
        if not term:
            raise HTTPException(404, f"Glossary term '{name}' not found")
        await session.delete(term)
        await cache_invalidate_prefix(POLICY_CACHE_PREFIX)
    return OkResponse(message=f"Glossary term '{name}' deleted")
