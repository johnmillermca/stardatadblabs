"""
Star Knowledge Catalog — Column Tags router.
Manage manual column tags and trigger auto-classification scans.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from ..database import db_session, cache_invalidate_prefix, COLUMN_TAG_PREFIX
from ..middleware.auth import require_read, require_write, require_admin
from ..models import ColumnTag, DataClassification, GlossaryTerm
from ..schemas import (
    ColumnTagCreate, ColumnTagOut, OkResponse,
    ScanRequest, ScanResponse, ScanColumnResult, ScanTableResult,
)
from ..engine import classifier as clf

router = APIRouter(prefix="/columns", tags=["Column Tags"])


def _enrich(tag: ColumnTag) -> ColumnTagOut:
    out = ColumnTagOut.model_validate(tag)
    if tag.glossary_term:
        out.glossary_term_name = tag.glossary_term.name
    if tag.classification:
        out.classification_name = tag.classification.name
    return out


# ── List / Get ───────────────────────────────────────────────────────────────

@router.get("", response_model=list[ColumnTagOut], summary="List column tags")
async def list_column_tags(
    database: Optional[str] = Query(None),
    table: Optional[str] = Query(None),
    principal: dict = Depends(require_read),
):
    async with db_session() as session:
        stmt = (
            select(ColumnTag)
            .options(
                selectinload(ColumnTag.glossary_term),
                selectinload(ColumnTag.classification),
            )
            .order_by(ColumnTag.doris_database, ColumnTag.doris_table, ColumnTag.column_name)
        )
        if database:
            stmt = stmt.where(ColumnTag.doris_database == database)
        if table:
            stmt = stmt.where(ColumnTag.doris_table == table)
        result = await session.execute(stmt)
        return [_enrich(t) for t in result.scalars().all()]


@router.get("/{db}/{table}/{column}", response_model=ColumnTagOut,
            summary="Get a specific column tag")
async def get_column_tag(
    db: str, table: str, column: str,
    principal: dict = Depends(require_read),
):
    async with db_session() as session:
        result = await session.execute(
            select(ColumnTag)
            .options(
                selectinload(ColumnTag.glossary_term),
                selectinload(ColumnTag.classification),
            )
            .where(
                ColumnTag.doris_database == db,
                ColumnTag.doris_table == table,
                ColumnTag.column_name == column,
            )
        )
        tag = result.scalar_one_or_none()
        if not tag:
            raise HTTPException(404, f"No tag for {db}.{table}.{column}")
        return _enrich(tag)


# ── Create / Update manual tag ───────────────────────────────────────────────

@router.post("", response_model=ColumnTagOut, status_code=201,
             summary="Manually tag a column")
async def create_column_tag(
    body: ColumnTagCreate,
    principal: dict = Depends(require_write),
):
    async with db_session() as session:
        existing = await session.execute(
            select(ColumnTag).where(
                ColumnTag.doris_database == body.doris_database,
                ColumnTag.doris_table == body.doris_table,
                ColumnTag.column_name == body.column_name,
            )
        )
        existing_tag = existing.scalar_one_or_none()
        if existing_tag:
            if existing_tag.auto_detected:
                # Promote auto-tag to manual
                existing_tag.glossary_term_id = body.glossary_term_id
                existing_tag.classification_id = body.classification_id
                existing_tag.auto_detected = False
                existing_tag.override_reason = body.override_reason
                await session.flush()
                await session.refresh(
                    existing_tag, ["glossary_term", "classification"]
                )
                await cache_invalidate_prefix(COLUMN_TAG_PREFIX)
                return _enrich(existing_tag)
            raise HTTPException(
                409,
                f"Column {body.doris_database}.{body.doris_table}.{body.column_name} "
                "is already manually tagged. Use PATCH to update.",
            )

        tag = ColumnTag(
            doris_database=body.doris_database,
            doris_table=body.doris_table,
            column_name=body.column_name,
            glossary_term_id=body.glossary_term_id,
            classification_id=body.classification_id,
            auto_detected=False,
            detection_score=None,
            override_reason=body.override_reason,
        )
        session.add(tag)
        await session.flush()
        await session.refresh(tag, ["glossary_term", "classification"])
        await cache_invalidate_prefix(COLUMN_TAG_PREFIX)
        return _enrich(tag)


@router.delete("/{db}/{table}/{column}", response_model=OkResponse,
               summary="Remove a column tag")
async def delete_column_tag(
    db: str, table: str, column: str,
    principal: dict = Depends(require_admin),
):
    async with db_session() as session:
        result = await session.execute(
            select(ColumnTag).where(
                ColumnTag.doris_database == db,
                ColumnTag.doris_table == table,
                ColumnTag.column_name == column,
            )
        )
        tag = result.scalar_one_or_none()
        if not tag:
            raise HTTPException(404, f"No tag for {db}.{table}.{column}")
        await session.delete(tag)
        await cache_invalidate_prefix(COLUMN_TAG_PREFIX)
    return OkResponse(message=f"Tag for {db}.{table}.{column} removed")


# ── Auto-classification scan ──────────────────────────────────────────────────

@router.post("/scan", response_model=ScanResponse,
             summary="Auto-classify columns using glossary patterns")
async def scan_columns(
    body: ScanRequest,
    principal: dict = Depends(require_write),
):
    """
    Scans the given Doris database (or single table) and automatically
    tags columns by matching their names against glossary term patterns.

    Returns a per-table summary of matches. Persists tags unless `dry_run=true`.
    Manual (non-auto-detected) tags are never overwritten regardless of `overwrite_existing`.
    """
    s_settings = None
    from ..config import get_settings as _gs
    threshold = _gs().auto_classify_threshold

    term_specs = await clf._load_term_specs()

    if body.doris_table:
        tables_matches = {
            body.doris_table: await clf.scan_table(
                body.doris_database, body.doris_table, term_specs, threshold
            )
        }
    else:
        tables_matches = await clf.scan_database(
            body.doris_database, term_specs, threshold
        )

    table_results: list[ScanTableResult] = []
    for table_name, matches in tables_matches.items():
        col_results: list[ScanColumnResult] = []
        tagged = 0

        for m in matches:
            if m.score < threshold or m.term_id is None:
                col_results.append(ScanColumnResult(
                    column_name=m.column_name,
                    matched_term=None,
                    matched_classification=None,
                    score=m.score,
                    action="skipped",
                ))
                continue

            action = "dry_run" if body.dry_run else "tagged"

            if not body.dry_run:
                async with db_session() as session:
                    existing = await session.execute(
                        select(ColumnTag).where(
                            ColumnTag.doris_database == body.doris_database,
                            ColumnTag.doris_table == table_name,
                            ColumnTag.column_name == m.column_name,
                        )
                    )
                    ex_tag = existing.scalar_one_or_none()

                    if ex_tag:
                        if not ex_tag.auto_detected:
                            action = "skipped"  # never overwrite manual tags
                        elif body.overwrite_existing:
                            ex_tag.glossary_term_id = m.term_id
                            ex_tag.classification_id = m.classification_id
                            ex_tag.detection_score = m.score
                        else:
                            action = "skipped"
                    else:
                        session.add(ColumnTag(
                            doris_database=body.doris_database,
                            doris_table=table_name,
                            column_name=m.column_name,
                            glossary_term_id=m.term_id,
                            classification_id=m.classification_id,
                            auto_detected=True,
                            detection_score=m.score,
                        ))

            if action == "tagged":
                tagged += 1
                await cache_invalidate_prefix(COLUMN_TAG_PREFIX)

            col_results.append(ScanColumnResult(
                column_name=m.column_name,
                matched_term=m.term_name,
                matched_classification=m.classification_name,
                score=round(m.score, 2),
                action=action,
            ))

        table_results.append(ScanTableResult(
            doris_database=body.doris_database,
            doris_table=table_name,
            columns_scanned=len(matches),
            columns_tagged=tagged,
            results=col_results,
        ))

    return ScanResponse(
        tables_scanned=len(table_results),
        tables_results=table_results,
    )
