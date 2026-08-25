"""
Star Knowledge Catalog — Masking Views router.
Apply and inspect Doris masked views.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select

from ..database import db_session
from ..middleware.auth import require_read, require_write
from ..models import ColumnTag, DorisViewManifest
from ..schemas import (
    ApplyMaskingRequest, ApplyMaskingResponse, ApplyMaskingResult,
    MaskedQueryRequest, MaskedQueryResponse,
)
from ..engine import masking
from ..engine.rbac_client import get_rbac_client

router = APIRouter(prefix="/masking", tags=["Masking Views"])


# ── Apply masked views ────────────────────────────────────────────────────────

@router.post("/apply", response_model=ApplyMaskingResponse,
             summary="Build and apply masked Doris views")
async def apply_masking(
    body: ApplyMaskingRequest,
    principal: dict = Depends(require_write),
):
    """
    Generates and applies CREATE OR REPLACE VIEW DDL in Doris for every
    tagged table in the given database (or a single table).

    The view replaces sensitive columns with masking expressions derived
    from active masking policies. All Doris query optimisations (vectorised
    execution, partition pruning, predicate push-down) apply to views, so
    sub-second performance is preserved.
    """
    if body.doris_table:
        tables = [body.doris_table]
    else:
        # Discover all tables with column tags in this database
        async with db_session() as session:
            result = await session.execute(
                select(ColumnTag.doris_table)
                .where(ColumnTag.doris_database == body.doris_database)
                .distinct()
            )
            tables = [r[0] for r in result.all()]

    results: list[ApplyMaskingResult] = []
    for table in tables:
        res = await masking.apply_masking_view(
            database=body.doris_database,
            table=table,
            dry_run=body.dry_run,
            force=body.force,
        )
        results.append(
            ApplyMaskingResult(
                doris_database=body.doris_database,
                doris_table=table,
                view_name=res["view_name"],
                columns_masked=res["columns_masked"],
                action=res["action"],
                detail=res.get("detail"),
            )
        )

    return ApplyMaskingResponse(
        tables_processed=len(results),
        results=results,
    )


# ── List view manifests ───────────────────────────────────────────────────────

@router.get("/views", summary="List applied masked view manifests")
async def list_view_manifests(
    database: Optional[str] = Query(None),
    principal: dict = Depends(require_read),
):
    async with db_session() as session:
        stmt = select(DorisViewManifest).order_by(
            DorisViewManifest.doris_database, DorisViewManifest.base_table
        )
        if database:
            stmt = stmt.where(DorisViewManifest.doris_database == database)
        result = await session.execute(stmt)
        manifests = result.scalars().all()
    return [
        {
            "doris_database": m.doris_database,
            "base_table": m.base_table,
            "view_name": m.view_name,
            "columns_masked": m.columns_masked,
            "view_checksum": m.view_checksum,
            "last_applied_at": m.last_applied_at,
        }
        for m in manifests
    ]


# ── Role-aware query planner ──────────────────────────────────────────────────

@router.post("/query", response_model=MaskedQueryResponse,
             summary="Generate a role-aware SELECT statement for a user")
async def plan_masked_query(
    body: MaskedQueryRequest,
    principal: dict = Depends(require_read),
):
    """
    Resolves which Doris object (base table or masked view) a user should
    query, then returns the complete SQL SELECT statement they should execute.

    The caller (analyst client, BI tool proxy, Jupyter notebook) runs the
    returned SQL directly against Doris — no Python in the hot query path.

    Masking decisions:
      • roles with a RoleMaskingException for ALL classifications present in
        the table → directed to the base table (full CLEAR access).
      • all other roles → directed to the `<table>_masked` view.
    """
    rbac = get_rbac_client()
    roles = await rbac.role_names(body.username)

    if not roles:
        raise HTTPException(
            404,
            f"User '{body.username}' not found in RBAC Control Plane or has no roles",
        )

    resolution = await masking.resolve_query_target(
        username=body.username,
        database=body.doris_database,
        table=body.doris_table,
        roles=roles,
    )

    target = resolution["target"]
    view_name = resolution["view_name"]
    masked_cols = resolution["columns_masked"]
    role = resolution["role"]

    # Build SELECT SQL
    if body.columns and body.columns != ["*"]:
        cols_sql = ", ".join(f"`{c}`" for c in body.columns)
    else:
        cols_sql = "*"

    if target == "base_table":
        from_obj = f"`{body.doris_database}`.`{body.doris_table}`"
        clear_cols = body.columns or ["*"]
        masked_cols = []
        note = f"Full access: role '{role}' has masking exception for all sensitive classifications in this table."
    else:
        from_obj = f"`{body.doris_database}`.`{view_name}`"
        clear_cols = []
        note = (
            f"Masked access: role '{role}' is routed to view '{view_name}'. "
            f"Columns masked: {masked_cols or 'none (no active policy)'}."
        )

    where_part = f"\nWHERE {body.where_clause}" if body.where_clause else ""
    sql = f"SELECT {cols_sql}\nFROM {from_obj}{where_part}\nLIMIT {body.limit};"

    return MaskedQueryResponse(
        username=body.username,
        role=role,
        target=target,
        sql=sql,
        columns_masked=masked_cols,
        columns_clear=clear_cols,
        note=note,
    )
