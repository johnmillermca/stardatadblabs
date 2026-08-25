"""
Star Knowledge Catalog — Governance Database Switch router.

Provides a per-database enable/disable circuit breaker.
When a database is disabled:
  • POST /api/v1/masking/apply  → skips all tables in that database.
  • POST /api/v1/masking/query  → routes ALL users to the base table
    regardless of their role (as if every classification had an exception).

Use this for:
  - Emergency maintenance windows
  - Bulk data migration / ETL periods where analysts need raw access
  - Incident response (temporarily expose data to a DBA team)
  - Testing base-table vs masked-view query performance comparison
"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from typing import Optional

from ..database import db_session, cache_invalidate_prefix, POLICY_CACHE_PREFIX
from ..middleware.auth import require_read, require_admin
from ..models import GovernanceDatabase

router = APIRouter(prefix="/governance-switch", tags=["Governance Switch"])


# ── Schemas ───────────────────────────────────────────────────────────────────

class GovernanceSwitchOut(BaseModel):
    id: int
    doris_database: str
    governance_enabled: bool
    disabled_reason: Optional[str]
    disabled_by: Optional[str]
    disabled_at: Optional[datetime]
    enabled_at: Optional[datetime]
    updated_at: Optional[datetime]

    model_config = {"from_attributes": True}


class DisableRequest(BaseModel):
    reason: str
    disabled_by: str = "admin"


class EnableRequest(BaseModel):
    enabled_by: str = "admin"


# ── Helper ────────────────────────────────────────────────────────────────────

async def _get_or_create(session, doris_database: str) -> GovernanceDatabase:
    result = await session.execute(
        select(GovernanceDatabase).where(
            GovernanceDatabase.doris_database == doris_database
        )
    )
    row = result.scalar_one_or_none()
    if not row:
        row = GovernanceDatabase(
            doris_database=doris_database,
            governance_enabled=True,
        )
        session.add(row)
        await session.flush()
    return row


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[GovernanceSwitchOut],
            summary="List governance status for all registered databases")
async def list_switches(principal: dict = Depends(require_read)):
    async with db_session() as session:
        result = await session.execute(
            select(GovernanceDatabase).order_by(GovernanceDatabase.doris_database)
        )
        return result.scalars().all()


@router.get("/{doris_database}", response_model=GovernanceSwitchOut,
            summary="Get governance status for a specific database")
async def get_switch(doris_database: str, principal: dict = Depends(require_read)):
    async with db_session() as session:
        result = await session.execute(
            select(GovernanceDatabase).where(
                GovernanceDatabase.doris_database == doris_database
            )
        )
        row = result.scalar_one_or_none()
        if not row:
            raise HTTPException(
                404,
                f"Database '{doris_database}' not registered in governance switch. "
                "Run a masking apply first, or POST /governance-switch/{db}/enable."
            )
        return row


@router.post("/{doris_database}/disable", response_model=GovernanceSwitchOut,
             summary="Disable masking for an entire Doris database (circuit breaker)")
async def disable_governance(
    doris_database: str,
    body: DisableRequest,
    principal: dict = Depends(require_admin),
):
    """
    Immediately disables governance for *doris_database*.

    Effect:
    - POST /api/v1/masking/apply  will skip this database.
    - POST /api/v1/masking/query  will route ALL users to base tables
      (equivalent to every role holding a masking exception).

    Existing masked views in Doris are NOT dropped — they remain in place
    so re-enabling is instant (no DDL re-apply needed).
    """
    async with db_session() as session:
        row = await _get_or_create(session, doris_database)
        if not row.governance_enabled:
            raise HTTPException(
                409,
                f"Database '{doris_database}' is already disabled. "
                f"Reason: {row.disabled_reason}"
            )
        row.governance_enabled = False
        row.disabled_reason = body.reason
        row.disabled_by = body.disabled_by
        row.disabled_at = datetime.now(timezone.utc)
        row.enabled_at = None
        await cache_invalidate_prefix(POLICY_CACHE_PREFIX)
        await session.flush()
        return row


@router.post("/{doris_database}/enable", response_model=GovernanceSwitchOut,
             summary="Re-enable masking for a Doris database")
async def enable_governance(
    doris_database: str,
    body: EnableRequest,
    principal: dict = Depends(require_admin),
):
    """
    Re-enables governance for *doris_database*.

    Effect:
    - POST /api/v1/masking/apply  will include this database again.
    - POST /api/v1/masking/query  will resume role-based routing
      (analyst → masked view, admin → base table).

    The existing masked views are already in Doris — users are immediately
    restricted back to their masked views without any DDL re-apply.
    If schema has changed since disabling, run masking/apply with force=true.
    """
    async with db_session() as session:
        row = await _get_or_create(session, doris_database)
        if row.governance_enabled:
            raise HTTPException(
                409,
                f"Database '{doris_database}' is already enabled."
            )
        row.governance_enabled = True
        row.disabled_reason = None
        row.disabled_by = None
        row.disabled_at = None
        row.enabled_at = datetime.now(timezone.utc)
        await cache_invalidate_prefix(POLICY_CACHE_PREFIX)
        await session.flush()
        return row
