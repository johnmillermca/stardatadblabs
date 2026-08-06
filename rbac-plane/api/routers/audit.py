"""
/audit  — audit log retrieval.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select

from ..database import db_session
from ..middleware.auth import require_read
from ..models import AuditLog
from ..schemas import AuditEntry

router = APIRouter(prefix="/audit", tags=["Audit"])


@router.get("", response_model=list[AuditEntry], summary="Query audit log")
async def get_audit_log(
    actor: Optional[str]   = Query(None, description="Filter by actor"),
    action: Optional[str]  = Query(None, description="Filter by action type"),
    since: Optional[datetime] = Query(None, description="ISO datetime lower bound"),
    limit: int             = Query(100, ge=1, le=1000),
    offset: int            = Query(0, ge=0),
    principal: dict        = Depends(require_read),
):
    async with db_session() as session:
        stmt = select(AuditLog).order_by(AuditLog.ts.desc()).limit(limit).offset(offset)
        if actor:
            stmt = stmt.where(AuditLog.actor == actor)
        if action:
            stmt = stmt.where(AuditLog.action == action)
        if since:
            stmt = stmt.where(AuditLog.ts >= since)
        result = await session.execute(stmt)
        return result.scalars().all()
