"""
Audit helper — fire-and-forget DB write.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from ..database import get_session_factory
from ..models import AuditLog

log = logging.getLogger(__name__)


async def audit(
    actor: str,
    action: str,
    target_type: Optional[str] = None,
    target_id: Optional[str] = None,
    detail: Optional[dict] = None,
    ip_address: Optional[str] = None,
) -> None:
    """Write an audit entry. Swallows errors — never block the main request."""
    try:
        factory = get_session_factory()
        async with factory() as session:
            session.add(AuditLog(
                actor=actor,
                action=action,
                target_type=target_type,
                target_id=str(target_id) if target_id else None,
                detail=detail or {},
                ip_address=ip_address,
            ))
            await session.commit()
    except Exception as exc:
        log.warning("audit write failed: %s", exc)
