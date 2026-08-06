"""
/sync  — push RBAC state to each service.

The sync engine is the core of the control plane.
For each (user, service) pair it:
  1. Computes a hash of the user's current role-set.
  2. Compares against the last known hash in sync_state.
  3. If unchanged → skip (idempotent, no-op).
  4. If changed → calls the service adapter to enforce the new state.
  5. Updates sync_state with the new hash.

This design ensures:
  - Zero wasted adapter calls when nothing changed.
  - Convergence even after partial failures (re-running sync always safe).
  - No "thundering herd": the adapter calls are batched and concurrency-limited.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from ..database import db_session
from ..middleware.auth import require_admin
from ..middleware.audit import audit
from ..models import (
    Permission, Role, RoleBinding, RolePermission,
    Service, SyncState, User,
)
from ..schemas import SyncRequest, SyncResponse, SyncResult

log = logging.getLogger(__name__)
router = APIRouter(prefix="/sync", tags=["Sync"])

# Limit concurrency to prevent overwhelming the services
_SEM = asyncio.Semaphore(10)


# ── Adapter registry ───────────────────────────────────────

def _get_adapter(service_name: str):
    from ..adapters import doris, kafka, opensearch, spark
    return {
        "doris":       doris.DorisAdapter(),
        "kafka":       kafka.KafkaAdapter(),
        "opensearch":  opensearch.OpenSearchAdapter(),
        "spark":       spark.SparkAdapter(),
    }.get(service_name)


# ── Resolve effective permissions for a user per service ──

async def resolve_user_service_perms(
    session, user: User, service: Service
) -> list[dict]:
    """
    Return a list of {permission_name, resource_scope} for the given
    (user, service) pair, respecting binding-level service filter and expiry.
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)

    bindings_result = await session.execute(
        select(RoleBinding)
        .options(
            selectinload(RoleBinding.role)
            .selectinload(Role.role_perms)
            .selectinload(RolePermission.permission),
            selectinload(RoleBinding.service),
        )
        .where(
            RoleBinding.user_id == user.id,
            (RoleBinding.expires_at.is_(None)) | (RoleBinding.expires_at > now),
        )
    )
    bindings = bindings_result.scalars().all()

    seen = {}   # permission_name → most permissive resource_scope
    for b in bindings:
        # Skip bindings scoped to a different service
        if b.service_id and b.service_id != service.id:
            continue
        for rp in b.role.role_perms:
            if rp.permission.service_id != service.id:
                continue
            key = rp.permission.name
            if key not in seen:
                seen[key] = rp.resource_scope or {}
    return [{"permission": k, "resource_scope": v} for k, v in seen.items()]


def _hash_perms(perms: list[dict]) -> str:
    serialized = json.dumps(sorted(perms, key=lambda p: p["permission"]),
                            sort_keys=True)
    return hashlib.sha256(serialized.encode()).hexdigest()


# ── Sync one user × one service ────────────────────────────

async def sync_user_service(
    session,
    user: User,
    service: Service,
    dry_run: bool,
    actor: str,
) -> SyncResult:
    async with _SEM:
        perms = await resolve_user_service_perms(session, user, service)
        new_hash = _hash_perms(perms)

        # Check if hash changed
        state_result = await session.execute(
            select(SyncState).where(
                SyncState.user_id == user.id,
                SyncState.service_id == service.id,
            )
        )
        state = state_result.scalar_one_or_none()
        if state and state.role_hash == new_hash:
            return SyncResult(
                username=user.username,
                service=service.name,
                status="skipped",
                detail="no change",
            )

        if dry_run:
            return SyncResult(
                username=user.username,
                service=service.name,
                status="dry_run",
                detail=f"would apply {len(perms)} permissions",
            )

        adapter = _get_adapter(service.name)
        if adapter is None:
            return SyncResult(
                username=user.username,
                service=service.name,
                status="error",
                detail=f"no adapter for service '{service.name}'",
            )

        try:
            await adapter.sync_user(user.username, perms)
        except Exception as exc:
            log.error("sync_user failed [%s/%s]: %s", user.username, service.name, exc)
            return SyncResult(
                username=user.username,
                service=service.name,
                status="error",
                detail=str(exc),
            )

        # Update or create sync_state
        if state:
            state.role_hash = new_hash
            from datetime import datetime, timezone
            state.synced_at = datetime.now(timezone.utc)
        else:
            session.add(SyncState(
                user_id=user.id,
                service_id=service.id,
                role_hash=new_hash,
            ))

        await audit(
            actor, "SYNC", "user_service",
            f"{user.username}:{service.name}",
            detail={"permissions": len(perms), "hash": new_hash},
        )

        return SyncResult(
            username=user.username,
            service=service.name,
            status="synced",
            detail=f"{len(perms)} permissions applied",
        )


# ── Router ─────────────────────────────────────────────────

@router.post("", response_model=SyncResponse, summary="Push RBAC state to services")
async def run_sync(
    body: SyncRequest,
    request: Request,
    principal: dict = Depends(require_admin),
):
    """
    Synchronise role bindings to the target services.

    - Omit `username` and `service` to do a full platform sync.
    - Specify `username` to sync a single user across all (or one) service.
    - Specify `service` to sync all users to that service.
    - Set `dry_run=true` to see what *would* change without applying it.
    """
    async with db_session() as session:
        # Build user list
        if body.username:
            user_result = await session.execute(
                select(User).where(User.username == body.username, User.enabled.is_(True))
            )
            users = user_result.scalars().all()
            if not users:
                raise HTTPException(404, f"User '{body.username}' not found or disabled")
        else:
            user_result = await session.execute(
                select(User).where(User.enabled.is_(True))
            )
            users = user_result.scalars().all()

        # Build service list
        if body.service:
            svc_result = await session.execute(
                select(Service).where(Service.name == body.service, Service.enabled.is_(True))
            )
            services = svc_result.scalars().all()
            if not services:
                raise HTTPException(404, f"Service '{body.service}' not found")
        else:
            svc_result = await session.execute(
                select(Service).where(Service.enabled.is_(True))
            )
            services = svc_result.scalars().all()

        results: list[SyncResult] = []
        tasks = [
            sync_user_service(session, u, svc, body.dry_run, principal["sub"])
            for u in users
            for svc in services
        ]
        results = list(await asyncio.gather(*tasks))

    errors = sum(1 for r in results if r.status == "error")
    return SyncResponse(results=results, errors=errors)
