"""
Star Knowledge Catalog — Role Masking Exceptions router.
Manage which roles bypass masking for a given classification.

When an exception is CREATED:
  • The exception row is written to star_catalog.role_masking_exceptions.
  • Every Doris user whose RBAC role matches body.role_name is immediately
    granted SELECT_PRIV on all base tables for that database so they can
    query clear data.  Masked-view grants are left in place (harmless).

When an exception is DELETED:
  • The exception row is removed from PostgreSQL.
  • Every Doris user whose role no longer holds any exception for the
    affected classifications is immediately re-locked: base-table SELECT
    is revoked and masked-view SELECT is (re-)granted.

This means the Doris grant state is always consistent with the exception
table — no manual GRANT/REVOKE needed after adding or removing an exception.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from ..config import get_settings
from ..database import db_session, cache_invalidate_prefix, POLICY_CACHE_PREFIX
from ..engine.masking import (
    _doris_conn,
    _doris_execute,
    _grant_view_to_user,
    _revoke_base_table_from_user,
)
from ..engine.rbac_client import get_rbac_client
from ..middleware.auth import require_read, require_admin
from ..models import ColumnTag, DataClassification, DorisViewManifest, RoleMaskingException
from ..schemas import OkResponse, RoleExceptionCreate, RoleExceptionOut

router = APIRouter(prefix="/exceptions", tags=["Role Masking Exceptions"])
log = logging.getLogger(__name__)


# ── helpers ──────────────────────────────────────────────────────────────────

def _enrich(ex: RoleMaskingException) -> RoleExceptionOut:
    out = RoleExceptionOut.model_validate(ex)
    if ex.classification:
        out.classification_name = ex.classification.name
    return out


async def _doris_users_with_role(role_name: str) -> list[str]:
    """Return all RBAC usernames that currently hold *role_name*."""
    rbac = get_rbac_client()
    s = get_settings()
    import httpx
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"{s.rbac_plane_url}/api/v1/users",
                headers={"Authorization": f"Bearer {s.rbac_plane_token}"},
            )
            if resp.status_code != 200:
                return []
            all_users = resp.json()
        result = []
        for u in all_users:
            roles = await rbac.role_names(u["username"])
            if role_name in roles:
                result.append(u["username"])
        return result
    except Exception as exc:
        log.warning("Exceptions: could not fetch users for role %s: %s", role_name, exc)
        return []


async def _all_sensitive_tables(database: str) -> list[tuple[str, str]]:
    """
    Return [(table, view_name), ...] for every table in *database* that
    has a masked view manifest — i.e. every table that was masked.
    """
    async with db_session() as session:
        result = await session.execute(
            select(DorisViewManifest.base_table, DorisViewManifest.view_name).where(
                DorisViewManifest.doris_database == database
            )
        )
        return result.all()


async def _grant_base_tables_to_user(database: str, doris_user: str) -> None:
    """
    Grant SELECT_PRIV on every base table (that has a masked view) directly
    to *doris_user*.  Called when an exception is added so the user can
    immediately query clear data.
    """
    tables = await _all_sensitive_tables(database)
    for base_table, _ in tables:
        sql = (
            f"GRANT SELECT_PRIV ON `{database}`.`{base_table}` "
            f"TO '{doris_user}'@'%';"
        )
        try:
            await _doris_execute(sql)
            log.info(
                "Exceptions: granted base-table SELECT on %s.%s to %s",
                database, base_table, doris_user,
            )
        except Exception as exc:
            log.warning(
                "Exceptions: could not grant %s.%s to %s: %s",
                database, base_table, doris_user, exc,
            )


async def _relock_user_for_database(
    database: str,
    doris_user: str,
    remaining_exempt_roles: list[str],
) -> None:
    """
    Called when an exception is revoked for *doris_user*'s role.
    If the user no longer holds ANY exception-covered role, revoke their
    base-table access and re-grant them only the masked views.
    """
    rbac = get_rbac_client()
    user_roles = await rbac.role_names(doris_user)

    # If the user still holds some other exempt role, leave them alone
    if any(r in remaining_exempt_roles for r in user_roles):
        log.debug(
            "Exceptions: relock skipped for %s — still holds exempt role %s",
            doris_user, user_roles,
        )
        return

    # Revoke base tables, re-grant masked views
    tables = await _all_sensitive_tables(database)
    for base_table, view_name in tables:
        await _revoke_base_table_from_user(database, base_table, doris_user)
        await _grant_view_to_user(database, view_name, doris_user)
        log.info(
            "Exceptions: relocked %s — revoked base %s.%s, granted view %s",
            doris_user, database, base_table, view_name,
        )


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[RoleExceptionOut],
            summary="List all role masking exceptions")
async def list_exceptions(principal: dict = Depends(require_read)):
    async with db_session() as session:
        result = await session.execute(
            select(RoleMaskingException)
            .options(selectinload(RoleMaskingException.classification))
            .order_by(RoleMaskingException.role_name)
        )
        return [_enrich(e) for e in result.scalars().all()]


@router.post("", response_model=RoleExceptionOut, status_code=201,
             summary="Grant a masking exception to a role")
async def create_exception(
    body: RoleExceptionCreate,
    principal: dict = Depends(require_admin),
):
    """
    Grants clear-data access to all users currently holding *role_name*.

    Two things happen atomically from the caller's perspective:
      1. Exception row written to star_catalog.role_masking_exceptions.
      2. Every matching Doris user receives SELECT_PRIV on the base tables
         for the governance_demo database immediately — no re-apply needed.
    """
    async with db_session() as session:
        cls = await session.get(DataClassification, body.classification_id)
        if not cls:
            raise HTTPException(404, f"Classification id={body.classification_id} not found")

        existing = await session.execute(
            select(RoleMaskingException).where(
                RoleMaskingException.role_name == body.role_name,
                RoleMaskingException.classification_id == body.classification_id,
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                409,
                f"Role '{body.role_name}' already has an exception for "
                f"classification id={body.classification_id}",
            )

        ex = RoleMaskingException(
            role_name=body.role_name,
            classification_id=body.classification_id,
            granted_by=body.granted_by,
        )
        session.add(ex)
        await session.flush()
        await session.refresh(ex, ["classification"])
        await cache_invalidate_prefix(POLICY_CACHE_PREFIX)
        enriched = _enrich(ex)

    # ── Propagate to Doris ────────────────────────────────────────────────
    # Find every RBAC user that holds body.role_name and immediately grant
    # them SELECT on the base tables so they can see clear data right now.
    s = get_settings()
    affected_users = await _doris_users_with_role(body.role_name)
    for doris_user in affected_users:
        await _grant_base_tables_to_user(s.doris_demo_database, doris_user)
        log.info(
            "Exceptions: user '%s' (role: %s) now has CLEAR access to %s",
            doris_user, body.role_name, s.doris_demo_database,
        )

    return enriched


@router.delete("/{exception_id}", response_model=OkResponse,
               summary="Revoke a masking exception")
async def delete_exception(
    exception_id: int,
    principal: dict = Depends(require_admin),
):
    """
    Revokes clear-data access for the role associated with this exception.

    Two things happen:
      1. Exception row deleted from star_catalog.role_masking_exceptions.
      2. Every matching Doris user is re-locked: base-table SELECT is revoked
         and they are re-granted only the masked view — unless they still hold
         another exempt role.
    """
    s = get_settings()

    async with db_session() as session:
        ex = await session.get(RoleMaskingException, exception_id)
        if not ex:
            raise HTTPException(404, f"Exception id={exception_id} not found")

        revoked_role = ex.role_name

        await session.delete(ex)
        await cache_invalidate_prefix(POLICY_CACHE_PREFIX)
        # Flush so the deleted row is gone before we re-query exempt roles below
        await session.flush()

        # Remaining exempt roles after this deletion
        remaining_result = await session.execute(
            select(RoleMaskingException.role_name).distinct()
        )
        remaining_exempt = [r[0] for r in remaining_result.all()]

    # ── Propagate to Doris ────────────────────────────────────────────────
    # Re-lock every user that was relying on this exception.
    affected_users = await _doris_users_with_role(revoked_role)
    for doris_user in affected_users:
        await _relock_user_for_database(
            s.doris_demo_database, doris_user, remaining_exempt
        )
        log.info(
            "Exceptions: user '%s' (role: %s) re-locked on %s",
            doris_user, revoked_role, s.doris_demo_database,
        )

    return OkResponse(message=f"Exception {exception_id} revoked")
