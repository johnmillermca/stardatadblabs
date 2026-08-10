"""
/roles  — CRUD for roles and their permission sets.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from ..database import db_session
from ..middleware.auth import require_read, require_write, require_admin
from ..middleware.audit import audit
from ..models import Permission, Role, RolePermission, Service
from ..schemas import OkResponse, RoleCreate, RoleOut, RolePermissionOut, RoleUpdate
from ..database import cache_invalidate_role


async def _resolve_permission(session, permission_name: str) -> Permission:
    """Resolve a 'service:PERMISSION_NAME' slug to a Permission row."""
    parts = permission_name.split(":", 1)
    if len(parts) != 2:
        raise HTTPException(
            400,
            f"Invalid permission format '{permission_name}'. "
            "Use 'service:PERMISSION_NAME', e.g. 'doris:SELECT'.",
        )
    service_slug, perm_name = parts
    result = await session.execute(
        select(Permission)
        .join(Service)
        .where(Service.name == service_slug, Permission.name == perm_name)
    )
    perm = result.scalar_one_or_none()
    if not perm:
        raise HTTPException(
            404,
            f"Permission '{permission_name}' not found. "
            "Use 'rbacctl role perms' to list available permissions.",
        )
    return perm

router = APIRouter(prefix="/roles", tags=["Roles"])


async def _load_role(session, role_id: int) -> Role:
    result = await session.execute(
        select(Role)
        .options(
            selectinload(Role.role_perms)
            .selectinload(RolePermission.permission)
            .selectinload(Permission.service)
        )
        .where(Role.id == role_id)
    )
    role = result.scalar_one_or_none()
    if not role:
        raise HTTPException(404, "Role not found")
    return role


def _serialize(role: Role) -> RoleOut:
    perms = [
        RolePermissionOut(
            permission_id=rp.permission_id,
            permission_name=rp.permission.name,
            service_name=rp.permission.service.name,
            resource_scope=rp.resource_scope or {},
        )
        for rp in role.role_perms
    ]
    return RoleOut(
        id=role.id,
        name=role.name,
        display_name=role.display_name,
        description=role.description,
        created_at=role.created_at,
        permissions=perms,
    )


@router.get("", response_model=list[RoleOut], summary="List all roles")
async def list_roles(
    q: str = Query("", description="Filter by name (substring)"),
    principal: dict = Depends(require_read),
):
    async with db_session() as session:
        stmt = (
            select(Role)
            .options(
                selectinload(Role.role_perms)
                .selectinload(RolePermission.permission)
                .selectinload(Permission.service)
            )
            .order_by(Role.name)
        )
        if q:
            stmt = stmt.where(Role.name.ilike(f"%{q}%"))
        result = await session.execute(stmt)
        return [_serialize(r) for r in result.scalars().all()]


@router.get("/{role_id}", response_model=RoleOut, summary="Get a role by ID")
async def get_role(role_id: int, principal: dict = Depends(require_read)):
    async with db_session() as session:
        return _serialize(await _load_role(session, role_id))


@router.post("", response_model=RoleOut, status_code=201, summary="Create a role")
async def create_role(
    body: RoleCreate,
    request: Request,
    principal: dict = Depends(require_write),
):
    async with db_session() as session:
        # Duplicate check
        existing = await session.execute(select(Role).where(Role.name == body.name))
        if existing.scalar_one_or_none():
            raise HTTPException(409, f"Role '{body.name}' already exists")

        role = Role(name=body.name, display_name=body.display_name,
                    description=body.description)
        session.add(role)
        await session.flush()   # get role.id

        for rp in body.permissions:
            perm = await _resolve_permission(session, rp.permission_name)
            session.add(RolePermission(
                role_id=role.id,
                permission_id=perm.id,
                resource_scope=rp.resource_scope,
            ))

        await audit(principal["sub"], "CREATE_ROLE", "role", role.name,
                    ip_address=request.client.host if request.client else None)
        await session.flush()
        return _serialize(await _load_role(session, role.id))


@router.patch("/{role_id}", response_model=RoleOut, summary="Update role metadata")
async def update_role(
    role_id: int,
    body: RoleUpdate,
    request: Request,
    principal: dict = Depends(require_write),
):
    async with db_session() as session:
        role = await _load_role(session, role_id)
        if body.display_name is not None:
            role.display_name = body.display_name
        if body.description is not None:
            role.description = body.description
        await audit(principal["sub"], "UPDATE_ROLE", "role", role.name,
                    ip_address=request.client.host if request.client else None)
        return _serialize(role)


@router.delete("/{role_id}", response_model=OkResponse, summary="Delete a role")
async def delete_role(
    role_id: int,
    request: Request,
    principal: dict = Depends(require_admin),
):
    async with db_session() as session:
        role = await _load_role(session, role_id)
        name = role.name
        await session.delete(role)
        await audit(principal["sub"], "DELETE_ROLE", "role", name,
                    ip_address=request.client.host if request.client else None)
    await cache_invalidate_role(name)
    return OkResponse(message=f"Role '{name}' deleted")


@router.post("/{role_id}/permissions/{service_name}/{permission_name}", response_model=RoleOut,
             summary="Add a permission to a role",
             description=(
                 "Grant a permission to a role using human-readable names. "
                 "`service_name` is one of `doris`, `kafka`, `opensearch`, `spark`. "
                 "`permission_name` is the permission token, e.g. `SELECT`, `CONSUME`, `INDEX_READ`."
             ))
async def add_permission_to_role(
    role_id: int,
    service_name: str,
    permission_name: str,
    resource_scope: dict = {},
    request: Request = None,
    principal: dict = Depends(require_write),
):
    async with db_session() as session:
        role = await _load_role(session, role_id)
        perm = await _resolve_permission(session, f"{service_name}:{permission_name}")
        # Check if already set
        exists = await session.execute(
            select(RolePermission).where(
                RolePermission.role_id == role_id,
                RolePermission.permission_id == perm.id,
            )
        )
        if exists.scalar_one_or_none():
            raise HTTPException(409, "Permission already in role")
        session.add(RolePermission(
            role_id=role_id, permission_id=perm.id,
            resource_scope=resource_scope,
        ))
        await audit(principal["sub"], "ADD_ROLE_PERM", "role", role.name,
                    detail={"permission": f"{service_name}:{permission_name}"},
                    ip_address=request.client.host if request.client else None)
        await session.flush()
        await cache_invalidate_role(role.name)
        return _serialize(await _load_role(session, role_id))


@router.delete("/{role_id}/permissions/{service_name}/{permission_name}", response_model=RoleOut,
               summary="Remove a permission from a role",
               description=(
                   "Revoke a permission from a role using human-readable names. "
                   "`service_name` is one of `doris`, `kafka`, `opensearch`, `spark`. "
                   "`permission_name` is the permission token, e.g. `SELECT`, `CONSUME`, `INDEX_READ`."
               ))
async def remove_permission_from_role(
    role_id: int,
    service_name: str,
    permission_name: str,
    request: Request,
    principal: dict = Depends(require_admin),
):
    async with db_session() as session:
        role = await _load_role(session, role_id)
        perm = await _resolve_permission(session, f"{service_name}:{permission_name}")
        result = await session.execute(
            select(RolePermission).where(
                RolePermission.role_id == role_id,
                RolePermission.permission_id == perm.id,
            )
        )
        rp = result.scalar_one_or_none()
        if not rp:
            raise HTTPException(404, "Permission not in role")
        await session.delete(rp)
        await audit(principal["sub"], "REMOVE_ROLE_PERM", "role", role.name,
                    detail={"permission": f"{service_name}:{permission_name}"},
                    ip_address=request.client.host if request.client else None)
        await cache_invalidate_role(role.name)
        return _serialize(await _load_role(session, role_id))
