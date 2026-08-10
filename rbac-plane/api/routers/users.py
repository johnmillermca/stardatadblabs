"""
/users  — user CRUD and role-binding management.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from ..database import db_session, cache_invalidate_user
from ..middleware.auth import require_read, require_write, require_admin
from ..middleware.audit import audit
from ..models import Permission, Role, RoleBinding, RolePermission, Service, User
from ..schemas import (
    BindingCreate, BindingOut, BulkBindRequest, BulkBindResponse, BulkBindResult,
    OkResponse, UserCreate, UserOut, UserRolesOut, UserUpdate,
)

router = APIRouter(prefix="/users", tags=["Users"])


# ── Helpers ────────────────────────────────────────────────

async def _get_user(session, username: str) -> User:
    result = await session.execute(
        select(User).where(User.username == username)
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(404, f"User '{username}' not found")
    return user


# ── CRUD ───────────────────────────────────────────────────

@router.get("", response_model=list[UserOut], summary="List users")
async def list_users(
    q: str = Query("", description="Filter by username (substring)"),
    enabled: Optional[bool] = Query(None),
    principal: dict = Depends(require_read),
):
    async with db_session() as session:
        stmt = select(User).order_by(User.username)
        if q:
            stmt = stmt.where(User.username.ilike(f"%{q}%"))
        if enabled is not None:
            stmt = stmt.where(User.enabled == enabled)
        result = await session.execute(stmt)
        return result.scalars().all()


@router.get("/{username}", response_model=UserOut, summary="Get a user")
async def get_user(username: str, principal: dict = Depends(require_read)):
    async with db_session() as session:
        return await _get_user(session, username)


@router.post("", response_model=UserOut, status_code=201, summary="Create a user")
async def create_user(
    body: UserCreate,
    request: Request,
    principal: dict = Depends(require_write),
):
    async with db_session() as session:
        existing = await session.execute(
            select(User).where(User.username == body.username)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(409, f"User '{body.username}' already exists")
        user = User(**body.model_dump())
        session.add(user)
        await audit(principal["sub"], "CREATE_USER", "user", body.username,
                    ip_address=request.client.host if request.client else None)
        await session.flush()
        return user


@router.patch("/{username}", response_model=UserOut, summary="Update a user")
async def update_user(
    username: str,
    body: UserUpdate,
    request: Request,
    principal: dict = Depends(require_write),
):
    async with db_session() as session:
        user = await _get_user(session, username)
        for k, v in body.model_dump(exclude_none=True).items():
            setattr(user, k, v)
        await audit(principal["sub"], "UPDATE_USER", "user", username,
                    ip_address=request.client.host if request.client else None)
        if body.enabled is False:
            await cache_invalidate_user(username)
        return user


@router.delete("/{username}", response_model=OkResponse, summary="Delete a user")
async def delete_user(
    username: str,
    request: Request,
    principal: dict = Depends(require_admin),
):
    async with db_session() as session:
        user = await _get_user(session, username)
        await session.delete(user)
        await audit(principal["sub"], "DELETE_USER", "user", username,
                    ip_address=request.client.host if request.client else None)
    await cache_invalidate_user(username)
    return OkResponse(message=f"User '{username}' deleted")


# ── Role bindings ──────────────────────────────────────────

@router.get("/{username}/bindings", response_model=list[BindingOut],
            summary="List role bindings for a user")
async def list_bindings(username: str, principal: dict = Depends(require_read)):
    async with db_session() as session:
        user = await _get_user(session, username)
        result = await session.execute(
            select(RoleBinding)
            .options(
                selectinload(RoleBinding.role),
                selectinload(RoleBinding.service),
            )
            .where(RoleBinding.user_id == user.id)
        )
        bindings = result.scalars().all()
        return [
            BindingOut(
                id=b.id,
                username=username,
                role_name=b.role.name,
                service_name=b.service.name if b.service else None,
                granted_by=b.granted_by,
                granted_at=b.granted_at,
                expires_at=b.expires_at,
            )
            for b in bindings
        ]


@router.post("/{username}/bindings", response_model=BindingOut, status_code=201,
             summary="Bind a role to a user")
async def bind_role(
    username: str,
    body: BindingCreate,
    request: Request,
    principal: dict = Depends(require_write),
):
    async with db_session() as session:
        user = await _get_user(session, username)

        role_result = await session.execute(
            select(Role).where(Role.name == body.role_name)
        )
        role = role_result.scalar_one_or_none()
        if not role:
            raise HTTPException(404, f"Role '{body.role_name}' not found")

        service = None
        if body.service_name:
            svc_result = await session.execute(
                select(Service).where(Service.name == body.service_name)
            )
            service = svc_result.scalar_one_or_none()
            if not service:
                raise HTTPException(404, f"Service '{body.service_name}' not found")

        # Check duplicate
        dup_stmt = select(RoleBinding).where(
            RoleBinding.user_id == user.id,
            RoleBinding.role_id == role.id,
            RoleBinding.service_id == (service.id if service else None),
        )
        if (await session.execute(dup_stmt)).scalar_one_or_none():
            raise HTTPException(409, "Binding already exists")

        binding = RoleBinding(
            user_id=user.id,
            role_id=role.id,
            service_id=service.id if service else None,
            granted_by=principal["sub"],
            expires_at=body.expires_at,
        )
        session.add(binding)
        await session.flush()
        await audit(
            principal["sub"], "BIND_ROLE", "binding", f"{username}:{body.role_name}",
            detail={"service": body.service_name},
            ip_address=request.client.host if request.client else None,
        )
        await cache_invalidate_user(username)

        return BindingOut(
            id=binding.id,
            username=username,
            role_name=role.name,
            service_name=service.name if service else None,
            granted_by=binding.granted_by,
            granted_at=binding.granted_at,
            expires_at=binding.expires_at,
        )


# ── Bulk role binding ──────────────────────────────────────

@router.post("/roles/{role_name}/members", response_model=BulkBindResponse,
             status_code=200,
             summary="Bind multiple users to a role in one request",
             description=(
                 "Bind a list of usernames to `role_name` in a single call. "
                 "Each user is processed independently — unknown users and "
                 "duplicate bindings are reported in the results rather than "
                 "aborting the whole batch."
             ))
async def bulk_bind_role(
    role_name: str,
    body: BulkBindRequest,
    request: Request,
    principal: dict = Depends(require_write),
):
    async with db_session() as session:
        role_result = await session.execute(
            select(Role).where(Role.name == role_name)
        )
        role = role_result.scalar_one_or_none()
        if not role:
            raise HTTPException(404, f"Role '{role_name}' not found")

        service = None
        if body.service_name:
            svc_result = await session.execute(
                select(Service).where(Service.name == body.service_name)
            )
            service = svc_result.scalar_one_or_none()
            if not service:
                raise HTTPException(404, f"Service '{body.service_name}' not found")

        results: list[BulkBindResult] = []
        for username in body.usernames:
            user_result = await session.execute(
                select(User).where(User.username == username)
            )
            user = user_result.scalar_one_or_none()
            if not user:
                results.append(BulkBindResult(username=username, status="user_not_found"))
                continue

            dup_stmt = select(RoleBinding).where(
                RoleBinding.user_id == user.id,
                RoleBinding.role_id == role.id,
                RoleBinding.service_id == (service.id if service else None),
            )
            if (await session.execute(dup_stmt)).scalar_one_or_none():
                results.append(BulkBindResult(username=username, status="already_exists"))
                continue

            binding = RoleBinding(
                user_id=user.id,
                role_id=role.id,
                service_id=service.id if service else None,
                granted_by=principal["sub"],
                expires_at=body.expires_at,
            )
            session.add(binding)
            await session.flush()
            await audit(
                principal["sub"], "BIND_ROLE", "binding", f"{username}:{role_name}",
                detail={"service": body.service_name, "bulk": True},
                ip_address=request.client.host if request.client else None,
            )
            await cache_invalidate_user(username)
            results.append(BulkBindResult(username=username, status="bound",
                                          binding_id=binding.id))

    bound   = sum(1 for r in results if r.status == "bound")
    skipped = sum(1 for r in results if r.status == "already_exists")
    errors  = sum(1 for r in results if r.status == "user_not_found")
    return BulkBindResponse(results=results, bound=bound, skipped=skipped, errors=errors)


@router.delete("/{username}/bindings/{binding_id}", response_model=OkResponse,
               summary="Remove a role binding")
async def remove_binding(
    username: str,
    binding_id: int,
    request: Request,
    principal: dict = Depends(require_admin),
):
    async with db_session() as session:
        user = await _get_user(session, username)
        result = await session.execute(
            select(RoleBinding).where(
                RoleBinding.id == binding_id,
                RoleBinding.user_id == user.id,
            )
        )
        binding = result.scalar_one_or_none()
        if not binding:
            raise HTTPException(404, "Binding not found")
        await session.delete(binding)
        await audit(principal["sub"], "UNBIND_ROLE", "binding", str(binding_id),
                    ip_address=request.client.host if request.client else None)
    await cache_invalidate_user(username)
    return OkResponse(message=f"Binding {binding_id} removed")


# ── Role lookup (cache-accelerated) ───────────────────────

@router.get("/{username}/roles", response_model=UserRolesOut,
            summary="Fast lookup of effective roles and permissions for a user")
async def get_user_roles(username: str, principal: dict = Depends(require_read)):
    """
    Returns the resolved set of roles and permissions for `username`.
    Result is served from the two-layer cache (in-process LRU → Redis → DB).
    Designed to be called by sidecars / guards on the hot path.
    """
    from ..database import cache_get_user_roles, cache_set_user_roles

    cached = await cache_get_user_roles(username)
    if cached is not None:
        return UserRolesOut(
            username=username,
            roles=[e["role"] for e in cached],
            permissions=[p for e in cached for p in e["permissions"]],
            cached=True,
        )

    async with db_session() as session:
        user_result = await session.execute(
            select(User).where(User.username == username, User.enabled.is_(True))
        )
        user = user_result.scalar_one_or_none()
        if not user:
            return UserRolesOut(username=username, roles=[], permissions=[], cached=False)

        now = datetime.now(timezone.utc)
        bindings_result = await session.execute(
            select(RoleBinding)
            .options(
                selectinload(RoleBinding.role)
                .selectinload(Role.role_perms)
                .selectinload(RolePermission.permission)
                .selectinload(Permission.service),
                selectinload(RoleBinding.service),
            )
            .where(
                RoleBinding.user_id == user.id,
                (RoleBinding.expires_at.is_(None)) | (RoleBinding.expires_at > now),
            )
        )
        bindings = bindings_result.scalars().all()

        result_list = []
        for b in bindings:
            svc_filter = b.service.name if b.service else None
            perms = []
            for rp in b.role.role_perms:
                if svc_filter and rp.permission.service.name != svc_filter:
                    continue
                perms.append({
                    "service":        rp.permission.service.name,
                    "permission":     rp.permission.name,
                    "resource_scope": rp.resource_scope or {},
                })
            result_list.append({"role": b.role.name, "permissions": perms})

    await cache_set_user_roles(username, result_list)

    return UserRolesOut(
        username=username,
        roles=[e["role"] for e in result_list],
        permissions=[p for e in result_list for p in e["permissions"]],
        cached=False,
    )
