"""
/services  — read-only list of registered services and their permissions.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from ..database import db_session
from ..middleware.auth import require_read
from ..models import Permission, Service
from ..schemas import PermissionOut, ServiceOut

router = APIRouter(prefix="/services", tags=["Services"])


@router.get("", response_model=list[ServiceOut], summary="List all registered services")
async def list_services(principal: dict = Depends(require_read)):
    async with db_session() as session:
        result = await session.execute(
            select(Service).order_by(Service.name)
        )
        return result.scalars().all()


@router.get("/{service_name}/permissions", response_model=list[PermissionOut],
            summary="List available permissions for a service")
async def list_permissions(service_name: str, principal: dict = Depends(require_read)):
    async with db_session() as session:
        result = await session.execute(
            select(Permission)
            .join(Service)
            .where(Service.name == service_name)
            .order_by(Permission.name)
        )
        perms = result.scalars().all()
        return [
            PermissionOut(
                id=p.id,
                service_id=p.service_id,
                service_name=service_name,
                name=p.name,
                description=p.description,
                metadata_=p.metadata_ or {},
            )
            for p in perms
        ]
