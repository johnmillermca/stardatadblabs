"""
Pydantic schemas — request / response models for the REST API.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


# ── Shared ─────────────────────────────────────────────────

class OkResponse(BaseModel):
    ok: bool = True
    message: str = "success"


# ── Services ───────────────────────────────────────────────

class ServiceOut(BaseModel):
    id: int
    name: str
    display_name: str
    description: Optional[str]
    enabled: bool

    model_config = {"from_attributes": True}


# ── Permissions ────────────────────────────────────────────

class PermissionOut(BaseModel):
    id: int
    service_id: int
    service_name: Optional[str] = None
    name: str
    description: Optional[str]
    metadata: dict = Field(default_factory=dict, alias="metadata_")

    model_config = {"from_attributes": True, "populate_by_name": True}


# ── Roles ──────────────────────────────────────────────────

class RolePermissionIn(BaseModel):
    permission_name: str = Field(
        ...,
        description=(
            "Permission to grant, in 'service:NAME' format. "
            "Example: 'doris:SELECT', 'kafka:CONSUME', 'opensearch:INDEX_READ'."
        ),
        pattern=r"^[a-z0-9_\-]+:[A-Z0-9_]+$",
    )
    resource_scope: dict = Field(default_factory=dict)


class RoleCreate(BaseModel):
    name: str = Field(..., pattern=r"^[a-z0-9_\-]+$", max_length=80)
    display_name: str = Field(..., max_length=120)
    description: Optional[str] = None
    permissions: list[RolePermissionIn] = Field(default_factory=list)


class RoleUpdate(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None


class RolePermissionOut(BaseModel):
    permission_id: int
    permission_name: str
    service_name: str
    resource_scope: dict

    model_config = {"from_attributes": True}


class RoleOut(BaseModel):
    id: int
    name: str
    display_name: str
    description: Optional[str]
    created_at: datetime
    permissions: list[RolePermissionOut] = Field(default_factory=list)

    model_config = {"from_attributes": True}


# ── Users ──────────────────────────────────────────────────

class UserCreate(BaseModel):
    username: str = Field(..., pattern=r"^[a-z0-9_\-\.]+$", max_length=80)
    display_name: Optional[str] = None
    email: Optional[str] = None


class UserUpdate(BaseModel):
    display_name: Optional[str] = None
    email: Optional[str] = None
    enabled: Optional[bool] = None


class UserOut(BaseModel):
    id: int
    username: str
    display_name: Optional[str]
    email: Optional[str]
    enabled: bool
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Role Bindings ──────────────────────────────────────────

class BindingCreate(BaseModel):
    role_name: str
    service_name: Optional[str] = Field(
        None,
        description="Restrict binding to one service. Omit for all services.",
    )
    expires_at: Optional[datetime] = None


class BulkBindRequest(BaseModel):
    usernames: list[str] = Field(
        ...,
        min_length=1,
        description="List of usernames to bind to the role.",
    )
    service_name: Optional[str] = Field(
        None,
        description="Restrict binding to one service. Omit for all services.",
    )
    expires_at: Optional[datetime] = None


class BulkBindResult(BaseModel):
    username: str
    status: str          # "bound" | "already_exists" | "user_not_found"
    binding_id: Optional[int] = None


class BulkBindResponse(BaseModel):
    results: list[BulkBindResult]
    bound: int
    skipped: int
    errors: int


class BindingOut(BaseModel):
    id: int
    username: str
    role_name: str
    service_name: Optional[str]
    granted_by: str
    granted_at: datetime
    expires_at: Optional[datetime]

    model_config = {"from_attributes": True}


# ── Sync ───────────────────────────────────────────────────

class SyncRequest(BaseModel):
    username: Optional[str] = Field(None, description="Sync one user. Omit for full sync.")
    service:  Optional[str] = Field(None, description="Sync to one service. Omit for all.")
    dry_run: bool = False


class SyncResult(BaseModel):
    username: str
    service: str
    status: str          # "synced" | "skipped" | "error"
    detail: Optional[str] = None


class SyncResponse(BaseModel):
    results: list[SyncResult]
    errors: int


# ── Lookup (cache-accelerated) ────────────────────────────

class UserRolesOut(BaseModel):
    username: str
    roles: list[str]
    permissions: list[dict]   # {service, name, resource_scope}
    cached: bool


# ── Auth ───────────────────────────────────────────────────

class TokenRequest(BaseModel):
    token: str = Field(..., description="Raw API token")


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class ApiTokenCreate(BaseModel):
    name: str
    scopes: list[str] = Field(default_factory=lambda: ["read", "write"])
    expires_days: Optional[int] = None


class ApiTokenOut(BaseModel):
    id: int
    name: str
    raw_token: Optional[str] = None    # only set on creation
    scopes: list[str]
    created_by: str
    created_at: datetime
    expires_at: Optional[datetime]
    revoked: bool

    model_config = {"from_attributes": True}


# ── Audit ──────────────────────────────────────────────────

class AuditEntry(BaseModel):
    id: int
    ts: datetime
    actor: str
    action: str
    target_type: Optional[str]
    target_id: Optional[str]
    detail: dict

    model_config = {"from_attributes": True}
