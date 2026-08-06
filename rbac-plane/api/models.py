"""
SQLAlchemy async models for the RBAC metadata store.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, ForeignKey, Index, Integer,
    String, Text, UniqueConstraint, func,
)
from sqlalchemy.dialects.postgresql import ARRAY, INET, JSONB
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


# ── helpers ────────────────────────────────────────────────

def _now():
    return func.now()


# ── Tables ─────────────────────────────────────────────────

class Service(Base):
    __tablename__ = "services"

    id           = Column(Integer, primary_key=True)
    name         = Column(Text, nullable=False, unique=True)
    display_name = Column(Text, nullable=False)
    description  = Column(Text)
    enabled      = Column(Boolean, nullable=False, default=True)
    created_at   = Column(DateTime(timezone=True), server_default=_now())
    updated_at   = Column(DateTime(timezone=True), server_default=_now(), onupdate=_now())

    permissions  = relationship("Permission", back_populates="service", cascade="all,delete")


class Permission(Base):
    __tablename__ = "permissions"
    __table_args__ = (UniqueConstraint("service_id", "name"),)

    id          = Column(Integer, primary_key=True)
    service_id  = Column(Integer, ForeignKey("services.id", ondelete="CASCADE"), nullable=False)
    name        = Column(Text, nullable=False)
    description = Column(Text)
    metadata_   = Column("metadata", JSONB, nullable=False, default=dict)

    service     = relationship("Service", back_populates="permissions")
    role_perms  = relationship("RolePermission", back_populates="permission", cascade="all,delete")


class Role(Base):
    __tablename__ = "roles"

    id           = Column(Integer, primary_key=True)
    name         = Column(Text, nullable=False, unique=True)
    display_name = Column(Text, nullable=False)
    description  = Column(Text)
    created_at   = Column(DateTime(timezone=True), server_default=_now())
    updated_at   = Column(DateTime(timezone=True), server_default=_now(), onupdate=_now())

    role_perms   = relationship("RolePermission", back_populates="role", cascade="all,delete")
    bindings     = relationship("RoleBinding", back_populates="role", cascade="all,delete")


class RolePermission(Base):
    __tablename__  = "role_permissions"
    __table_args__ = (UniqueConstraint("role_id", "permission_id"),)

    role_id        = Column(Integer, ForeignKey("roles.id",       ondelete="CASCADE"), primary_key=True)
    permission_id  = Column(Integer, ForeignKey("permissions.id", ondelete="CASCADE"), primary_key=True)
    resource_scope = Column(JSONB, nullable=False, default=dict)

    role       = relationship("Role",       back_populates="role_perms")
    permission = relationship("Permission", back_populates="role_perms")


class User(Base):
    __tablename__ = "users"

    id           = Column(Integer, primary_key=True)
    username     = Column(Text, nullable=False, unique=True)
    display_name = Column(Text)
    email        = Column(Text)
    enabled      = Column(Boolean, nullable=False, default=True)
    created_at   = Column(DateTime(timezone=True), server_default=_now())
    updated_at   = Column(DateTime(timezone=True), server_default=_now(), onupdate=_now())

    bindings     = relationship("RoleBinding", back_populates="user", cascade="all,delete")
    sync_states  = relationship("SyncState", back_populates="user", cascade="all,delete")


class RoleBinding(Base):
    __tablename__  = "role_bindings"
    __table_args__ = (UniqueConstraint("user_id", "role_id", "service_id"),)

    id          = Column(Integer, primary_key=True)
    user_id     = Column(Integer, ForeignKey("users.id",    ondelete="CASCADE"), nullable=False)
    role_id     = Column(Integer, ForeignKey("roles.id",    ondelete="CASCADE"), nullable=False)
    service_id  = Column(Integer, ForeignKey("services.id", ondelete="CASCADE"))
    granted_by  = Column(Text, nullable=False, default="system")
    granted_at  = Column(DateTime(timezone=True), server_default=_now())
    expires_at  = Column(DateTime(timezone=True))

    user    = relationship("User",    back_populates="bindings")
    role    = relationship("Role",    back_populates="bindings")
    service = relationship("Service")


class SyncState(Base):
    __tablename__ = "sync_state"

    user_id    = Column(Integer, ForeignKey("users.id",    ondelete="CASCADE"), primary_key=True)
    service_id = Column(Integer, ForeignKey("services.id", ondelete="CASCADE"), primary_key=True)
    role_hash  = Column(Text, nullable=False)
    synced_at  = Column(DateTime(timezone=True), server_default=_now())

    user    = relationship("User",    back_populates="sync_states")
    service = relationship("Service")


class AuditLog(Base):
    __tablename__ = "audit_log"

    id          = Column(BigInteger, primary_key=True)
    ts          = Column(DateTime(timezone=True), server_default=_now(), index=True)
    actor       = Column(Text, nullable=False)
    action      = Column(Text, nullable=False)
    target_type = Column(Text)
    target_id   = Column(Text)
    detail      = Column(JSONB, nullable=False, default=dict)
    ip_address  = Column(INET)


class ApiToken(Base):
    __tablename__ = "api_tokens"

    id           = Column(Integer, primary_key=True)
    name         = Column(Text, nullable=False)
    token_hash   = Column(Text, nullable=False, unique=True)
    scopes       = Column(ARRAY(Text), nullable=False, default=list)
    created_by   = Column(Text, nullable=False)
    created_at   = Column(DateTime(timezone=True), server_default=_now())
    last_used_at = Column(DateTime(timezone=True))
    expires_at   = Column(DateTime(timezone=True))
    revoked      = Column(Boolean, nullable=False, default=False)
