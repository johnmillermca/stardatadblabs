"""
Star Knowledge Catalog — Pydantic request/response schemas.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


# ── Shared ──────────────────────────────────────────────────────────────────

class OkResponse(BaseModel):
    ok: bool = True
    message: str = "success"


# ── Data Classifications ─────────────────────────────────────────────────────

class ClassificationCreate(BaseModel):
    name: str = Field(..., pattern=r"^[A-Z0-9_\-]+$", max_length=60)
    display_name: str
    description: Optional[str] = None
    sensitivity: str = Field("medium", pattern=r"^(low|medium|high|critical)$")
    color_hex: Optional[str] = Field(None, pattern=r"^#[0-9A-Fa-f]{6}$")


class ClassificationUpdate(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    sensitivity: Optional[str] = Field(None, pattern=r"^(low|medium|high|critical)$")
    color_hex: Optional[str] = Field(None, pattern=r"^#[0-9A-Fa-f]{6}$")


class ClassificationOut(BaseModel):
    id: int
    name: str
    display_name: str
    description: Optional[str]
    sensitivity: str
    color_hex: Optional[str]
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Glossary Terms ───────────────────────────────────────────────────────────

class GlossaryTermCreate(BaseModel):
    name: str = Field(..., pattern=r"^[a-z0-9_]+$", max_length=80)
    display_name: str
    description: Optional[str] = None
    classification_id: Optional[int] = None
    column_name_patterns: list[str] = Field(default_factory=list)
    description_patterns: list[str] = Field(default_factory=list)
    steward: Optional[str] = None


class GlossaryTermUpdate(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    classification_id: Optional[int] = None
    column_name_patterns: Optional[list[str]] = None
    description_patterns: Optional[list[str]] = None
    steward: Optional[str] = None


class GlossaryTermOut(BaseModel):
    id: int
    name: str
    display_name: str
    description: Optional[str]
    classification_id: Optional[int]
    classification_name: Optional[str] = None
    column_name_patterns: list[str]
    description_patterns: list[str]
    steward: Optional[str]
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Masking Algorithms ────────────────────────────────────────────────────────

class AlgorithmCreate(BaseModel):
    name: str = Field(..., pattern=r"^[A-Z0-9_]+$", max_length=60)
    display_name: str
    description: Optional[str] = None
    algorithm_type: str = Field(
        ...,
        pattern=r"^(REDACT|HASH|PARTIAL_MASK|TOKENIZE|DATE_GENERALIZE|NULL_OUT|CUSTOM)$",
    )
    doris_expression: str = Field(
        ...,
        description="Doris SQL expression with {col} placeholder, e.g. SHA2({col}, 256)",
    )
    applicable_types: list[str] = Field(
        default_factory=lambda: ["VARCHAR", "TEXT", "STRING"]
    )


class AlgorithmUpdate(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    doris_expression: Optional[str] = None
    applicable_types: Optional[list[str]] = None


class AlgorithmOut(BaseModel):
    id: int
    name: str
    display_name: str
    description: Optional[str]
    algorithm_type: str
    doris_expression: str
    applicable_types: list[str]
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Masking Policies ─────────────────────────────────────────────────────────

class PolicyCreate(BaseModel):
    name: str = Field(..., pattern=r"^[a-z0-9_\-]+$", max_length=80)
    description: Optional[str] = None
    classification_id: Optional[int] = None
    glossary_term_id: Optional[int] = None
    algorithm_id: int
    priority: int = 100
    enabled: bool = True

    @field_validator("glossary_term_id")
    @classmethod
    def exactly_one_target(cls, v, info):
        cid = info.data.get("classification_id")
        if (cid is None) == (v is None):
            raise ValueError(
                "Exactly one of classification_id or glossary_term_id must be set"
            )
        return v


class PolicyUpdate(BaseModel):
    description: Optional[str] = None
    algorithm_id: Optional[int] = None
    priority: Optional[int] = None
    enabled: Optional[bool] = None


class PolicyOut(BaseModel):
    id: int
    name: str
    description: Optional[str]
    classification_id: Optional[int]
    glossary_term_id: Optional[int]
    algorithm_id: int
    algorithm_name: Optional[str] = None
    priority: int
    enabled: bool
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Column Tags ──────────────────────────────────────────────────────────────

class ColumnTagCreate(BaseModel):
    doris_database: str
    doris_table: str
    column_name: str
    glossary_term_id: Optional[int] = None
    classification_id: Optional[int] = None
    override_reason: Optional[str] = None


class ColumnTagOut(BaseModel):
    id: int
    doris_database: str
    doris_table: str
    column_name: str
    glossary_term_id: Optional[int]
    glossary_term_name: Optional[str] = None
    classification_id: Optional[int]
    classification_name: Optional[str] = None
    auto_detected: bool
    detection_score: Optional[float]
    override_reason: Optional[str]
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Auto-classify scan ────────────────────────────────────────────────────────

class ScanRequest(BaseModel):
    doris_database: str = Field(..., description="Doris database to scan")
    doris_table: Optional[str] = Field(
        None,
        description="If set, scan only this table. Omit to scan all tables in the database.",
    )
    dry_run: bool = Field(
        False,
        description="If true, return detected tags without persisting them.",
    )
    overwrite_existing: bool = Field(
        False,
        description="If true, overwrite existing auto-detected tags. Manual tags are never overwritten.",
    )


class ScanColumnResult(BaseModel):
    column_name: str
    matched_term: Optional[str]
    matched_classification: Optional[str]
    score: float
    action: str  # "tagged" | "skipped" | "dry_run"


class ScanTableResult(BaseModel):
    doris_database: str
    doris_table: str
    columns_scanned: int
    columns_tagged: int
    results: list[ScanColumnResult]


class ScanResponse(BaseModel):
    tables_scanned: int
    tables_results: list[ScanTableResult]


# ── Masking view apply ────────────────────────────────────────────────────────

class ApplyMaskingRequest(BaseModel):
    doris_database: str
    doris_table: Optional[str] = Field(
        None,
        description="Apply to one table. Omit for all tagged tables in the database.",
    )
    dry_run: bool = False
    force: bool = Field(
        False,
        description="Re-apply even if view DDL checksum has not changed.",
    )


class ApplyMaskingResult(BaseModel):
    doris_database: str
    doris_table: str
    view_name: str
    columns_masked: list[str]
    action: str   # "created" | "updated" | "unchanged" | "dry_run" | "error"
    detail: Optional[str] = None


class ApplyMaskingResponse(BaseModel):
    tables_processed: int
    results: list[ApplyMaskingResult]


# ── Masking Query (role-aware) ────────────────────────────────────────────────

class MaskedQueryRequest(BaseModel):
    username: str = Field(..., description="Username from RBAC Control Plane")
    doris_database: str
    doris_table: str
    columns: Optional[list[str]] = Field(
        None,
        description="Columns to select. Omit or use ['*'] for all.",
    )
    where_clause: Optional[str] = Field(
        None,
        description="Optional SQL WHERE predicate (without the WHERE keyword).",
    )
    limit: int = Field(1000, ge=1, le=50_000)


class MaskedQueryResponse(BaseModel):
    username: str
    role: str
    target: str   # "base_table" | "masked_view"
    sql: str      # the generated SELECT statement
    columns_masked: list[str]
    columns_clear: list[str]
    note: str


# ── Role Masking Exceptions ────────────────────────────────────────────────────

class RoleExceptionCreate(BaseModel):
    role_name: str
    classification_id: int
    granted_by: str = "admin"


class RoleExceptionOut(BaseModel):
    id: int
    role_name: str
    classification_id: int
    classification_name: Optional[str] = None
    granted_by: str
    granted_at: datetime

    model_config = {"from_attributes": True}


# ── Auth ──────────────────────────────────────────────────────────────────────

class TokenRequest(BaseModel):
    token: str = Field(..., description="Raw API token")


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
