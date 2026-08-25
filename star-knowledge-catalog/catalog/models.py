"""
Star Knowledge Catalog — SQLAlchemy async models.

Tables:
  data_classifications   — sensitivity tiers (PII, PCI, PHI, CONFIDENTIAL, PUBLIC)
  glossary_terms         — business glossary with auto-detection patterns
  masking_algorithms     — named Doris SQL masking expressions
  masking_policies       — binds classification / term → algorithm
  column_tags            — maps Doris db.table.column → term + classification
  role_masking_exceptions— roles that bypass masking for a classification
  doris_masked_views     — audit trail of applied masked views in Doris
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger, Boolean, CheckConstraint, Column, DateTime,
    ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint, func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


def _now():
    return func.now()


# ── Data Classifications ─────────────────────────────────────────────────────

class DataClassification(Base):
    __tablename__ = "data_classifications"
    __table_args__ = (
        CheckConstraint("sensitivity IN ('low','medium','high','critical')",
                        name="ck_classification_sensitivity"),
    )

    id           = Column(Integer, primary_key=True)
    name         = Column(Text, nullable=False, unique=True)  # "PII"
    display_name = Column(Text, nullable=False)
    description  = Column(Text)
    sensitivity  = Column(Text, nullable=False, default="medium")
    color_hex    = Column(Text)
    created_at   = Column(DateTime(timezone=True), server_default=_now())
    updated_at   = Column(DateTime(timezone=True), server_default=_now(), onupdate=_now())

    glossary_terms      = relationship("GlossaryTerm",       back_populates="classification")
    masking_policies    = relationship("MaskingPolicy",      back_populates="classification",
                                       foreign_keys="MaskingPolicy.classification_id")
    role_exceptions     = relationship("RoleMaskingException", back_populates="classification")
    column_tags         = relationship("ColumnTag",          back_populates="classification")


# ── Glossary Terms ───────────────────────────────────────────────────────────

class GlossaryTerm(Base):
    __tablename__ = "glossary_terms"

    id                   = Column(Integer, primary_key=True)
    name                 = Column(Text, nullable=False, unique=True)  # "email_address"
    display_name         = Column(Text, nullable=False)
    description          = Column(Text)
    classification_id    = Column(Integer, ForeignKey("data_classifications.id",
                                                       ondelete="SET NULL"))
    # JSON array of lowercase column-name keywords used for auto-detection
    column_name_patterns = Column(ARRAY(Text), nullable=False, default=list)
    # JSON array of lowercase keywords checked against column descriptions
    description_patterns = Column(ARRAY(Text), nullable=False, default=list)
    steward              = Column(Text)   # data steward username
    created_at           = Column(DateTime(timezone=True), server_default=_now())
    updated_at           = Column(DateTime(timezone=True), server_default=_now(), onupdate=_now())

    classification  = relationship("DataClassification", back_populates="glossary_terms")
    masking_policies = relationship("MaskingPolicy", back_populates="glossary_term",
                                    foreign_keys="MaskingPolicy.glossary_term_id")
    column_tags     = relationship("ColumnTag", back_populates="glossary_term")


# ── Masking Algorithms ───────────────────────────────────────────────────────

class MaskingAlgorithm(Base):
    __tablename__ = "masking_algorithms"
    __table_args__ = (
        CheckConstraint(
            "algorithm_type IN ('REDACT','HASH','PARTIAL_MASK','TOKENIZE',"
            "'DATE_GENERALIZE','NULL_OUT','CUSTOM')",
            name="ck_algorithm_type",
        ),
    )

    id               = Column(Integer, primary_key=True)
    name             = Column(Text, nullable=False, unique=True)  # "SHA256_HASH"
    display_name     = Column(Text, nullable=False)
    description      = Column(Text)
    algorithm_type   = Column(Text, nullable=False)
    # Doris SQL expression; use {col} as the column placeholder
    # e.g.  SHA2({col}, 256)   or   CONCAT(LEFT({col},2),'***')
    doris_expression = Column(Text, nullable=False)
    applicable_types = Column(ARRAY(Text), nullable=False,
                              default=lambda: ["VARCHAR", "TEXT", "STRING"])
    created_at       = Column(DateTime(timezone=True), server_default=_now())
    updated_at       = Column(DateTime(timezone=True), server_default=_now(), onupdate=_now())

    masking_policies = relationship("MaskingPolicy", back_populates="algorithm")


# ── Masking Policies ─────────────────────────────────────────────────────────

class MaskingPolicy(Base):
    __tablename__ = "masking_policies"
    __table_args__ = (
        CheckConstraint(
            "(classification_id IS NOT NULL AND glossary_term_id IS NULL) OR "
            "(classification_id IS NULL AND glossary_term_id IS NOT NULL)",
            name="ck_policy_single_target",
        ),
    )

    id                  = Column(Integer, primary_key=True)
    name                = Column(Text, nullable=False, unique=True)
    description         = Column(Text)
    classification_id   = Column(Integer, ForeignKey("data_classifications.id",
                                                      ondelete="CASCADE"))
    glossary_term_id    = Column(Integer, ForeignKey("glossary_terms.id",
                                                     ondelete="CASCADE"))
    algorithm_id        = Column(Integer, ForeignKey("masking_algorithms.id"),
                                 nullable=False)
    # Higher priority wins when multiple policies match a column
    priority            = Column(Integer, nullable=False, default=100)
    enabled             = Column(Boolean, nullable=False, default=True)
    created_at          = Column(DateTime(timezone=True), server_default=_now())
    updated_at          = Column(DateTime(timezone=True), server_default=_now(), onupdate=_now())

    classification  = relationship("DataClassification", back_populates="masking_policies",
                                   foreign_keys=[classification_id])
    glossary_term   = relationship("GlossaryTerm",        back_populates="masking_policies",
                                   foreign_keys=[glossary_term_id])
    algorithm       = relationship("MaskingAlgorithm",    back_populates="masking_policies")


# ── Column Tags ──────────────────────────────────────────────────────────────

class ColumnTag(Base):
    __tablename__ = "column_tags"
    __table_args__ = (
        UniqueConstraint("doris_database", "doris_table", "column_name",
                         name="uq_column_tag"),
    )

    id                  = Column(Integer, primary_key=True)
    doris_database      = Column(Text, nullable=False)
    doris_table         = Column(Text, nullable=False)
    column_name         = Column(Text, nullable=False)
    glossary_term_id    = Column(Integer, ForeignKey("glossary_terms.id",
                                                     ondelete="SET NULL"))
    classification_id   = Column(Integer, ForeignKey("data_classifications.id",
                                                      ondelete="SET NULL"))
    auto_detected       = Column(Boolean, nullable=False, default=False)
    detection_score     = Column(Numeric(3, 2))  # 0.00–1.00
    override_reason     = Column(Text)
    created_at          = Column(DateTime(timezone=True), server_default=_now())
    updated_at          = Column(DateTime(timezone=True), server_default=_now(), onupdate=_now())

    glossary_term   = relationship("GlossaryTerm",       back_populates="column_tags")
    classification  = relationship("DataClassification", back_populates="column_tags")


# ── Role Masking Exceptions ───────────────────────────────────────────────────

class RoleMaskingException(Base):
    """
    When a role is listed here for a given classification the engine emits
    CLEAR (base-table) SELECT grants instead of masked-view grants.
    """
    __tablename__ = "role_masking_exceptions"
    __table_args__ = (
        UniqueConstraint("role_name", "classification_id",
                         name="uq_role_masking_exception"),
    )

    id                = Column(Integer, primary_key=True)
    # role_name mirrors the name in the RBAC Control Plane's roles table.
    # We store by name (not FK) to avoid tight coupling.
    role_name         = Column(Text, nullable=False)
    classification_id = Column(Integer,
                               ForeignKey("data_classifications.id", ondelete="CASCADE"),
                               nullable=False)
    granted_by        = Column(Text, nullable=False, default="system")
    granted_at        = Column(DateTime(timezone=True), server_default=_now())

    classification = relationship("DataClassification", back_populates="role_exceptions")


# ── Doris Masked View Manifest ────────────────────────────────────────────────

class DorisViewManifest(Base):
    """
    Records the last DDL applied to Doris for each (database, base_table) pair.
    Used for drift detection — if the checksum changes the engine re-applies.
    """
    __tablename__ = "doris_masked_views"
    __table_args__ = (
        UniqueConstraint("doris_database", "base_table",
                         name="uq_doris_view_manifest"),
    )

    id              = Column(Integer, primary_key=True)
    doris_database  = Column(Text, nullable=False)
    base_table      = Column(Text, nullable=False)
    view_name       = Column(Text, nullable=False)   # e.g. "customers_masked"
    view_ddl        = Column(Text, nullable=False)   # last CREATE OR REPLACE DDL
    columns_masked  = Column(ARRAY(Text), nullable=False, default=list)
    view_checksum   = Column(Text)                   # MD5 of view_ddl for drift detection
    last_applied_at = Column(DateTime(timezone=True), server_default=_now(),
                             onupdate=_now())
