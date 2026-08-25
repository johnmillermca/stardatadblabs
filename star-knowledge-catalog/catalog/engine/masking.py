"""
Star Knowledge Catalog — Masking View Builder.

For each Doris table that has tagged columns this engine:

  1. Reads all ColumnTag rows for (database, table) from PostgreSQL.
  2. Resolves the applicable MaskingPolicy for each tagged column
     (term-level policy takes precedence over classification-level policy).
  3. Generates a CREATE OR REPLACE VIEW DDL that:
       • replaces each sensitive column with the masking expression
       • passes all other columns through unchanged
  4. Applies the DDL to Doris via the MySQL-protocol connector.
  5. Grants SELECT on the masked view to the 'analyst'@'%' Doris user.
  6. Persists the view manifest (DDL + checksum) to PostgreSQL.

Performance guarantee
---------------------
Masked views are pre-computed DDL in Doris — the masking expressions are
evaluated at query time by the Doris vectorised engine exactly like any other
projection. There is no Python in the query path. Sub-second performance is
preserved because:
  • The view definition only adds cheap scalar functions (SHA2, CONCAT, etc.).
  • Vectorised execution is fully native in Doris 4.x (Apache Arrow / Velox).
  • Predicate push-down, partition pruning, and all Doris optimisations apply.
"""
from __future__ import annotations

import hashlib
import logging
from typing import Optional

import aiomysql
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from ..config import get_settings
from ..database import db_session, cache_invalidate_prefix, POLICY_CACHE_PREFIX
from ..models import (
    ColumnTag,
    DataClassification,
    DorisViewManifest,
    GlossaryTerm,
    MaskingAlgorithm,
    MaskingPolicy,
)

log = logging.getLogger(__name__)


# ── Doris connection helper ──────────────────────────────────────────────────

async def _doris_conn() -> aiomysql.Connection:
    s = get_settings()
    return await aiomysql.connect(
        host=s.doris_host,
        port=s.doris_port,
        user=s.doris_admin_user,
        password=s.doris_admin_password,
        db="information_schema",
        connect_timeout=5,
        charset="utf8mb4",
    )


async def _doris_execute(sql: str) -> None:
    conn = await _doris_conn()
    try:
        async with conn.cursor() as cur:
            await cur.execute(sql)
        await conn.commit()
    finally:
        conn.close()


# ── Column metadata from Doris ───────────────────────────────────────────────

async def _get_column_order(database: str, table: str) -> list[str]:
    """Return columns in their original ordinal order from information_schema."""
    conn = await _doris_conn()
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
                "ORDER BY ORDINAL_POSITION",
                (database, table),
            )
            rows = await cur.fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


# ── Policy resolution ────────────────────────────────────────────────────────

async def _resolve_policy_for_column(tag: ColumnTag) -> Optional[MaskingAlgorithm]:
    """
    Return the highest-priority active MaskingAlgorithm for *tag*.

    Resolution order (higher priority wins):
      1. Term-level policy   (priority from masking_policies.priority)
      2. Classification-level policy
    On equal priority, term-level always wins (more specific).
    """
    async with db_session() as session:
        policies: list[MaskingPolicy] = []

        if tag.glossary_term_id is not None:
            result = await session.execute(
                select(MaskingPolicy)
                .where(
                    MaskingPolicy.glossary_term_id == tag.glossary_term_id,
                    MaskingPolicy.enabled.is_(True),
                )
                .order_by(MaskingPolicy.priority.desc())
                .limit(1)
            )
            p = result.scalar_one_or_none()
            if p:
                policies.append(p)

        if tag.classification_id is not None:
            result = await session.execute(
                select(MaskingPolicy)
                .where(
                    MaskingPolicy.classification_id == tag.classification_id,
                    MaskingPolicy.enabled.is_(True),
                )
                .order_by(MaskingPolicy.priority.desc())
                .limit(1)
            )
            p = result.scalar_one_or_none()
            if p:
                policies.append(p)

        if not policies:
            return None

        # Highest priority wins; term-level tie-breaks over class-level
        best = max(
            policies,
            key=lambda p: (p.priority, 1 if p.glossary_term_id is not None else 0),
        )

        algo_result = await session.execute(
            select(MaskingAlgorithm).where(MaskingAlgorithm.id == best.algorithm_id)
        )
        return algo_result.scalar_one_or_none()


# ── DDL generation ───────────────────────────────────────────────────────────

def _build_view_ddl(
    database: str,
    table: str,
    view_name: str,
    all_columns: list[str],
    masked_columns: dict[str, str],  # column_name → doris_expression (with {col} filled)
) -> str:
    """
    Generate:
      CREATE OR REPLACE VIEW `<database>`.`<view_name>` AS
      SELECT
        `col1`,
        <expr> AS `col2`,   -- masked
        `col3`,
        ...
      FROM `<database>`.`<table>`;
    """
    col_exprs = []
    for col in all_columns:
        if col in masked_columns:
            expr = masked_columns[col]
            col_exprs.append(f"  {expr} AS `{col}`")
        else:
            col_exprs.append(f"  `{col}`")

    cols_sql = ",\n".join(col_exprs)
    ddl = (
        f"CREATE OR REPLACE VIEW `{database}`.`{view_name}` AS\n"
        f"SELECT\n"
        f"{cols_sql}\n"
        f"FROM `{database}`.`{table}`;"
    )
    return ddl


# ── Grant helper ─────────────────────────────────────────────────────────────

async def _grant_view_to_user(database: str, view_name: str, doris_user: str) -> None:
    """Grant SELECT on the masked view to *doris_user*@'%'."""
    sql = (
        f"GRANT SELECT_PRIV ON `{database}`.`{view_name}` "
        f"TO '{doris_user}'@'%';"
    )
    try:
        await _doris_execute(sql)
        log.debug("Masking: granted SELECT on %s.%s to %s", database, view_name, doris_user)
    except Exception as exc:
        log.warning(
            "Masking: could not grant view %s.%s to %s: %s",
            database, view_name, doris_user, exc,
        )


# ── Public API ───────────────────────────────────────────────────────────────

async def apply_masking_view(
    database: str,
    table: str,
    dry_run: bool = False,
    force: bool = False,
) -> dict:
    """
    Build and apply a masked view for *database*.*table*.

    Returns a result dict with keys:
      view_name, columns_masked, action, detail
    """
    s = get_settings()
    view_name = f"{table}{s.masked_view_suffix}"

    # Load column tags for this table
    async with db_session() as session:
        result = await session.execute(
            select(ColumnTag).where(
                ColumnTag.doris_database == database,
                ColumnTag.doris_table == table,
            )
        )
        tags: list[ColumnTag] = result.scalars().all()

    if not tags:
        return {
            "view_name": view_name,
            "columns_masked": [],
            "action": "skipped",
            "detail": "No column tags found — nothing to mask",
        }

    # Resolve algorithm for each tagged column
    masked_exprs: dict[str, str] = {}
    for tag in tags:
        algo = await _resolve_policy_for_column(tag)
        if algo is None:
            log.debug("No active policy for %s.%s.%s — skipping", database, table, tag.column_name)
            continue
        # Substitute {col} placeholder with the backtick-quoted column name
        expr = algo.doris_expression.replace("{col}", f"`{tag.column_name}`")
        masked_exprs[tag.column_name] = expr

    if not masked_exprs:
        return {
            "view_name": view_name,
            "columns_masked": [],
            "action": "skipped",
            "detail": "Column tags found but no active masking policies matched",
        }

    # Get full column order from Doris
    try:
        all_columns = await _get_column_order(database, table)
    except Exception as exc:
        return {
            "view_name": view_name,
            "columns_masked": list(masked_exprs.keys()),
            "action": "error",
            "detail": f"Could not fetch column metadata from Doris: {exc}",
        }

    # Generate DDL
    ddl = _build_view_ddl(database, table, view_name, all_columns, masked_exprs)
    checksum = hashlib.md5(ddl.encode()).hexdigest()

    if dry_run:
        return {
            "view_name": view_name,
            "columns_masked": list(masked_exprs.keys()),
            "action": "dry_run",
            "detail": ddl,
        }

    # Check if view already exists and DDL hasn't changed
    async with db_session() as session:
        existing = await session.execute(
            select(DorisViewManifest).where(
                DorisViewManifest.doris_database == database,
                DorisViewManifest.base_table == table,
            )
        )
        manifest = existing.scalar_one_or_none()

    if manifest and manifest.view_checksum == checksum and not force:
        return {
            "view_name": view_name,
            "columns_masked": list(masked_exprs.keys()),
            "action": "unchanged",
            "detail": "DDL checksum matches — view is current",
        }

    # Apply to Doris
    action = "updated" if manifest else "created"
    try:
        await _doris_execute(ddl)
        log.info("Masking: %s view %s.%s", action, database, view_name)
    except Exception as exc:
        return {
            "view_name": view_name,
            "columns_masked": list(masked_exprs.keys()),
            "action": "error",
            "detail": str(exc),
        }

    # Grant to analyst Doris user (best-effort)
    await _grant_view_to_user(database, view_name, "analyst")

    # Persist manifest
    async with db_session() as session:
        if manifest:
            manifest.view_ddl = ddl
            manifest.view_checksum = checksum
            manifest.columns_masked = list(masked_exprs.keys())
            session.add(manifest)
        else:
            session.add(
                DorisViewManifest(
                    doris_database=database,
                    base_table=table,
                    view_name=view_name,
                    view_ddl=ddl,
                    columns_masked=list(masked_exprs.keys()),
                    view_checksum=checksum,
                )
            )

    # Invalidate policy cache so the next query resolution uses fresh data
    await cache_invalidate_prefix(POLICY_CACHE_PREFIX)

    return {
        "view_name": view_name,
        "columns_masked": list(masked_exprs.keys()),
        "action": action,
        "detail": None,
    }


async def resolve_query_target(
    username: str,
    database: str,
    table: str,
    roles: list[str],
) -> dict:
    """
    Decide whether *username* should query the base table or the masked view.

    Returns:
      {
        "target":          "base_table" | "masked_view",
        "view_name":       str,
        "columns_masked":  list[str],   # empty when base_table
        "role":            str,         # highest-privilege role used
      }

    Logic:
      • If any of the user's roles has a masking exception for ALL classifications
        present in the table's tagged columns → route to base_table.
      • Otherwise → route to masked_view.
    """
    s = get_settings()
    view_name = f"{table}{s.masked_view_suffix}"

    # Fetch tags for this table
    async with db_session() as session:
        tags_result = await session.execute(
            select(ColumnTag).where(
                ColumnTag.doris_database == database,
                ColumnTag.doris_table == table,
            )
        )
        tags: list[ColumnTag] = tags_result.scalars().all()

    if not tags:
        # No sensitive columns — direct base table access
        return {
            "target": "base_table",
            "view_name": view_name,
            "columns_masked": [],
            "role": roles[0] if roles else "unknown",
        }

    # Collect unique classifications in the table
    classification_ids = {
        t.classification_id for t in tags if t.classification_id is not None
    }

    # Check role masking exceptions in PostgreSQL
    from ..models import RoleMaskingException
    async with db_session() as session:
        exempt_result = await session.execute(
            select(RoleMaskingException).where(
                RoleMaskingException.role_name.in_(roles),
                RoleMaskingException.classification_id.in_(classification_ids),
            )
        )
        exempt_rows = exempt_result.scalars().all()

    exempt_class_ids = {e.classification_id for e in exempt_rows}
    has_full_exception = classification_ids.issubset(exempt_class_ids)

    # Determine highest role for logging
    priority_order = [
        "data_admin", "platform_admin", "account_admin",
        "data_engineer", "iceberg_engineer", "etl_writer",
        "analyst", "kafka_consumer", "snowflake_reader", "spark_user",
    ]
    role = next((r for r in priority_order if r in roles), roles[0] if roles else "unknown")

    # Fetch columns_masked from manifest
    async with db_session() as session:
        manifest_result = await session.execute(
            select(DorisViewManifest).where(
                DorisViewManifest.doris_database == database,
                DorisViewManifest.base_table == table,
            )
        )
        manifest = manifest_result.scalar_one_or_none()
    masked_cols = manifest.columns_masked if manifest else []

    if has_full_exception:
        return {
            "target": "base_table",
            "view_name": view_name,
            "columns_masked": [],
            "role": role,
        }

    return {
        "target": "masked_view",
        "view_name": view_name,
        "columns_masked": masked_cols,
        "role": role,
    }
