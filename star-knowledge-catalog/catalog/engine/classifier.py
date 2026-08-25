"""
Star Knowledge Catalog — Auto Column Classifier.

Scans Doris column names for a given table and matches them against the
glossary term patterns stored in PostgreSQL.  The classifier is entirely
string-based (no LLM dependency) — it uses substring matching against the
curated column_name_patterns list for each glossary term.

Scoring
-------
  1.0  — exact match between column name and a pattern token
  0.9  — column name contains a pattern token as a full word (word boundary)
  0.7  — column name contains a pattern token as a substring
  0.0  — no match

The highest score across all patterns for a term wins.  A column is tagged
when score >= settings.auto_classify_threshold (default 0.70).

When multiple terms match the same column, the highest-scoring term wins.
On a tie the term whose classification has the higher sensitivity is preferred
(critical > high > medium > low).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

import aiomysql

from ..config import get_settings
from ..database import db_session
from ..models import DataClassification, GlossaryTerm

log = logging.getLogger(__name__)

_SENSITIVITY_RANK = {"low": 0, "medium": 1, "high": 2, "critical": 3}


@dataclass
class ColumnMatch:
    column_name: str
    term_id: Optional[int] = None
    term_name: Optional[str] = None
    classification_id: Optional[int] = None
    classification_name: Optional[str] = None
    score: float = 0.0


@dataclass
class TermSpec:
    id: int
    name: str
    classification_id: Optional[int]
    classification_name: Optional[str]
    classification_sensitivity: str
    patterns: list[str] = field(default_factory=list)


async def _load_term_specs() -> list[TermSpec]:
    """Load all glossary terms and their classification from PostgreSQL."""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    async with db_session() as session:
        result = await session.execute(
            select(GlossaryTerm).options(
                selectinload(GlossaryTerm.classification)
            )
        )
        terms = result.scalars().all()

    specs = []
    for t in terms:
        cls = t.classification
        specs.append(
            TermSpec(
                id=t.id,
                name=t.name,
                classification_id=t.classification_id,
                classification_name=cls.name if cls else None,
                classification_sensitivity=cls.sensitivity if cls else "low",
                patterns=[p.lower() for p in (t.column_name_patterns or [])],
            )
        )
    return specs


def _score_column(col_lower: str, patterns: list[str]) -> float:
    """Return the highest match score for col_lower against a list of patterns."""
    best = 0.0
    for pat in patterns:
        if not pat:
            continue
        if col_lower == pat:
            return 1.0
        # word-boundary match: surrounded by _ or start/end
        if re.search(r"(?<![a-z0-9])" + re.escape(pat) + r"(?![a-z0-9])", col_lower):
            best = max(best, 0.9)
        elif pat in col_lower:
            best = max(best, 0.7)
    return best


async def _get_doris_columns(database: str, table: str) -> list[str]:
    """Fetch column names from Doris information_schema."""
    s = get_settings()
    conn = await aiomysql.connect(
        host=s.doris_host,
        port=s.doris_port,
        user=s.doris_admin_user,
        password=s.doris_admin_password,
        db="information_schema",
        connect_timeout=5,
        charset="utf8mb4",
    )
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


async def _get_doris_tables(database: str) -> list[str]:
    """Fetch all base table names in *database* (exclude views and masked views)."""
    s = get_settings()
    suffix = s.masked_view_suffix
    conn = await aiomysql.connect(
        host=s.doris_host,
        port=s.doris_port,
        user=s.doris_admin_user,
        password=s.doris_admin_password,
        db="information_schema",
        connect_timeout=5,
        charset="utf8mb4",
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'",
                (database,),
            )
            rows = await cur.fetchall()
        return [r[0] for r in rows if not r[0].endswith(suffix)]
    finally:
        conn.close()


def classify_columns(
    column_names: list[str],
    term_specs: list[TermSpec],
    threshold: float,
) -> list[ColumnMatch]:
    """
    Pure-Python classification of column names against term specs.
    Returns one ColumnMatch per column (unmatched columns have score=0).
    """
    results: list[ColumnMatch] = []
    for col in column_names:
        col_lower = col.lower().replace(" ", "_")
        best_match = ColumnMatch(column_name=col)

        for ts in term_specs:
            score = _score_column(col_lower, ts.patterns)
            if score < threshold:
                continue
            # Prefer higher score; on tie prefer higher sensitivity classification
            if score > best_match.score or (
                score == best_match.score
                and _SENSITIVITY_RANK.get(ts.classification_sensitivity, 0)
                > _SENSITIVITY_RANK.get(
                    best_match.classification_name and "low" or "low", 0
                )
            ):
                best_match = ColumnMatch(
                    column_name=col,
                    term_id=ts.id,
                    term_name=ts.name,
                    classification_id=ts.classification_id,
                    classification_name=ts.classification_name,
                    score=score,
                )

        results.append(best_match)
    return results


async def scan_table(
    database: str,
    table: str,
    term_specs: Optional[list[TermSpec]] = None,
    threshold: Optional[float] = None,
) -> list[ColumnMatch]:
    """
    Classify all columns in *database*.*table*.

    Args:
        database:    Doris database name.
        table:       Doris table name.
        term_specs:  Pre-loaded term specs (pass to avoid repeated DB round-trips).
        threshold:   Override settings.auto_classify_threshold.

    Returns a ColumnMatch per column.
    """
    s = get_settings()
    if threshold is None:
        threshold = s.auto_classify_threshold
    if term_specs is None:
        term_specs = await _load_term_specs()

    columns = await _get_doris_columns(database, table)
    log.info("Classifier: scanning %s.%s — %d columns", database, table, len(columns))
    return classify_columns(columns, term_specs, threshold)


async def scan_database(
    database: str,
    term_specs: Optional[list[TermSpec]] = None,
    threshold: Optional[float] = None,
) -> dict[str, list[ColumnMatch]]:
    """
    Classify all base tables in *database*.

    Returns {table_name: [ColumnMatch, ...]}
    """
    if term_specs is None:
        term_specs = await _load_term_specs()

    tables = await _get_doris_tables(database)
    results: dict[str, list[ColumnMatch]] = {}
    for table in tables:
        try:
            results[table] = await scan_table(database, table, term_specs, threshold)
        except Exception as exc:
            log.warning("Classifier: error scanning %s.%s: %s", database, table, exc)
    return results
