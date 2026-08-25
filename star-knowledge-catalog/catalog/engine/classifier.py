"""
Star Knowledge Catalog — Auto Column Classifier (v2)

Three-tier scoring + contextual multi-signal arbitration
=========================================================

Every column passes through two stages:

Stage 1 — Base scoring (unchanged from v1)
───────────────────────────────────────────
  1.0  exact match          column == pattern
  0.9  word-boundary match  _pattern_ or ^pattern_ or _pattern$
  0.7  substring match      pattern anywhere inside column name
  0.0  no match

Stage 2 — Confidence arbitration (new)
────────────────────────────────────────
Score 1.0 / 0.9 → always HIGH confidence → tag immediately.
Score 0.7       → run three independent arbitration signals:

  Signal A — Token position weight
    +0.15 if the matched pattern token starts  the column name   (prefix)
    +0.10 if the matched pattern token ends    the column name   (suffix)
    +0.00 if the token is buried in the middle (weakest signal)

  Signal B — Sibling table context
    +0.20 if ≥1 other column in the SAME table already has a HIGH/MEDIUM
           confidence tag whose classification == this column's candidate
           classification.  Rationale: a table with confirmed PII columns
           is almost certainly a person-record table — other ambiguous
           columns in it should lean toward tagging.

  Signal C — Negative token guard
    -1.0 (immediate REJECT) if the column name contains any token from
           the glossary term's negative_patterns list.
           e.g.  full_name term has negative ["company","display","brand",
                 "product","vendor"] so "company_name" is rejected.

  Signal D — Table name context
    -1.0 (immediate REJECT) if the TABLE name contains any token from
           the glossary term's table_name_negative_patterns list.
           e.g.  full_name has table_name_negatives ["product","item",
                 "inventory","catalog","sku"] so a column called "name"
                 inside "products" or "item_catalog" is rejected.
    +0.20 (strong boost) if the TABLE name contains a token from the
           glossary term's table_name_positive_patterns list.
           e.g.  full_name has table_name_positives ["customer","employee",
                 "user","person","staff","contact"] so "name" in
                 "customers" receives a strong context boost.

Combined arbitration score  =  0.7 base + A + B + D_boost
  ≥ 0.85  → MEDIUM confidence  → tag with conservative masking
  ≥ 0.95  → HIGH   confidence  → tag with full term masking
  any REJECT signal fired      → REJECT (not tagged, stored in audit log)

The `confidence` field on ColumnMatch carries: HIGH | MEDIUM | LOW | REJECT.
LOW means score was 0.7 but arbitration score 0.70–0.84 (below MEDIUM).
REJECT means negative guard fired.

Only HIGH and MEDIUM produce tags.  LOW and REJECT do not.
MEDIUM tags use the CLASSIFICATION-level default policy (conservative),
not the TERM-level specific policy.  This is intentional — if we're not
sure it's an email address, we SHA-256 it (the PII default) rather than
applying the more revealing EMAIL_PARTIAL expression.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import aiomysql

from ..config import get_settings
from ..database import db_session
from ..models import GlossaryTerm

log = logging.getLogger(__name__)

_SENSITIVITY_RANK = {"low": 0, "medium": 1, "high": 2, "critical": 3}

# Threshold below which a base score is not even considered
_BASE_THRESHOLD = 0.70

# Arbitration thresholds (only applied when base score == 0.7)
_ARB_MEDIUM_THRESHOLD = 0.85   # 0.7 + at least one positive signal
_ARB_HIGH_THRESHOLD   = 0.95   # 0.7 + multiple strong signals


class Confidence(str, Enum):
    HIGH   = "HIGH"    # tag with term-level masking policy
    MEDIUM = "MEDIUM"  # tag with classification-level default policy
    LOW    = "LOW"     # below arbitration threshold — not tagged
    REJECT = "REJECT"  # negative guard fired — not tagged, audit logged


@dataclass
class ColumnMatch:
    column_name: str
    term_id: Optional[int]       = None
    term_name: Optional[str]     = None
    classification_id: Optional[int]   = None
    classification_name: Optional[str] = None
    score: float                 = 0.0
    confidence: Confidence       = Confidence.LOW
    # Arbitration breakdown — useful for debugging and the scan API response
    arb_score: float             = 0.0   # final arbitration score (0.7 cols only)
    arb_signals: list[str]       = field(default_factory=list)
    # When MEDIUM: use classification-level policy instead of term-level
    use_conservative_policy: bool = False


@dataclass
class TermSpec:
    id: int
    name: str
    classification_id: Optional[int]
    classification_name: Optional[str]
    classification_sensitivity: str
    patterns: list[str]                    = field(default_factory=list)
    negative_patterns: list[str]           = field(default_factory=list)
    # Signal D — table name context
    table_name_negative_patterns: list[str] = field(default_factory=list)
    table_name_positive_patterns: list[str] = field(default_factory=list)


# ── Term spec loader ──────────────────────────────────────────────────────────

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
                negative_patterns=[p.lower() for p in (t.negative_patterns or [])],
                table_name_negative_patterns=[p.lower() for p in (t.table_name_negative_patterns or [])],
                table_name_positive_patterns=[p.lower() for p in (t.table_name_positive_patterns or [])],
            )
        )
    return specs


# ── Stage 1: base scoring ─────────────────────────────────────────────────────

def _base_score(col_lower: str, patterns: list[str]) -> tuple[float, str]:
    """
    Returns (score, matched_pattern).
    score: 1.0 | 0.9 | 0.7 | 0.0
    """
    best_score = 0.0
    best_pat = ""
    for pat in patterns:
        if not pat:
            continue
        if col_lower == pat:
            return 1.0, pat
        if re.search(r"(?<![a-z0-9])" + re.escape(pat) + r"(?![a-z0-9])", col_lower):
            if 0.9 > best_score:
                best_score, best_pat = 0.9, pat
        elif pat in col_lower:
            if 0.7 > best_score:
                best_score, best_pat = 0.7, pat
    return best_score, best_pat


# ── Stage 2: arbitration signals ─────────────────────────────────────────────

def _signal_A_position(col_lower: str, matched_pat: str) -> tuple[float, str]:
    """
    Signal A — token position weight.
    Reward patterns that appear at the start or end of the column name.
    A token at the start is the strongest position signal.
    """
    if col_lower.startswith(matched_pat):
        return 0.15, f"A:prefix(+0.15) — '{matched_pat}' is prefix of '{col_lower}'"
    if col_lower.endswith(matched_pat):
        return 0.10, f"A:suffix(+0.10) — '{matched_pat}' is suffix of '{col_lower}'"
    # Check with underscore boundary at start/end
    if col_lower.startswith(matched_pat + "_"):
        return 0.15, f"A:prefix_(+0.15) — '{matched_pat}_' starts '{col_lower}'"
    if col_lower.endswith("_" + matched_pat):
        return 0.10, f"A:_suffix(+0.10) — '_{matched_pat}' ends '{col_lower}'"
    return 0.0, f"A:middle(+0.00) — '{matched_pat}' is buried in '{col_lower}'"


def _signal_B_sibling_context(
    classification_name: Optional[str],
    already_tagged: list[ColumnMatch],
) -> tuple[float, str]:
    """
    Signal B — sibling table context.
    If another column in the SAME table scan pass is already HIGH/MEDIUM
    and shares the same classification, boost this ambiguous column.
    """
    if not classification_name:
        return 0.0, "B:no_class(+0.00)"

    confirmed_siblings = [
        m for m in already_tagged
        if m.confidence in (Confidence.HIGH, Confidence.MEDIUM)
        and m.classification_name == classification_name
    ]
    if len(confirmed_siblings) >= 2:
        return 0.20, (
            f"B:strong_context(+0.20) — {len(confirmed_siblings)} confirmed "
            f"{classification_name} siblings in same table"
        )
    if len(confirmed_siblings) == 1:
        return 0.12, (
            f"B:weak_context(+0.12) — 1 confirmed "
            f"{classification_name} sibling in same table"
        )
    return 0.0, "B:no_context(+0.00) — no confirmed siblings in same table"


def _signal_C_negative_guard(
    col_lower: str,
    negative_patterns: list[str],
) -> tuple[bool, str]:
    """
    Signal C — negative token guard.
    Returns (rejected, reason).
    If ANY negative pattern token appears as a word boundary in the
    column name, immediately reject it.
    """
    for neg in negative_patterns:
        if not neg:
            continue
        # Word-boundary match for negative patterns (same logic as base score tier 2)
        if re.search(r"(?<![a-z0-9])" + re.escape(neg) + r"(?![a-z0-9])", col_lower):
            return True, f"C:negative_guard — '{neg}' found in '{col_lower}'"
        # Also reject exact prefix (e.g. "company" in "company_name")
        if col_lower.startswith(neg + "_") or col_lower == neg:
            return True, f"C:negative_prefix — '{neg}' prefixes '{col_lower}'"
    return False, ""


def _signal_D_table_context(
    table_lower: str,
    ts: TermSpec,
) -> tuple[bool, float, str]:
    """
    Signal D — table name context.
    Returns (rejected, boost, description).

    If the table name contains a token from table_name_negative_patterns
    → immediately reject (e.g. 'name' in 'products' table).
    If the table name contains a token from table_name_positive_patterns
    → boost by +0.20 (e.g. 'name' in 'customers' table).
    """
    for neg in ts.table_name_negative_patterns:
        if not neg:
            continue
        if re.search(r"(?<![a-z0-9])" + re.escape(neg) + r"(?![a-z0-9])", table_lower):
            return True, 0.0, f"D:table_reject — table '{table_lower}' contains '{neg}'"
        if table_lower.startswith(neg) or table_lower.endswith(neg):
            return True, 0.0, f"D:table_reject — table '{table_lower}' starts/ends with '{neg}'"

    for pos in ts.table_name_positive_patterns:
        if not pos:
            continue
        if re.search(r"(?<![a-z0-9])" + re.escape(pos) + r"(?![a-z0-9])", table_lower):
            return False, 0.20, f"D:table_boost(+0.20) — table '{table_lower}' contains '{pos}'"
        if table_lower.startswith(pos) or table_lower.endswith(pos):
            return False, 0.20, f"D:table_boost(+0.20) — table '{table_lower}' starts/ends with '{pos}'"

    return False, 0.0, f"D:table_neutral(+0.00) — table '{table_lower}' matches no context patterns"


# ── Arbitration orchestrator ──────────────────────────────────────────────────

def _arbitrate(
    col_lower: str,
    matched_pat: str,
    ts: TermSpec,
    already_tagged: list[ColumnMatch],
    table_lower: str = "",
) -> tuple[Confidence, float, list[str]]:
    """
    Run all four signals and return (Confidence, arb_score, signal_descriptions).
    Called only when base score == 0.7.
    """
    signals: list[str] = []

    # Signal C — check negative guard first (fast exit)
    rejected, reason = _signal_C_negative_guard(col_lower, ts.negative_patterns)
    if rejected:
        signals.append(reason)
        return Confidence.REJECT, 0.0, signals

    # Signal D — table name context (fast exit on table-level reject)
    if table_lower:
        d_rejected, d_boost, d_desc = _signal_D_table_context(table_lower, ts)
        signals.append(d_desc)
        if d_rejected:
            return Confidence.REJECT, 0.0, signals
    else:
        d_boost = 0.0

    # Signal A — position
    a_boost, a_desc = _signal_A_position(col_lower, matched_pat)
    signals.append(a_desc)

    # Signal B — sibling context
    b_boost, b_desc = _signal_B_sibling_context(ts.classification_name, already_tagged)
    signals.append(b_desc)

    arb_score = 0.7 + a_boost + b_boost + d_boost
    signals.append(f"arb_total={arb_score:.2f}")

    if arb_score >= _ARB_HIGH_THRESHOLD:
        return Confidence.HIGH, arb_score, signals
    if arb_score >= _ARB_MEDIUM_THRESHOLD:
        return Confidence.MEDIUM, arb_score, signals
    return Confidence.LOW, arb_score, signals


# ── Main classification ───────────────────────────────────────────────────────

def classify_columns(
    column_names: list[str],
    term_specs: list[TermSpec],
    threshold: float = _BASE_THRESHOLD,
    table_name: str = "",
) -> list[ColumnMatch]:
    """
    Classify a list of column names against term specs using two-stage scoring.

    Stage 1: base score (exact / word-boundary / substring).
    Stage 2: contextual arbitration for 0.7 (substring) hits only.

    Returns one ColumnMatch per column. Columns with Confidence.LOW or
    Confidence.REJECT are included in the list (for the audit trail and scan
    response) but will NOT produce column tags in the router.

    The `already_tagged` accumulator is built left-to-right through the column
    list, so Signal B (sibling context) benefits from earlier high-confidence
    hits in the same table scan.  Column order is Doris ordinal position, so
    primary identifying columns (id, name, email) typically appear first and
    provide context for later ambiguous columns.

    `table_name` is passed to Signal D for table-level context filtering.
    """
    results: list[ColumnMatch] = []
    # Accumulates confirmed matches so far — used by Signal B
    already_tagged: list[ColumnMatch] = []
    table_lower = table_name.lower().replace(" ", "_")

    for col in column_names:
        col_lower = col.lower().replace(" ", "_")
        best_match = ColumnMatch(column_name=col, confidence=Confidence.LOW)

        for ts in term_specs:
            base, matched_pat = _base_score(col_lower, ts.patterns)

            if base < threshold:
                continue

            if base >= 0.9:
                # High-confidence base score — no arbitration needed
                # Still run Signal D table reject even for high base scores
                if table_lower and ts.table_name_negative_patterns:
                    d_rejected, _, d_desc = _signal_D_table_context(table_lower, ts)
                    if d_rejected:
                        confidence = Confidence.REJECT
                        arb_score = 0.0
                        arb_signals = [f"base_score={base}", d_desc]
                    else:
                        confidence = Confidence.HIGH
                        arb_score = base
                        arb_signals = [f"base_score={base} → direct HIGH", d_desc]
                else:
                    confidence = Confidence.HIGH
                    arb_score = base
                    arb_signals = [f"base_score={base} → direct HIGH"]
            else:
                # base == 0.7 — run arbitration
                confidence, arb_score, arb_signals = _arbitrate(
                    col_lower, matched_pat, ts, already_tagged, table_lower
                )

            # Build a candidate match
            candidate = ColumnMatch(
                column_name=col,
                term_id=ts.id,
                term_name=ts.name,
                classification_id=ts.classification_id,
                classification_name=ts.classification_name,
                score=base,
                confidence=confidence,
                arb_score=arb_score,
                arb_signals=arb_signals,
                # MEDIUM confidence → conservative: use class-level default policy
                use_conservative_policy=(confidence == Confidence.MEDIUM),
            )

            # Only consider taggable confidences
            if confidence in (Confidence.REJECT, Confidence.LOW):
                # Still record as candidate if it's the only match seen so far
                # (for the audit response) but don't use it as best_match
                # unless there's literally nothing better
                if best_match.confidence in (Confidence.LOW, Confidence.REJECT):
                    best_match = candidate
                continue

            # Prefer HIGH over MEDIUM, then higher base score, then higher sensitivity
            def _rank(m: ColumnMatch) -> tuple:
                conf_rank = {Confidence.HIGH: 2, Confidence.MEDIUM: 1,
                             Confidence.LOW: 0, Confidence.REJECT: -1}
                return (
                    conf_rank[m.confidence],
                    m.arb_score,
                    _SENSITIVITY_RANK.get(ts.classification_sensitivity, 0),
                )

            if _rank(candidate) > _rank(best_match):
                best_match = candidate

        results.append(best_match)

        # Add to sibling context only if it will produce a tag
        if best_match.confidence in (Confidence.HIGH, Confidence.MEDIUM):
            already_tagged.append(best_match)

    return results


# ── Doris metadata helpers ────────────────────────────────────────────────────

async def _get_doris_columns(database: str, table: str) -> list[str]:
    """Fetch column names from Doris information_schema in ordinal order."""
    s = get_settings()
    conn = await aiomysql.connect(
        host=s.doris_host, port=s.doris_port,
        user=s.doris_admin_user, password=s.doris_admin_password,
        db="information_schema", connect_timeout=5, charset="utf8mb4",
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
    """Fetch all base table names (exclude views and masked views)."""
    s = get_settings()
    suffix = s.masked_view_suffix
    conn = await aiomysql.connect(
        host=s.doris_host, port=s.doris_port,
        user=s.doris_admin_user, password=s.doris_admin_password,
        db="information_schema", connect_timeout=5, charset="utf8mb4",
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


# ── Public scan API ───────────────────────────────────────────────────────────

async def scan_table(
    database: str,
    table: str,
    term_specs: Optional[list[TermSpec]] = None,
    threshold: float = _BASE_THRESHOLD,
) -> list[ColumnMatch]:
    """Classify all columns in database.table. Returns one ColumnMatch per column."""
    if term_specs is None:
        term_specs = await _load_term_specs()
    columns = await _get_doris_columns(database, table)
    log.info("Classifier: scanning %s.%s — %d columns", database, table, len(columns))
    return classify_columns(columns, term_specs, threshold, table_name=table)


async def scan_database(
    database: str,
    term_specs: Optional[list[TermSpec]] = None,
    threshold: float = _BASE_THRESHOLD,
) -> dict[str, list[ColumnMatch]]:
    """Classify all base tables in database. Returns {table: [ColumnMatch]}."""
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
