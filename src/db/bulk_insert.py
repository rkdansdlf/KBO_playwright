"""Bound multi-row INSERT statements by the database's bind-parameter limit.

SQLAlchemy compiles ``insert(T).values(rows)`` — a list of dicts — into ONE
statement whose bind-parameter count is ``rows x columns``. PostgreSQL refuses a
statement with more than 65,535 parameters and SQLite 32,766
(``SQLITE_MAX_VARIABLE_NUMBER``, 3.32+), and the refusal is an error for the whole
batch rather than a skipped row. Measured 2026-11-02: a nightly ``daily_sync`` DAG
run failed on exactly this limit; the statement could not be re-identified
afterwards because container recreation had taken the logs.

The sites this bounds are the roster, player and season-stat batch upserts. Their
row counts grow with the season — ``player_season_batting`` needs only ~1,872 rows
at 35 columns to cross 65,535, which a full-season recalculation reaches — so the
limit is a matter of when, not whether.

Not every list-of-dicts execute is a risk: ``session.execute(insert(T), rows)``
(without ``.values``) is an ``executemany``, which sends one statement per row and
is not bounded by this limit.
"""

from __future__ import annotations

from typing import Any

#: Target bind parameters per statement. Sits below SQLite's 32,766 as well as
#: PostgreSQL's and MySQL's 65,535, so one bound covers every dialect these
#: repositories run on and a chunk never depends on which one is connected.
MAX_BIND_PARAMS_PER_STATEMENT = 30_000


def chunk_for_bind(
    rows: list[dict[str, Any]],
    *,
    limit: int | None = None,
) -> list[list[dict[str, Any]]]:
    """Split rows into chunks whose bind-parameter count stays under the limit.

    ``rows`` must share the same keys: SQLAlchemy takes the column list from the
    first dict, so a row with extra keys would raise the real parameter count
    above the one computed here. Every row builder in this repository produces
    uniform rows, and chunking preserves that.

    Args:
        rows: Rows for one multi-row INSERT.
        limit: Maximum bind parameters per statement; defaults to the module
            constant, read at call time so a test can lower it without touching
            callers.

    Returns:
        Non-empty chunks in order. Empty input yields no chunks.

    """
    if not rows:
        return []
    effective = MAX_BIND_PARAMS_PER_STATEMENT if limit is None else limit
    per_row = max(1, len(rows[0]))
    size = max(1, effective // per_row)
    if len(rows) <= size:
        return [rows]
    return [rows[i : i + size] for i in range(0, len(rows), size)]
