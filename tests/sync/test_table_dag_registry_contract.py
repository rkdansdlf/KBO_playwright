"""Contract for the SQLite to Oracle initial-load table registry.

The registry decides which natural key each table upserts on. A wrong key does
not raise: it silently inserts a duplicate row on every sync, so the key has to
be pinned against the model's own unique constraint rather than trusted.
"""

from __future__ import annotations

import sqlalchemy as sa
import pytest

import src.models  # registers every ORM table as an import side effect
from src.models.base import Base
from src.sync.table_dag import TABLE_META_BY_NAME, TABLE_REGISTRY, SyncStrategy, get_tables_by_level

#: Operational ledgers added alongside the crawl-run lineage work, with the
#: unique column each one upserts on.
EXPECTED_LEDGER_KEYS = {
    "crawl_execution_runs": "run_id",
    "crawl_dead_letters": "dlq_id",
    "notification_incidents": "incident_key",
}


def _unique_column_names(table: sa.Table) -> set[str]:
    names: set[str] = set()
    for constraint in table.constraints:
        if isinstance(constraint, sa.UniqueConstraint) and len(constraint.columns) == 1:
            names.add(constraint.columns.keys()[0])
    names.update(column.name for column in table.columns if column.unique)
    return names


def test_registry_covers_every_orm_table() -> None:
    assert set(Base.metadata.tables) == set(TABLE_META_BY_NAME)


def test_registry_has_no_duplicate_names() -> None:
    names = [meta.name for meta in TABLE_REGISTRY]
    assert len(names) == len(set(names))


def test_every_registered_table_exists_in_orm_metadata() -> None:
    missing = [meta.name for meta in TABLE_REGISTRY if meta.name not in Base.metadata.tables]
    assert not missing, f"registry entries with no ORM model: {missing}"


@pytest.mark.parametrize("table", sorted(EXPECTED_LEDGER_KEYS))
def test_ledger_natural_key_matches_the_model_constraint(table: str) -> None:
    """The key must be a real unique column, or sync duplicates rows forever."""
    expected = EXPECTED_LEDGER_KEYS[table]
    meta = TABLE_META_BY_NAME[table]

    assert meta.natural_keys == [expected], f"{table} must upsert on {expected}"
    assert expected in _unique_column_names(Base.metadata.tables[table])


@pytest.mark.parametrize("table", sorted(EXPECTED_LEDGER_KEYS))
def test_ledger_is_an_incremental_level_3_table(table: str) -> None:
    meta = TABLE_META_BY_NAME[table]

    assert meta.level == 3, f"{table} is operational telemetry, not domain data"
    assert meta.strategy is SyncStrategy.INCREMENTAL
    # Incremental sync filters on this column, so it has to exist.
    assert meta.timestamp_col in Base.metadata.tables[table].c


def test_level_three_contains_the_ledgers() -> None:
    level_three = {meta.name for meta in get_tables_by_level()[3]}
    assert EXPECTED_LEDGER_KEYS.keys() <= level_three


def test_natural_keys_reference_real_columns() -> None:
    """A key naming a missing column fails at sync time, not at import time."""
    bad: list[str] = []
    for meta in TABLE_REGISTRY:
        table = Base.metadata.tables.get(meta.name)
        if table is None or not meta.natural_keys:
            continue
        absent = [key for key in meta.natural_keys if key not in table.c]
        if absent:
            bad.append(f"{meta.name}: {absent}")
    assert not bad, f"natural keys referencing unknown columns: {bad}"
