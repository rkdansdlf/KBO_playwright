"""The record tables are created by migrations; the ORM has to agree with them.

A migration and a model that disagree do not fail anywhere in particular. The
migration runs once at deploy, the ORM metadata is only consulted when something
else creates tables, and the first sign of a mismatch is a live query failing on
a column that does not exist -- in the daily quality gate, with the stack trace
somewhere else entirely.

These compare the two directly, so a column added to one and forgotten in the
other shows up in the suite instead.
"""

from __future__ import annotations

import re
import sqlite3

import pytest

from src.models.base import Base
from src.models.pa_violation_record import PaUnrecoverable, PaViolationBaseline

MIGRATIONS = {
    "pa_violation_baseline": "migrations/sqlite/069_pa_violation_baseline.sql",
    "pa_unrecoverable": "migrations/sqlite/069_pa_violation_baseline.sql",
}

_MODELS = {
    "pa_violation_baseline": PaViolationBaseline,
    "pa_unrecoverable": PaUnrecoverable,
}

_CREATE_TABLE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)\s*\((.*?)\n\);",
    re.DOTALL | re.IGNORECASE,
)


def _columns_from_migration(table: str) -> set[str]:
    """Read the column names a migration actually creates."""
    from pathlib import Path

    body = Path(MIGRATIONS[table]).read_text(encoding="utf-8")
    match = next(m for m in _CREATE_TABLE.finditer(body) if m.group(1) == table)
    declared: set[str] = set()
    for line in match.group(2).splitlines():
        line = line.strip().rstrip(",")
        if not line or line.upper().startswith(("CONSTRAINT", "PRIMARY KEY", "UNIQUE")):
            continue
        declared.add(line.split()[0])
    return declared


@pytest.mark.parametrize("table", sorted(MIGRATIONS))
def test_the_migration_creates_exactly_the_columns_the_model_declares(table: str) -> None:
    """Either side adding a column alone is a runtime failure waiting to happen."""
    declared = _columns_from_migration(table)
    modelled = {column.name for column in _MODELS[table].__table__.columns}

    assert declared == modelled, (
        f"{table}: migration creates {sorted(declared - modelled)} that the model lacks, "
        f"and the model declares {sorted(modelled - declared)} the migration never creates"
    )


@pytest.mark.parametrize("table", sorted(MIGRATIONS))
def test_the_migration_runs_and_the_model_reads_the_resulting_table(table: str) -> None:
    """The DDL has to be executable, not merely parseable by a regex."""
    from pathlib import Path

    conn = sqlite3.connect(":memory:")
    conn.executescript(Path(MIGRATIONS[table]).read_text(encoding="utf-8"))

    names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}

    assert table in names, "the migration did not create the table it claims to"
    conn.close()


def test_the_models_are_registered_in_metadata() -> None:
    """An unregistered model is a table the migration inventory rejects.

    The inventory checks that every migration names a table the ORM knows about;
    an import that never reaches ``Base.metadata`` fails that check with no
    pointer to the model that was left out.
    """
    assert PaViolationBaseline.__tablename__ in Base.metadata.tables
    assert PaUnrecoverable.__tablename__ in Base.metadata.tables


def test_importing_the_package_registers_both_models() -> None:
    """The inventory reads ``src.models``, so the import list is the real gate.

    ``Base.metadata`` already holds both tables because this module imported the
    classes directly. The inventory does something else: it imports the package
    and reads what that import reached. Dropping the line from ``__init__`` is
    therefore invisible to the assertion above and caught only here.
    """
    import src.models

    assert "PaViolationBaseline" in vars(src.models)
    assert "PaUnrecoverable" in vars(src.models)


def test_the_season_is_part_of_the_key() -> None:
    """Storing it outside the key is the defect this model exists to prevent.

    A bare player id cannot tell a one-season run which rows are its own, so the
    other seasons' rows read as repairs -- 714 of them, every run.
    """
    key = [column.name for column in PaViolationBaseline.__table__.primary_key.columns]

    assert key == ["check_id", "scope_season", "player_id"]
