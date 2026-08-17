from __future__ import annotations

import pytest

from scripts.verification.reset_oracle_verification_schema import (
    PRESERVED_OBJECT_NAMES,
    PRESERVED_OBJECT_PREFIXES,
    _drop_statement,
    _quote_oracle_identifier,
)


def test_drop_statement_uses_cascade_purge_for_tables() -> None:
    """Make table reset remove dependent constraints and storage."""
    assert _drop_statement("TABLE", "GAME", "DROP TABLE") == 'DROP TABLE "GAME" CASCADE CONSTRAINTS PURGE'


def test_drop_statement_forces_oracle_types() -> None:
    """Allow disposable type dependencies to be removed during reset."""
    assert _drop_statement("TYPE", "KBO_TYPE", "DROP TYPE") == 'DROP TYPE "KBO_TYPE" FORCE'


def test_identifier_validation_rejects_sql_fragments() -> None:
    """Never interpolate an unvalidated object name into reset DDL."""
    with pytest.raises(ValueError, match="Unsafe Oracle object identifier"):
        _quote_oracle_identifier('GAME" CASCADE CONSTRAINTS')


def test_database_tools_objects_are_preserved() -> None:
    """Do not remove Oracle Database Tools' internal history objects."""
    assert "DBTOOLS$EXECUTION_HISTORY" in PRESERVED_OBJECT_NAMES


def test_oracle_lob_indexes_are_preserved() -> None:
    """Leave Oracle-managed LOB indexes under database control."""
    assert "SYS_IL" in PRESERVED_OBJECT_PREFIXES
