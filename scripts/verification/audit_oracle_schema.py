#!/usr/bin/env python3
"""Read-only comparison of SQLAlchemy metadata with an Oracle schema."""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import src.models  # noqa: F401
from src.db.engine import create_engine_for_url
from src.models.base import Base

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.reflection import Inspector
    from sqlalchemy.sql.schema import Table

type ForeignKeySignature = tuple[tuple[str, ...], str, tuple[str, ...]]
type IndexSignature = tuple[tuple[str, ...], bool]

KNOWN_ORACLE_SCHEMA_EXCEPTIONS = {
    ("stadium_seat_sections", ("section_code", "stadium_id")): (
        "Oracle migration 052 intentionally drops this nullable section-code constraint"
    ),
}
KNOWN_ORACLE_TABLE_EXCEPTIONS = {
    "dbtools$execution_history": "Oracle Database Tools internal execution history table",
    "schema_migrations": "Oracle migration tracking table, not an ORM model",
    "team_profiles": "Legacy compatibility table created by Oracle migration 029",
}


def _normalized(value: str) -> str:
    """Normalize Oracle's case-insensitive object names."""
    return value.casefold()


def _normalized_names(values: Iterable[str]) -> set[str]:
    """Return normalized names from an iterable."""
    return {_normalized(value) for value in values}


def _expected_foreign_keys(table: Table) -> set[ForeignKeySignature]:
    """Return normalized foreign-key signatures from a model table."""
    return {
        (
            tuple(_normalized(column) for column in constraint.column_keys),
            _normalized(constraint.elements[0].column.table.name),
            tuple(_normalized(element.column.name) for element in constraint.elements),
        )
        for constraint in table.foreign_key_constraints
    }


def _expected_unique_constraints(table: Table) -> set[tuple[str, ...]]:
    """Return normalized non-primary unique constraint signatures."""
    return {
        tuple(sorted(_normalized(column.name) for column in constraint.columns))
        for constraint in table.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }


def _expected_indexes(table: Table) -> dict[str, IndexSignature]:
    """Return expected index definitions keyed by normalized name."""
    return {
        _normalized(str(index.name)): (
            tuple(_normalized(column.name) for column in index.columns),
            bool(index.unique),
        )
        for index in table.indexes
        if index.name
    }


def _actual_indexes(inspector: Inspector, table_name: str) -> dict[str, IndexSignature]:
    """Return actual index definitions keyed by normalized name."""
    return {
        _normalized(str(index["name"])): (
            tuple(_normalized(str(column)) for column in index.get("column_names", [])),
            bool(index.get("unique")),
        )
        for index in inspector.get_indexes(table_name)
        if index.get("name")
    }


def _actual_foreign_keys(inspector: Inspector, table_name: str) -> set[ForeignKeySignature]:
    """Return normalized foreign-key signatures from an inspected table."""
    return {
        (
            tuple(sorted(_normalized(column) for column in foreign_key.get("constrained_columns", []))),
            _normalized(str(foreign_key.get("referred_table", ""))),
            tuple(sorted(_normalized(column) for column in foreign_key.get("referred_columns", []))),
        )
        for foreign_key in inspector.get_foreign_keys(table_name)
    }


def _actual_unique_constraints(inspector: Inspector, table_name: str) -> set[tuple[str, ...]]:
    """Return normalized non-primary unique constraint signatures."""
    signatures = {
        tuple(sorted(_normalized(column) for column in constraint.get("column_names", [])))
        for constraint in inspector.get_unique_constraints(table_name)
    }
    signatures.update(
        tuple(sorted(signature)) for signature, unique in _actual_indexes(inspector, table_name).values() if unique
    )
    return signatures


@dataclass(frozen=True, slots=True)
class OracleSchemaAuditReport:
    """Describe model-to-Oracle schema differences."""

    expected_table_count: int
    actual_table_count: int
    missing_tables: list[str]
    unexpected_tables: list[str]
    missing_columns: dict[str, list[str]]
    missing_indexes: dict[str, list[str]]
    index_name_differences: dict[str, list[dict[str, object]]]
    missing_unique_constraints: dict[str, list[list[str]]]
    missing_foreign_keys: dict[str, list[dict[str, object]]]
    known_exceptions: list[dict[str, object]]
    oracle_object_counts: dict[str, int]

    @property
    def schema_drift(self) -> bool:
        """Return whether expected model objects are missing from Oracle."""
        return bool(
            self.missing_tables
            or self.missing_columns
            or self.missing_indexes
            or self.missing_unique_constraints
            or self.missing_foreign_keys
        )

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-compatible report mapping."""
        payload = asdict(self)
        payload["schema_drift"] = self.schema_drift
        return payload


def _oracle_object_counts(engine: Engine) -> dict[str, int]:
    """Read Oracle object counts without changing schema state."""
    if engine.dialect.name != "oracle":
        return {}
    queries = {
        "tables": "SELECT COUNT(*) FROM USER_TABLES",
        "indexes": "SELECT COUNT(*) FROM USER_INDEXES",
        "constraints": "SELECT COUNT(*) FROM USER_CONSTRAINTS",
        "sequences": "SELECT COUNT(*) FROM USER_SEQUENCES",
    }
    try:
        with engine.connect() as connection:
            return {name: int(connection.execute(text(query)).scalar_one()) for name, query in queries.items()}
    except SQLAlchemyError:
        return {}


def _audit_columns(inspector: Inspector, table: Table, actual_name: str) -> list[str]:
    """Return model columns absent from an inspected table."""
    actual_columns = _normalized_names(str(column["name"]) for column in inspector.get_columns(actual_name))
    expected_columns = _normalized_names(column.name for column in table.columns)
    return sorted(expected_columns - actual_columns)


def _audit_indexes(
    inspector: Inspector,
    table: Table,
    actual_name: str,
) -> tuple[list[str], list[dict[str, object]]]:
    """Return missing indexes and equivalent indexes with different names."""
    actual_indexes = _actual_indexes(inspector, actual_name)
    missing: list[str] = []
    name_differences: list[dict[str, object]] = []
    for expected_name, expected_signature in _expected_indexes(table).items():
        matching_names = [
            actual_index_name
            for actual_index_name, actual_signature in actual_indexes.items()
            if actual_signature == expected_signature
        ]
        if not matching_names:
            missing.append(expected_name)
        elif expected_name not in matching_names:
            name_differences.append(
                {
                    "expected": expected_name,
                    "actual": sorted(matching_names),
                    "columns": list(expected_signature[0]),
                    "unique": expected_signature[1],
                },
            )
    return missing, name_differences


def _audit_unique_constraints(
    inspector: Inspector,
    table: Table,
    actual_name: str,
    normalized_name: str,
) -> tuple[list[list[str]], list[dict[str, object]]]:
    """Return missing unique constraints and documented Oracle exceptions."""
    actual_unique = _actual_unique_constraints(inspector, actual_name)
    missing: list[list[str]] = []
    exceptions: list[dict[str, object]] = []
    for signature in sorted(_expected_unique_constraints(table) - actual_unique):
        exception = KNOWN_ORACLE_SCHEMA_EXCEPTIONS.get((normalized_name, signature))
        if exception:
            exceptions.append(
                {
                    "table": normalized_name,
                    "object": "unique_constraint",
                    "columns": list(signature),
                    "reason": exception,
                },
            )
            continue
        missing.append(list(signature))
    return missing, exceptions


def _audit_foreign_keys(inspector: Inspector, table: Table, actual_name: str) -> list[dict[str, object]]:
    """Return foreign-key signatures absent from an inspected table."""
    missing = _expected_foreign_keys(table) - _actual_foreign_keys(inspector, actual_name)
    return [
        {
            "columns": list(columns),
            "referred_table": referred_table,
            "referred_columns": list(referred_columns),
        }
        for columns, referred_table, referred_columns in sorted(missing)
    ]


def audit_oracle_schema(engine: Engine) -> OracleSchemaAuditReport:
    """Compare registered SQLAlchemy tables and constraints with Oracle objects."""
    metadata_tables = {_normalized(table.name): table for table in Base.metadata.sorted_tables}
    inspector = inspect(engine)
    actual_names = {_normalized(name): name for name in inspector.get_table_names()}
    missing_tables = sorted(set(metadata_tables) - set(actual_names))
    unexpected_tables = sorted(set(actual_names) - set(metadata_tables))
    missing_columns: dict[str, list[str]] = {}
    missing_indexes: dict[str, list[str]] = {}
    index_name_differences: dict[str, list[dict[str, object]]] = {}
    missing_unique_constraints: dict[str, list[list[str]]] = {}
    missing_foreign_keys: dict[str, list[dict[str, object]]] = {}
    known_exceptions: list[dict[str, object]] = [
        {
            "table": table_name,
            "object": "table",
            "reason": reason,
        }
        for table_name, reason in sorted(KNOWN_ORACLE_TABLE_EXCEPTIONS.items())
        if table_name in actual_names
    ]

    for normalized_name, table in metadata_tables.items():
        actual_name = actual_names.get(normalized_name)
        if actual_name is None:
            continue

        if missing := _audit_columns(inspector, table, actual_name):
            missing_columns[normalized_name] = missing

        missing, name_differences = _audit_indexes(inspector, table, actual_name)
        if missing:
            missing_indexes[normalized_name] = missing
        if name_differences:
            index_name_differences[normalized_name] = name_differences

        missing_unique, exceptions = _audit_unique_constraints(inspector, table, actual_name, normalized_name)
        if missing_unique:
            missing_unique_constraints[normalized_name] = missing_unique
        known_exceptions.extend(exceptions)

        missing_foreign = _audit_foreign_keys(inspector, table, actual_name)
        if missing_foreign:
            missing_foreign_keys[normalized_name] = missing_foreign

    return OracleSchemaAuditReport(
        expected_table_count=len(metadata_tables),
        actual_table_count=len(actual_names),
        missing_tables=missing_tables,
        unexpected_tables=unexpected_tables,
        missing_columns=missing_columns,
        missing_indexes=missing_indexes,
        index_name_differences=index_name_differences,
        missing_unique_constraints=missing_unique_constraints,
        missing_foreign_keys=missing_foreign_keys,
        known_exceptions=known_exceptions,
        oracle_object_counts=_oracle_object_counts(engine),
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run a read-only Oracle schema audit."""
    load_dotenv()
    parser = argparse.ArgumentParser(description="Audit Oracle schema against SQLAlchemy models")
    parser.add_argument("--url", help="Oracle URL; defaults to ORACLE_TARGET_URL, OCI_DB_URL, or DATABASE_URL")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    url = args.url or os.getenv("ORACLE_TARGET_URL") or os.getenv("OCI_DB_URL") or os.getenv("DATABASE_URL")
    if not url or not url.startswith("oracle"):
        parser.error("An Oracle URL is required")

    engine = create_engine_for_url(url, tns_admin=os.getenv("TNS_ADMIN"))
    try:
        report = audit_oracle_schema(engine)
    finally:
        engine.dispose()

    sys.stdout.write(json.dumps(report.as_dict(), ensure_ascii=False, indent=2) + "\n")
    return 1 if report.schema_drift else 0


if __name__ == "__main__":
    raise SystemExit(main())
