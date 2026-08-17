from __future__ import annotations

from sqlalchemy import Column, Index, Integer, MetaData, Table, UniqueConstraint

from scripts.verification.audit_oracle_schema import (
    OracleSchemaAuditReport,
    _actual_unique_constraints,
    _audit_indexes,
    _audit_unique_constraints,
)


class _InspectorStub:
    def __init__(
        self,
        *,
        indexes: list[dict[str, object]] | None = None,
        unique_constraints: list[dict[str, object]] | None = None,
    ) -> None:
        self._indexes = indexes or []
        self._unique_constraints = unique_constraints or []

    def get_indexes(self, _table_name: str) -> list[dict[str, object]]:
        return self._indexes

    def get_unique_constraints(self, _table_name: str) -> list[dict[str, object]]:
        return self._unique_constraints


def test_audit_indexes_accepts_equivalent_alternate_name() -> None:
    """Treat an equivalent Oracle index with a different name as non-drift."""
    metadata = MetaData()
    table = Table("example", metadata, Column("value", Integer))
    Index("ix_expected_value", table.c.value)
    inspector = _InspectorStub(
        indexes=[{"name": "idx_actual_value", "column_names": ["VALUE"], "unique": False}],
    )

    missing, name_differences = _audit_indexes(inspector, table, "EXAMPLE")

    assert missing == []
    assert name_differences[0]["expected"] == "ix_expected_value"
    assert name_differences[0]["actual"] == ["idx_actual_value"]


def test_actual_unique_constraints_include_unique_indexes() -> None:
    """Recognize Oracle unique indexes when reflection omits constraints."""
    inspector = _InspectorStub(
        indexes=[{"name": "uq_actual", "column_names": ["VALUE"], "unique": True}],
    )

    assert _actual_unique_constraints(inspector, "EXAMPLE") == {("value",)}


def test_audit_unique_constraints_records_the_documented_oracle_exception() -> None:
    """Keep the nullable stadium section-code exception out of drift findings."""
    metadata = MetaData()
    table = Table(
        "stadium_seat_sections",
        metadata,
        Column("stadium_id", Integer),
        Column("section_code", Integer),
        UniqueConstraint("stadium_id", "section_code"),
    )

    missing, exceptions = _audit_unique_constraints(
        _InspectorStub(),
        table,
        "STADIUM_SEAT_SECTIONS",
        "stadium_seat_sections",
    )

    assert missing == []
    assert exceptions[0]["object"] == "unique_constraint"


def test_schema_audit_report_ignores_name_only_differences() -> None:
    """Only missing model objects should make the report drift."""
    report = OracleSchemaAuditReport(
        expected_table_count=1,
        actual_table_count=2,
        missing_tables=[],
        unexpected_tables=["legacy_table"],
        missing_columns={},
        missing_indexes={},
        index_name_differences={"example": [{"expected": "ix_a", "actual": ["idx_a"]}]},
        missing_unique_constraints={},
        missing_foreign_keys={},
        known_exceptions=[{"table": "example", "object": "index"}],
        oracle_object_counts={},
    )

    assert report.schema_drift is False
