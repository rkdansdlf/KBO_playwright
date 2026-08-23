"""Unit tests for src.models.dto."""

from __future__ import annotations

from src.models.dto import (
    ColumnSchemaMeta,
    ColumnTypeCategory,
    SchemaDriftIssue,
    SchemaParityReport,
    TableSchemaMeta,
)


def test_column_type_category_values() -> None:
    assert ColumnTypeCategory.NUMERIC == "numeric"
    assert ColumnTypeCategory.TEXT == "text"
    assert ColumnTypeCategory.DATETIME == "datetime"
    assert ColumnTypeCategory.BOOLEAN == "boolean"
    assert ColumnTypeCategory.JSON == "json"
    assert ColumnTypeCategory.VECTOR == "vector"
    assert ColumnTypeCategory.OTHER == "other"


def test_column_schema_meta_to_dict() -> None:
    meta = ColumnSchemaMeta(
        name="player_id",
        column_type="INTEGER",
        category=ColumnTypeCategory.NUMERIC,
        is_nullable=False,
        is_primary_key=True,
    )
    d = meta.to_dict()
    assert d["name"] == "player_id"
    assert d["category"] == "numeric"
    assert d["is_primary_key"] is True


def test_table_schema_meta_to_dict() -> None:
    col = ColumnSchemaMeta(
        name="id",
        column_type="INTEGER",
        category=ColumnTypeCategory.NUMERIC,
        is_primary_key=True,
    )
    table = TableSchemaMeta(
        table_name="test_table",
        model_class_name="TestModel",
        columns={"id": col},
        primary_keys=["id"],
    )
    d = table.to_dict()
    assert d["table_name"] == "test_table"
    assert d["columns_count"] == 1
    assert "id" in d["columns"]


def test_schema_parity_report_to_dict() -> None:
    issue = SchemaDriftIssue(
        severity="ERROR",
        table_name="missing_table",
        issue_type="MISSING_TABLE",
        message="Table missing in database",
    )
    report = SchemaParityReport(
        total_tables=10,
        total_columns=50,
        matched_tables=9,
        drifted_tables=1,
        issues=[issue],
    )
    d = report.to_dict()
    assert d["total_tables"] == 10
    assert d["issues_count"] == 1
    assert d["issues"][0]["issue_type"] == "MISSING_TABLE"
