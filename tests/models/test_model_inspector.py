"""Unit tests for src.models.inspector."""

from __future__ import annotations

import json

from sqlalchemy import Column, ForeignKeyConstraint, Index, Integer, MetaData, String, Table, create_engine

from src.models.dto import ColumnTypeCategory
from src.models.inspector import ModelInspector, categorize_column_type


def test_categorize_column_type() -> None:
    assert categorize_column_type(Integer()) == ColumnTypeCategory.NUMERIC
    assert categorize_column_type(String(50)) == ColumnTypeCategory.TEXT
    assert categorize_column_type("VARCHAR2(100)") == ColumnTypeCategory.TEXT
    assert categorize_column_type("NUMBER(10)") == ColumnTypeCategory.NUMERIC
    assert categorize_column_type("TIMESTAMP") == ColumnTypeCategory.DATETIME
    assert categorize_column_type("BOOLEAN") == ColumnTypeCategory.BOOLEAN
    assert categorize_column_type("JSON") == ColumnTypeCategory.JSON


def test_inspect_models_and_compare_clean() -> None:
    custom_meta = MetaData()
    Table(
        "sample_table",
        custom_meta,
        Column("id", Integer, primary_key=True),
        Column("name", String(100), nullable=False),
    )

    engine = create_engine("sqlite:///:memory:")
    custom_meta.create_all(engine)

    inspector = ModelInspector(metadata=custom_meta)
    report = inspector.audit_engine(engine)

    assert report.total_tables == 1
    assert report.total_columns == 2
    assert report.matched_tables == 1
    assert report.drifted_tables == 0
    assert len(report.issues) == 0


def test_compare_schemas_detects_missing_table_and_columns() -> None:
    custom_meta = MetaData()
    Table(
        "table_a",
        custom_meta,
        Column("id", Integer, primary_key=True),
        Column("col_missing", String(50)),
    )
    Table(
        "table_missing",
        custom_meta,
        Column("id", Integer, primary_key=True),
    )

    # In DB, table_missing doesn't exist, and table_a only has 'id'
    db_engine = create_engine("sqlite:///:memory:")
    db_meta = MetaData()
    Table(
        "table_a",
        db_meta,
        Column("id", Integer, primary_key=True),
    )
    db_meta.create_all(db_engine)

    inspector = ModelInspector(metadata=custom_meta)
    report = inspector.audit_engine(db_engine)

    assert report.total_tables == 2
    assert report.drifted_tables == 2
    issue_types = [i.issue_type for i in report.issues]
    assert "MISSING_TABLE" in issue_types
    assert "MISSING_COLUMN" in issue_types


def test_inspect_database_preserves_fk_and_index_metadata() -> None:
    """Regression: FK rows keep their list payloads and index names survive reflection."""
    custom_meta = MetaData()
    Table(
        "orders",
        custom_meta,
        Column("id", Integer, primary_key=True),
        Column("customer_id", Integer),
        ForeignKeyConstraint(["customer_id"], ["customers.id"]),
        Index("idx_orders_customer", "customer_id"),
    )
    Table(
        "customers",
        custom_meta,
        Column("id", Integer, primary_key=True),
    )

    engine = create_engine("sqlite:///:memory:")
    custom_meta.create_all(engine)

    inspector = ModelInspector(metadata=custom_meta)
    db_tables = inspector.inspect_database(engine)

    orders = db_tables["orders"]
    assert orders.index_names == ["idx_orders_customer"]
    assert len(orders.foreign_keys) == 1
    fk = orders.foreign_keys[0]
    assert fk["referred_table"] == "customers"
    assert fk["constrained_columns"] == ["customer_id"]

    # to_dict round-trip preserves the new union value type
    payload = json.dumps(orders.to_dict(), default=str)
    assert "idx_orders_customer" in payload
    assert "customers" in payload
