"""Declarative Schema Inspector and Parity Auditor for KBO Database Models."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import inspect
from sqlalchemy.types import (
    BOOLEAN,
    DATE,
    DATETIME,
    DECIMAL,
    FLOAT,
    INTEGER,
    JSON,
    NUMERIC,
    TEXT,
    TIME,
    TIMESTAMP,
    VARCHAR,
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    Time,
)

from src.models.base import Base
from src.models.dto import (
    ColumnSchemaMeta,
    ColumnTypeCategory,
    SchemaDriftIssue,
    SchemaParityReport,
    TableSchemaMeta,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.schema import MetaData

logger = logging.getLogger(__name__)

_NUMERIC_CLASSES = (
    Integer,
    INTEGER,
    BigInteger,
    SmallInteger,
    Numeric,
    NUMERIC,
    Float,
    FLOAT,
    DECIMAL,
)
_TEXT_CLASSES = (String, VARCHAR, Text, TEXT)
_DATETIME_CLASSES = (DateTime, DATETIME, Date, DATE, Time, TIME, TIMESTAMP)
_BOOLEAN_CLASSES = (Boolean, BOOLEAN)

_STRING_TYPE_MAP: tuple[tuple[tuple[str, ...], ColumnTypeCategory], ...] = (
    (("INT", "NUM", "FLOAT", "DOUBLE", "DECIMAL", "REAL", "BIGINT"), ColumnTypeCategory.NUMERIC),
    (("CHAR", "VARCHAR", "TEXT", "CLOB", "STR"), ColumnTypeCategory.TEXT),
    (("DATE", "TIME", "TIMESTAMP"), ColumnTypeCategory.DATETIME),
    (("BOOL", "TINYINT(1)"), ColumnTypeCategory.BOOLEAN),
    (("JSON",), ColumnTypeCategory.JSON),
    (("VECTOR",), ColumnTypeCategory.VECTOR),
)


def categorize_column_type(type_obj: object) -> ColumnTypeCategory:
    """Categorize SQLAlchemy or DB type into a normalized category for cross-engine parity."""
    category = ColumnTypeCategory.OTHER
    if isinstance(type_obj, _NUMERIC_CLASSES):
        category = ColumnTypeCategory.NUMERIC
    elif isinstance(type_obj, _TEXT_CLASSES):
        category = ColumnTypeCategory.TEXT
    elif isinstance(type_obj, _DATETIME_CLASSES):
        category = ColumnTypeCategory.DATETIME
    elif isinstance(type_obj, _BOOLEAN_CLASSES):
        category = ColumnTypeCategory.BOOLEAN
    elif isinstance(type_obj, JSON):
        category = ColumnTypeCategory.JSON
    else:
        type_str = str(type_obj).upper()
        for keywords, cat in _STRING_TYPE_MAP:
            if any(k in type_str for k in keywords):
                category = cat
                break
    return category


class ModelInspector:
    """Introspects ORM models and database catalogs to detect schema parity drifts."""

    def __init__(self, metadata: MetaData | None = None) -> None:
        """Initialize the model inspector."""
        self.metadata = metadata or Base.metadata

    def inspect_models(self) -> dict[str, TableSchemaMeta]:
        """Extract schema metadata for all declared SQLAlchemy ORM models."""
        tables_meta: dict[str, TableSchemaMeta] = {}

        for table_name, table in self.metadata.tables.items():
            columns_meta: dict[str, ColumnSchemaMeta] = {}
            primary_keys: list[str] = []

            for col in table.columns:
                is_pk = bool(col.primary_key)
                if is_pk:
                    primary_keys.append(col.name)

                fk_targets = [str(fk.target_fullname) for fk in col.foreign_keys]
                cat = categorize_column_type(col.type)

                columns_meta[col.name] = ColumnSchemaMeta(
                    name=col.name,
                    column_type=str(col.type),
                    category=cat,
                    is_nullable=bool(col.nullable),
                    is_primary_key=is_pk,
                    default_val=str(col.default) if col.default is not None else None,
                    foreign_keys=fk_targets,
                )

            foreign_keys_list = [
                {
                    "constrained_column": fk.parent.name if fk.parent is not None else "",
                    "target": str(fk.target_fullname),
                }
                for fk in table.foreign_keys
            ]
            index_names = [idx.name for idx in table.indexes if idx.name]

            tables_meta[table_name] = TableSchemaMeta(
                table_name=table_name,
                columns=columns_meta,
                primary_keys=primary_keys,
                foreign_keys=foreign_keys_list,
                index_names=index_names,
            )

        return tables_meta

    def inspect_database(self, engine: Engine) -> dict[str, TableSchemaMeta]:
        """Reflect and extract schema metadata directly from physical database catalog."""
        inspector = inspect(engine)
        tables_meta: dict[str, TableSchemaMeta] = {}

        table_names = inspector.get_table_names()
        for t_name in table_names:
            columns = inspector.get_columns(t_name)
            pk_constraint = inspector.get_pk_constraint(t_name)
            pks = pk_constraint.get("constrained_columns", []) if pk_constraint else []
            fks = inspector.get_foreign_keys(t_name)
            indexes = inspector.get_indexes(t_name)

            columns_meta: dict[str, ColumnSchemaMeta] = {}
            for col in columns:
                col_name = col["name"]
                is_pk = col_name in pks
                cat = categorize_column_type(col.get("type", "UNKNOWN"))

                columns_meta[col_name] = ColumnSchemaMeta(
                    name=col_name,
                    column_type=str(col.get("type", "UNKNOWN")),
                    category=cat,
                    is_nullable=bool(col.get("nullable", True)),
                    is_primary_key=is_pk,
                    default_val=str(col.get("default")) if col.get("default") is not None else None,
                )

            foreign_keys_list = [
                {
                    "constrained_columns": fk.get("constrained_columns", []),
                    "referred_table": fk.get("referred_table", ""),
                }
                for fk in fks
            ]
            index_names = [idx.get("name", "") for idx in indexes if idx.get("name")]

            tables_meta[t_name] = TableSchemaMeta(
                table_name=t_name,
                columns=columns_meta,
                primary_keys=pks,
                foreign_keys=foreign_keys_list,
                index_names=index_names,
            )

        return tables_meta

    def compare_schemas(
        self,
        orm_tables: dict[str, TableSchemaMeta],
        db_tables: dict[str, TableSchemaMeta],
    ) -> SchemaParityReport:
        """Compare declared ORM tables against reflected database catalog and detect drifts."""
        issues: list[SchemaDriftIssue] = []
        matched_tables = 0
        drifted_tables = 0
        total_columns = sum(len(t.columns) for t in orm_tables.values())

        for t_name, orm_t in orm_tables.items():
            if t_name not in db_tables:
                issues.append(
                    SchemaDriftIssue(
                        severity="ERROR",
                        table_name=t_name,
                        issue_type="MISSING_TABLE",
                        message=f"Table '{t_name}' declared in ORM but does not exist in database.",
                    )
                )
                drifted_tables += 1
                continue

            db_t = db_tables[t_name]
            table_has_drift = False

            # Compare Columns
            for col_name, orm_col in orm_t.columns.items():
                if col_name not in db_t.columns:
                    issues.append(
                        SchemaDriftIssue(
                            severity="ERROR",
                            table_name=t_name,
                            column_name=col_name,
                            issue_type="MISSING_COLUMN",
                            message=f"Column '{col_name}' declared in ORM table '{t_name}' missing in database.",
                        )
                    )
                    table_has_drift = True
                else:
                    db_col = db_t.columns[col_name]
                    # Category mismatch check
                    if {orm_col.category, db_col.category} <= {
                        ColumnTypeCategory.NUMERIC,
                        ColumnTypeCategory.TEXT,
                        ColumnTypeCategory.DATETIME,
                        ColumnTypeCategory.BOOLEAN,
                        ColumnTypeCategory.JSON,
                        ColumnTypeCategory.VECTOR,
                    } and orm_col.category != db_col.category:
                        issues.append(
                            SchemaDriftIssue(
                                severity="WARN",
                                table_name=t_name,
                                column_name=col_name,
                                issue_type="TYPE_MISMATCH",
                                message=(
                                    f"Column '{t_name}.{col_name}' category mismatch: "
                                    f"ORM={orm_col.category.value} vs DB={db_col.category.value}."
                                ),
                            )
                        )
                        table_has_drift = True

            if table_has_drift:
                drifted_tables += 1
            else:
                matched_tables += 1

        return SchemaParityReport(
            total_tables=len(orm_tables),
            total_columns=total_columns,
            matched_tables=matched_tables,
            drifted_tables=drifted_tables,
            issues=issues,
        )

    def audit_engine(self, engine: Engine) -> SchemaParityReport:
        """Run full schema parity audit on a live database engine."""
        orm_tables = self.inspect_models()
        db_tables = self.inspect_database(engine)
        return self.compare_schemas(orm_tables, db_tables)
