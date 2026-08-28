"""Database schema drift detection engine and DDL remediation generator."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
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
    inspect,
    text,
)

from src.db.drift_dto import (
    DriftSeverity,
    DriftType,
    SchemaDriftItem,
    SchemaDriftReport,
)
from src.models.base import Base

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy import MetaData
    from sqlalchemy.engine import Connection, Engine
    from sqlalchemy.types import TypeEngine

logger = logging.getLogger(__name__)


class SchemaDriftDetector:
    """Detects discrepancies between live database schema catalog and SQLAlchemy ORM metadata."""

    def __init__(
        self,
        bind: Engine | Connection,
        *,
        metadata: MetaData | None = None,
        dialect: str | None = None,
    ) -> None:
        """Initialize schema drift detector with database engine/connection and ORM metadata."""
        self.bind = bind
        self.metadata = metadata or Base.metadata
        self.dialect = (dialect or getattr(getattr(bind, "dialect", None), "name", "sqlite")).lower()

    def _map_sa_type_to_dialect(self, sa_type: TypeEngine[Any]) -> str:  # noqa: C901, PLR0911, PLR0912
        """Convert SQLAlchemy column type to target dialect SQL DDL type."""
        d = self.dialect

        if isinstance(sa_type, String):
            length = sa_type.length or 255
            if d == "oracle":
                return f"VARCHAR2({length})"
            if d == "sqlite":
                return "TEXT"
            return f"VARCHAR({length})"

        if isinstance(sa_type, Text):
            if d == "oracle":
                return "CLOB"
            return "TEXT"

        if isinstance(sa_type, (Integer, SmallInteger, BigInteger)):
            if d == "oracle":
                if isinstance(sa_type, BigInteger):
                    return "NUMBER(19)"
                if isinstance(sa_type, SmallInteger):
                    return "NUMBER(5)"
                return "NUMBER(10)"
            if d == "sqlite":
                return "INTEGER"
            if isinstance(sa_type, BigInteger):
                return "BIGINT"
            return "INTEGER"

        if isinstance(sa_type, (Float, Numeric)):
            if d == "oracle":
                return "NUMBER(14,4)"
            if d == "sqlite":
                return "REAL"
            return "NUMERIC(14,4)"

        if isinstance(sa_type, Boolean):
            if d == "oracle":
                return "NUMBER(1)"
            if d == "sqlite":
                return "INTEGER"
            return "BOOLEAN"

        if isinstance(sa_type, Date):
            if d == "oracle":
                return "DATE"
            if d == "sqlite":
                return "TEXT"
            return "DATE"

        if isinstance(sa_type, DateTime):
            if d == "oracle":
                return "TIMESTAMP"
            if d == "sqlite":
                return "TEXT"
            return "TIMESTAMP"

        # Fallback to string representation of type
        return str(sa_type)

    def _generate_add_column_ddl(
        self,
        table_name: str,
        column_name: str,
        col_type_sql: str,
        *,
        nullable: bool,
    ) -> str:
        """Generate ALTER TABLE ADD COLUMN DDL statement for target dialect."""
        if self.dialect == "oracle":
            null_clause = "" if nullable else " NOT NULL"
            return f"ALTER TABLE {table_name} ADD ({column_name} {col_type_sql}{null_clause});"
        if self.dialect == "sqlite":
            return f"ALTER TABLE {table_name} ADD COLUMN {column_name} {col_type_sql};"
        # PostgreSQL / default
        null_clause = "" if nullable else " NOT NULL"
        return f"ALTER TABLE {table_name} ADD COLUMN {column_name} {col_type_sql}{null_clause};"

    def _generate_create_index_ddl(
        self,
        table_name: str,
        index_name: str,
        columns: list[str],
        *,
        unique: bool,
    ) -> str:
        """Generate CREATE INDEX DDL statement."""
        unique_clause = "UNIQUE " if unique else ""
        cols_str = ", ".join(columns)
        return f"CREATE {unique_clause}INDEX {index_name} ON {table_name} ({cols_str});"

    def detect_drift(
        self,
        table_filter: Sequence[str] | None = None,
    ) -> SchemaDriftReport:
        """Compare database catalog against ORM metadata and return comprehensive drift report."""
        inspector = inspect(self.bind)
        db_tables = set(inspector.get_table_names())
        orm_tables = self.metadata.tables

        filter_set = set(table_filter) if table_filter else None

        drifts: list[SchemaDriftItem] = []
        generated_ddl: list[str] = []
        checked_tables_count = 0

        for table_name, orm_table in orm_tables.items():
            if filter_set and table_name not in filter_set:
                continue

            checked_tables_count += 1

            # 1. Missing Table Check
            if table_name not in db_tables:
                item = SchemaDriftItem(
                    drift_type=DriftType.MISSING_TABLE,
                    table_name=table_name,
                    object_name=table_name,
                    expected=f"Table {table_name} defined in ORM",
                    actual="Table not found in database",
                    severity=DriftSeverity.HIGH,
                    description=f"Table {table_name} is missing from live database schema.",
                )
                drifts.append(item)
                continue

            # 2. Inspect Columns in Existing Table
            db_cols = {col["name"]: col for col in inspector.get_columns(table_name)}

            for col in orm_table.columns:
                col_name = col.name
                col_type_sql = self._map_sa_type_to_dialect(col.type)

                if col_name not in db_cols:
                    ddl = self._generate_add_column_ddl(
                        table_name,
                        col_name,
                        col_type_sql,
                        nullable=col.nullable,
                    )
                    generated_ddl.append(ddl)

                    item = SchemaDriftItem(
                        drift_type=DriftType.MISSING_COLUMN,
                        table_name=table_name,
                        object_name=col_name,
                        expected=f"Column {col_name} ({col_type_sql}, nullable={col.nullable})",
                        actual="Column missing in DB",
                        severity=DriftSeverity.HIGH if not col.nullable else DriftSeverity.MEDIUM,
                        ddl_statement=ddl,
                        description=f"Column {col_name} defined on model but missing in database table {table_name}.",
                    )
                    drifts.append(item)

            # 3. Inspect Indexes in Existing Table
            db_indexes = {idx["name"] for idx in inspector.get_indexes(table_name) if idx.get("name")}

            for orm_idx in orm_table.indexes:
                if orm_idx.name and orm_idx.name not in db_indexes:
                    col_names = [c.name for c in orm_idx.columns]
                    ddl = self._generate_create_index_ddl(
                        table_name,
                        orm_idx.name,
                        col_names,
                        unique=orm_idx.unique,
                    )
                    generated_ddl.append(ddl)

                    item = SchemaDriftItem(
                        drift_type=DriftType.MISSING_INDEX,
                        table_name=table_name,
                        object_name=orm_idx.name,
                        expected=f"Index {orm_idx.name} ({', '.join(col_names)})",
                        actual="Index missing in DB",
                        severity=DriftSeverity.LOW,
                        ddl_statement=ddl,
                        description=f"Index {orm_idx.name} is defined in ORM model but not created in database.",
                    )
                    drifts.append(item)

        url_str = str(getattr(getattr(self.bind, "engine", self.bind), "url", "unknown"))

        return SchemaDriftReport(
            dialect=self.dialect,
            database_url=url_str,
            total_tables_checked=checked_tables_count,
            drift_count=len(drifts),
            drifts=drifts,
            generated_ddl=generated_ddl,
            is_synced=len(drifts) == 0,
        )

    def apply_remediation(self, report: SchemaDriftReport) -> int:
        """Execute all generated remediation DDL statements."""
        if not report.generated_ddl:
            return 0

        applied_count = 0
        is_conn = hasattr(self.bind, "execute") and not hasattr(self.bind, "connect")

        if is_conn:
            for stmt in report.generated_ddl:
                clean_stmt = stmt.rstrip(";").strip()
                if clean_stmt:
                    self.bind.execute(text(clean_stmt))  # type: ignore[union-attr]
                    applied_count += 1
        else:
            with self.bind.connect() as conn, conn.begin():  # type: ignore[union-attr]
                for stmt in report.generated_ddl:
                    clean_stmt = stmt.rstrip(";").strip()
                    if clean_stmt:
                        conn.execute(text(clean_stmt))
                        applied_count += 1

        logger.info("Successfully applied %d remediation DDL statements", applied_count)
        return applied_count


__all__ = ["SchemaDriftDetector"]
