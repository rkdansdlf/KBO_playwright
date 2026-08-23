"""Standard Data Transfer Objects (DTOs) for Schema Inspection and Parity Auditing."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class ColumnTypeCategory(StrEnum):
    """Categorized data types for cross-database (SQLite vs. Oracle) schema comparison."""

    NUMERIC = "numeric"
    TEXT = "text"
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    JSON = "json"
    VECTOR = "vector"
    OTHER = "other"


@dataclass
class ColumnSchemaMeta:
    """Metadata describing a single column in an ORM model or DB table."""

    name: str
    column_type: str
    category: ColumnTypeCategory
    is_nullable: bool = True
    is_primary_key: bool = False
    default_val: str | None = None
    foreign_keys: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert column metadata to dictionary."""
        return {
            "name": self.name,
            "column_type": self.column_type,
            "category": self.category.value,
            "is_nullable": self.is_nullable,
            "is_primary_key": self.is_primary_key,
            "default_val": self.default_val,
            "foreign_keys": self.foreign_keys,
        }


@dataclass
class TableSchemaMeta:
    """Metadata describing an entire database table or ORM entity."""

    table_name: str
    model_class_name: str = ""
    columns: dict[str, ColumnSchemaMeta] = field(default_factory=dict)
    primary_keys: list[str] = field(default_factory=list)
    foreign_keys: list[dict[str, str]] = field(default_factory=list)
    index_names: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert table metadata to dictionary."""
        return {
            "table_name": self.table_name,
            "model_class_name": self.model_class_name,
            "columns_count": len(self.columns),
            "columns": {k: v.to_dict() for k, v in self.columns.items()},
            "primary_keys": self.primary_keys,
            "foreign_keys": self.foreign_keys,
            "index_names": self.index_names,
        }


@dataclass
class SchemaDriftIssue:
    """Represents a discrepancy or drift detected between ORM models and database tables."""

    severity: str  # ERROR, WARN, INFO
    table_name: str
    issue_type: str  # MISSING_TABLE, MISSING_COLUMN, TYPE_MISMATCH, NULLABLE_MISMATCH, PK_MISMATCH
    message: str
    column_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert schema drift issue to dictionary."""
        return asdict(self)


@dataclass
class SchemaParityReport:
    """Aggregated schema audit report comparing ORM models with physical database catalog."""

    total_tables: int
    total_columns: int
    matched_tables: int
    drifted_tables: int
    issues: list[SchemaDriftIssue] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert schema parity report to dictionary."""
        return {
            "total_tables": self.total_tables,
            "total_columns": self.total_columns,
            "matched_tables": self.matched_tables,
            "drifted_tables": self.drifted_tables,
            "issues_count": len(self.issues),
            "issues": [i.to_dict() for i in self.issues],
        }
