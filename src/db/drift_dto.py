"""Data Transfer Objects for Database Schema Drift Detection and DDL Generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class DriftType(StrEnum):
    """Types of schema differences between live DB and ORM models."""

    MISSING_TABLE = "MISSING_TABLE"
    EXTRA_TABLE = "EXTRA_TABLE"
    MISSING_COLUMN = "MISSING_COLUMN"
    EXTRA_COLUMN = "EXTRA_COLUMN"
    TYPE_MISMATCH = "TYPE_MISMATCH"
    NULLABILITY_MISMATCH = "NULLABILITY_MISMATCH"
    MISSING_INDEX = "MISSING_INDEX"


class DriftSeverity(StrEnum):
    """Severity levels for detected schema drift."""

    HIGH = "HIGH"  # Missing tables or non-nullable columns
    MEDIUM = "MEDIUM"  # Missing nullable columns or type mismatches
    LOW = "LOW"  # Missing indexes or extra objects


@dataclass
class SchemaDriftItem:
    """Represents a single detected schema drift occurrence."""

    drift_type: DriftType
    table_name: str
    object_name: str
    expected: str
    actual: str
    severity: DriftSeverity = DriftSeverity.MEDIUM
    ddl_statement: str | None = None
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert drift item to dictionary."""
        return {
            "drift_type": self.drift_type.value,
            "table_name": self.table_name,
            "object_name": self.object_name,
            "expected": self.expected,
            "actual": self.actual,
            "severity": self.severity.value,
            "ddl_statement": self.ddl_statement,
            "description": self.description,
        }


@dataclass
class SchemaDriftReport:
    """Aggregated schema drift detection report."""

    dialect: str
    database_url: str
    total_tables_checked: int
    drift_count: int
    drifts: list[SchemaDriftItem] = field(default_factory=list)
    generated_ddl: list[str] = field(default_factory=list)
    is_synced: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert drift report to serializable dictionary."""
        return {
            "dialect": self.dialect,
            "database_url": self.database_url,
            "total_tables_checked": self.total_tables_checked,
            "drift_count": self.drift_count,
            "is_synced": self.is_synced,
            "drifts": [d.to_dict() for d in self.drifts],
            "generated_ddl": self.generated_ddl,
        }

    def to_markdown(self) -> str:
        """Render report as a markdown document."""
        lines = [
            f"# 🔍 Database Schema Drift Report ({self.dialect.upper()})",
            "",
            f"- **Target Dialect**: `{self.dialect}`",
            f"- **Tables Checked**: {self.total_tables_checked}",
            f"- **Total Drifts Detected**: {self.drift_count}",
            f"- **Schema In Sync**: {'✅ Yes' if self.is_synced else '❌ No (Action Required)'}",
            "",
        ]

        if not self.drifts:
            lines.append("🎉 **No schema drift detected.** Live database matches SQLAlchemy ORM metadata perfectly.")
            return "\n".join(lines)

        lines.extend(
            [
                "## 📋 Detected Schema Drifts",
                "",
                "| Severity | Drift Type | Table | Target Object | Expected | Actual |",
                "|:---:|:---:|:---:|:---:|:---:|:---:|",
            ]
        )

        for d in self.drifts:
            if d.severity == DriftSeverity.HIGH:
                sev_icon = "🔴"
            elif d.severity == DriftSeverity.MEDIUM:
                sev_icon = "🟡"
            else:
                sev_icon = "🔵"
            lines.append(
                f"| {sev_icon} {d.severity.value} | `{d.drift_type.value}` | `{d.table_name}` | "
                f"`{d.object_name}` | {d.expected} | {d.actual} |"
            )

        if self.generated_ddl:
            lines.extend(
                [
                    "",
                    "## 🛠️ Remediation DDL Statements",
                    "",
                    f"```{self.dialect}",
                ]
            )
            lines.extend(self.generated_ddl)
            lines.append("```")

        return "\n".join(lines)


__all__ = ["DriftSeverity", "DriftType", "SchemaDriftItem", "SchemaDriftReport"]
