"""Standard Data Transfer Objects (DTOs) for Database Migrations and DDL Versioning."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class MigrationDialect(StrEnum):
    """Supported database dialects for schema migrations."""

    ORACLE = "oracle"
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    PGVECTOR = "pgvector"


@dataclass
class MigrationFileMeta:
    """Metadata describing a single versioned SQL migration file."""

    version: int
    filename: str
    path: str
    dialect: MigrationDialect
    is_safety_gated: bool = False
    checksum: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert migration file metadata to dictionary."""
        return {
            "version": self.version,
            "filename": self.filename,
            "path": self.path,
            "dialect": self.dialect.value,
            "is_safety_gated": self.is_safety_gated,
            "checksum": self.checksum,
        }


@dataclass
class MigrationExecutionResult:
    """Result of executing or simulating a single migration file."""

    filename: str
    version: int
    status: str  # APPLIED, SKIPPED, FAILED
    duration_seconds: float = 0.0
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert execution result to dictionary."""
        return {
            "filename": self.filename,
            "version": self.version,
            "status": self.status,
            "duration_seconds": round(self.duration_seconds, 3),
            "error_message": self.error_message,
        }


@dataclass
class MigrationStatusReport:
    """Aggregated status report of database schema migrations for a dialect."""

    dialect: str
    total_available: int
    applied_count: int
    pending_count: int
    applied_versions: list[int] = field(default_factory=list)
    pending_versions: list[int] = field(default_factory=list)
    results: list[MigrationExecutionResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert migration status report to dictionary."""
        return {
            "dialect": self.dialect,
            "total_available": self.total_available,
            "applied_count": self.applied_count,
            "pending_count": self.pending_count,
            "applied_versions": self.applied_versions,
            "pending_versions": self.pending_versions,
            "results": [r.to_dict() for r in self.results],
        }
