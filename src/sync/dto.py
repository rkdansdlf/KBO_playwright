"""Standard Data Transfer Objects (DTOs) for Database Synchronization and Cloud Data Lake Operations."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class SyncExecutionMode(StrEnum):
    """Execution mode for database synchronization."""

    INCREMENTAL = "incremental"
    FULL = "full"
    SNAPSHOT = "snapshot"
    VERIFY_ONLY = "verify_only"


@dataclass
class SyncEngineConfig:
    """Configuration options for OciSyncEngine."""

    mode: str = "incremental"
    apply: bool = False
    concurrency: int = 4
    batch_size: int = 5000


@dataclass
class SyncTablePlan:
    """Planning metadata for syncing a single table."""

    table_name: str
    level: int
    strategy: str
    candidate_count: int = 0
    is_dirty: bool = True
    reason: str = "initial"

    def to_dict(self) -> dict[str, Any]:
        """Convert table plan to dictionary."""
        return asdict(self)


@dataclass
class TableSyncResult:
    """Execution result for a single table synchronization."""

    table_name: str
    level: int
    strategy: str
    candidates_count: int
    synced_count: int
    error_count: int
    elapsed_seconds: float
    status: str  # SUCCESS, SKIPPED, FAILED, DRY_RUN
    oci_total_after: int | None = None
    message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary."""
        return asdict(self)


@dataclass
class SyncRunSummary:
    """Aggregated synchronization run report."""

    run_id: str
    started_at: str
    completed_at: str
    total_elapsed_seconds: float
    mode: str
    apply: bool
    tables_total: int
    tables_synced: int
    tables_skipped: int
    tables_failed: int
    total_rows_synced: int
    table_results: list[TableSyncResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert summary to dictionary."""
        return {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "total_elapsed_seconds": round(self.total_elapsed_seconds, 3),
            "mode": self.mode,
            "apply": self.apply,
            "tables_total": self.tables_total,
            "tables_synced": self.tables_synced,
            "tables_skipped": self.tables_skipped,
            "tables_failed": self.tables_failed,
            "total_rows_synced": self.total_rows_synced,
            "table_results": [t.to_dict() for t in self.table_results],
        }


@dataclass
class ConsistencyCheckItem:
    """Row count and integrity comparison for a single table."""

    table_name: str
    level: int
    sqlite_count: int
    oci_count: int
    diff: int
    status: str  # MATCH, MISMATCH, OCI_EMPTY, SQLITE_EMPTY, ERROR
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert check item to dictionary."""
        return asdict(self)


@dataclass
class SyncVerificationReport:
    """Comprehensive consistency verification report across all tables."""

    timestamp: str
    overall_status: str  # PASS, WARN, FAIL
    tables_checked: int
    matching_tables: int
    mismatched_tables: int
    error_tables: int
    details: list[ConsistencyCheckItem] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert verification report to dictionary."""
        return {
            "timestamp": self.timestamp,
            "overall_status": self.overall_status,
            "tables_checked": self.tables_checked,
            "matching_tables": self.matching_tables,
            "mismatched_tables": self.mismatched_tables,
            "error_tables": self.error_tables,
            "details": [d.to_dict() for d in self.details],
        }
