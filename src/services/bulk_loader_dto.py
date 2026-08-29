"""Data Transfer Objects and Models for Bulk Chunk Loader & Checkpoint Pipeline."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any

MAX_VISIBLE_PARTITIONS = 8
PERCENT_BAR_STEP = 5.0
MAX_BAR_LENGTH = 20


class ChunkProgressStatus(StrEnum):
    """Execution status for an individual chunk partition."""

    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass
class ChunkPartition:
    """Definition of a chunk partition for batch loading."""

    partition_id: str
    category: str
    start_key: int | str
    end_key: int | str
    total_items: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert chunk partition to dictionary."""
        return asdict(self)


@dataclass
class ChunkCheckpoint:
    """Progress and execution state of a chunk partition."""

    partition_id: str
    status: ChunkProgressStatus = ChunkProgressStatus.PENDING
    items_total: int = 0
    items_completed: int = 0
    error_message: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    duration_seconds: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert chunk checkpoint to dictionary."""
        d = asdict(self)
        d["status"] = self.status.value
        return d


@dataclass
class BulkLoadManifest:
    """Overall manifest for a multi-partition bulk loading job."""

    job_id: str
    category: str
    start_year: int
    end_year: int
    concurrency: int = 4
    chunk_size: int = 1
    partitions: list[ChunkCheckpoint] = field(default_factory=list)
    is_resumed: bool = False
    started_at: str | None = None
    completed_at: str | None = None
    total_duration_seconds: float = 0.0

    @property
    def total_partitions(self) -> int:
        """Get total number of partitions."""
        return len(self.partitions)

    @property
    def completed_partitions(self) -> int:
        """Get number of completed partitions."""
        return sum(1 for p in self.partitions if p.status == ChunkProgressStatus.COMPLETED)

    @property
    def failed_partitions(self) -> int:
        """Get number of failed partitions."""
        return sum(1 for p in self.partitions if p.status == ChunkProgressStatus.FAILED)

    @property
    def pending_partitions(self) -> int:
        """Get number of pending partitions."""
        return sum(1 for p in self.partitions if p.status == ChunkProgressStatus.PENDING)

    @property
    def total_records_processed(self) -> int:
        """Get sum of processed records across partitions."""
        return sum(p.items_completed for p in self.partitions)

    def to_ascii_summary(self) -> str:
        """Render a terminal ASCII progress summary card."""
        pct = (self.completed_partitions / self.total_partitions * 100.0) if self.total_partitions > 0 else 0.0
        bar_len = round(pct / PERCENT_BAR_STEP)
        empty_len = MAX_BAR_LENGTH - bar_len
        bar_visual = f"[{'█' * bar_len}{'░' * empty_len}]"

        prog_line = (
            f"║ Progress: {pct:5.1f}% {bar_visual} ({self.completed_partitions}/{self.total_partitions} partitions)"
        )
        rec_line = f"║ Records: {self.total_records_processed:,} processed | Failures: {self.failed_partitions}"

        lines = [
            "╔════════════════════════════════════════════════════════════════════╗",
            f"║ 🚀 KBO BULK LOADER MANIFEST: {self.category.upper()} ({self.start_year}~{self.end_year})".ljust(68)
            + "║",
            f"║ Job ID: {self.job_id} | Workers: {self.concurrency} | Resumed: {self.is_resumed}".ljust(68) + "║",
            "╠════════════════════════════════════════════════════════════════════╣",
            prog_line.ljust(68) + "║",
            rec_line.ljust(68) + "║",
            f"║ Elapsed Time: {self.total_duration_seconds:.2f}s".ljust(68) + "║",
            "╠════════════════════════════════════════════════════════════════════╣",
            "║ [PARTITION DETAILS]                                                ║",
        ]
        for p in self.partitions[:MAX_VISIBLE_PARTITIONS]:
            if p.status == ChunkProgressStatus.COMPLETED:
                status_icon = "✅"
            elif p.status == ChunkProgressStatus.FAILED:
                status_icon = "❌"
            else:
                status_icon = "⏳"
            p_line = (
                f"║  {status_icon} [{p.status.value:<9}] {p.partition_id:<16} "
                f"({p.items_completed:,} items, {p.duration_seconds:4.2f}s)"
            )
            lines.append(p_line.ljust(68) + "║")

        if len(self.partitions) > MAX_VISIBLE_PARTITIONS:
            remaining = len(self.partitions) - MAX_VISIBLE_PARTITIONS
            lines.append(f"║  ... and {remaining} more partitions".ljust(68) + "║")

        lines.append("╚════════════════════════════════════════════════════════════════════╝")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Convert bulk load manifest to dictionary."""
        return {
            "job_id": self.job_id,
            "category": self.category,
            "start_year": self.start_year,
            "end_year": self.end_year,
            "concurrency": self.concurrency,
            "chunk_size": self.chunk_size,
            "is_resumed": self.is_resumed,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "total_duration_seconds": round(self.total_duration_seconds, 2),
            "total_partitions": self.total_partitions,
            "completed_partitions": self.completed_partitions,
            "failed_partitions": self.failed_partitions,
            "pending_partitions": self.pending_partitions,
            "total_records_processed": self.total_records_processed,
            "partitions": [p.to_dict() for p in self.partitions],
        }


__all__ = [
    "MAX_BAR_LENGTH",
    "MAX_VISIBLE_PARTITIONS",
    "PERCENT_BAR_STEP",
    "BulkLoadManifest",
    "ChunkCheckpoint",
    "ChunkPartition",
    "ChunkProgressStatus",
]
