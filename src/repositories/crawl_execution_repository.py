"""Repository for the generic crawl execution run ledger."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import select

from src.models.crawl_execution import (
    RUN_STATUS_FAILED,
    RUN_STATUS_PARTIAL,
    RUN_STATUS_RUNNING,
    RUN_STATUS_SUCCESS,
    CrawlExecutionRun,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _utcnow() -> datetime:
    """Return a naive UTC timestamp matching the rest of the schema."""
    return datetime.now(UTC).replace(tzinfo=None)


@dataclass(frozen=True)
class CrawlRunSpec:
    """Immutable description of a crawl execution to record."""

    crawler: str
    target_type: str
    target_id: str | None = None
    season: int | None = None
    game_id: str | None = None
    attempt: int = 1
    source_url: str | None = None
    parser_version: str | None = None
    parent_run_id: str | None = None
    replay_of_run_id: str | None = None
    snapshot_id: int | None = None
    evidence_id: int | None = None
    run_id: str | None = None


class CrawlExecutionRepository:
    """Persist crawl execution runs without owning the transaction boundary."""

    def __init__(self, session: Session) -> None:
        """Initialize the repository with a caller-managed session."""
        self.session = session

    def start_run(self, spec: CrawlRunSpec) -> CrawlExecutionRun:
        """Insert a new run in the ``running`` state and flush it."""
        run = CrawlExecutionRun(
            run_id=spec.run_id or uuid4().hex,
            crawler=spec.crawler,
            target_type=spec.target_type,
            target_id=spec.target_id,
            season=spec.season,
            game_id=spec.game_id,
            status=RUN_STATUS_RUNNING,
            attempt=spec.attempt,
            started_at=_utcnow(),
            source_url=spec.source_url,
            parser_version=spec.parser_version,
            parent_run_id=spec.parent_run_id,
            replay_of_run_id=spec.replay_of_run_id,
            snapshot_id=spec.snapshot_id,
            evidence_id=spec.evidence_id,
        )
        self.session.add(run)
        self.session.flush()
        return run

    def mark_success(  # noqa: PLR0913
        self,
        run: CrawlExecutionRun,
        *,
        records_read: int | None = None,
        records_written: int | None = None,
        records_failed: int | None = None,
        checkpoint: dict | None = None,
        finished_at: datetime | None = None,
    ) -> CrawlExecutionRun:
        """Finalize a run as successful."""
        return self._finalize(
            run,
            RUN_STATUS_SUCCESS,
            records_read=records_read,
            records_written=records_written,
            records_failed=records_failed,
            checkpoint=checkpoint,
            finished_at=finished_at,
        )

    def mark_partial(  # noqa: PLR0913
        self,
        run: CrawlExecutionRun,
        *,
        records_read: int | None = None,
        records_written: int | None = None,
        records_failed: int | None = None,
        checkpoint: dict | None = None,
        finished_at: datetime | None = None,
    ) -> CrawlExecutionRun:
        """Finalize a run as partially successful."""
        return self._finalize(
            run,
            RUN_STATUS_PARTIAL,
            records_read=records_read,
            records_written=records_written,
            records_failed=records_failed,
            checkpoint=checkpoint,
            finished_at=finished_at,
        )

    def mark_failed(  # noqa: PLR0913
        self,
        run: CrawlExecutionRun,
        *,
        error_code: str,
        error_message: str,
        records_read: int | None = None,
        records_written: int | None = None,
        records_failed: int | None = None,
        finished_at: datetime | None = None,
    ) -> CrawlExecutionRun:
        """Finalize a run as failed with an error classification."""
        run.status = RUN_STATUS_FAILED
        run.error_code = error_code
        run.error_message = error_message
        _apply_counts(run, records_read, records_written, records_failed)
        run.finished_at = finished_at or _utcnow()
        self.session.flush()
        return run

    def increment_counts(
        self,
        run: CrawlExecutionRun,
        *,
        records_read: int = 0,
        records_written: int = 0,
        records_failed: int = 0,
    ) -> CrawlExecutionRun:
        """Atomically add the supplied deltas to the run counters."""
        run.records_read += records_read
        run.records_written += records_written
        run.records_failed += records_failed
        self.session.flush()
        return run

    def get_by_run_id(self, run_id: str) -> CrawlExecutionRun | None:
        """Return the run with the given ``run_id`` if it exists."""
        return self.session.execute(
            select(CrawlExecutionRun).where(CrawlExecutionRun.run_id == run_id),
        ).scalar_one_or_none()

    def list_recent(
        self,
        *,
        crawler: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[CrawlExecutionRun]:
        """Return recent runs, optionally filtered by crawler and status."""
        stmt = select(CrawlExecutionRun)
        if crawler is not None:
            stmt = stmt.where(CrawlExecutionRun.crawler == crawler)
        if status is not None:
            stmt = stmt.where(CrawlExecutionRun.status == status)
        stmt = stmt.order_by(CrawlExecutionRun.started_at.desc(), CrawlExecutionRun.id.desc()).limit(limit)
        return list(self.session.execute(stmt).scalars().all())

    def _finalize(  # noqa: PLR0913
        self,
        run: CrawlExecutionRun,
        status: str,
        *,
        records_read: int | None,
        records_written: int | None,
        records_failed: int | None,
        checkpoint: dict | None,
        finished_at: datetime | None,
    ) -> CrawlExecutionRun:
        """Apply final status, counts, and timestamps to a run."""
        run.status = status
        _apply_counts(run, records_read, records_written, records_failed)
        if checkpoint is not None:
            run.checkpoint = checkpoint
        run.finished_at = finished_at or _utcnow()
        self.session.flush()
        return run


def _apply_counts(
    run: CrawlExecutionRun,
    records_read: int | None,
    records_written: int | None,
    records_failed: int | None,
) -> None:
    """Overwrite counters on the run only when values are supplied."""
    if records_read is not None:
        run.records_read = records_read
    if records_written is not None:
        run.records_written = records_written
    if records_failed is not None:
        run.records_failed = records_failed
