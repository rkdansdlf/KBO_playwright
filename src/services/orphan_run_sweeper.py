"""Close run rows stranded in ``running`` by a process that died mid-crawl.

The ledger is the only durable record of what a crawl did, and it is also the
state space the alerts read. When a process dies between opening a run and
closing it, the row stays ``running`` forever: `started_at` is old, `finished_at`
is null, and no terminal transition ever happened. On 2026-10-08 one such row
was found five days old -- `crawl_execution_runs.id=37`, a `schedule` run for
`2026-10` started 10/3 08:09, during the database outage.

Replay runs were already covered. `recover_stuck_retrying` finalizes a replay
interrupted mid-flight, because the dead letter that caused it is still in the
queue. A general crawl run has no such guarantee, so nothing reclaimed it. This
is that missing half, and it matters more after an outage than in normal
operation: the database is the ledger, so a database that is down cannot record
anything, and the rows it was mid-way through writing are exactly the ones that
never arrive.

The recovery is a status transition, not a re-run. `RUN_INTERRUPTED` is
explicitly non-retryable, unlike `REPLAY_INTERRUPTED`: a replay has a dead
letter naming the unit to repeat, while a stranded general run only proves that
something stopped. Re-running it would guess where it stopped, and guessing is
what the ledger exists to prevent.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from src.crawlers.failure_taxonomy import FailureCode
from src.db.engine import SessionLocal
from src.models.crawl_execution import RUN_STATUS_RUNNING, CrawlExecutionRun
from src.services.crawl_run_service import CrawlRunService

logger = logging.getLogger(__name__)

#: How long a run may stay `running` before it is treated as abandoned.
#:
#: Separate from how often this runs, on purpose. The sweep ticks every 30
#: minutes with the dead letter recovery job, so the worst-case detection delay
#: is one tick -- but a crawl that legitimately runs for an hour or two must not
#: be closed out from under itself, and six hours clears that margin while
#: still finding a stranded row the same day. Override with
#: `CRAWL_RUN_STALE_SECONDS` when a job has a genuinely longer runtime.
DEFAULT_STALE_SECONDS = 6 * 60 * 60

#: Upper bound on rows closed per sweep.
#:
#: A database outage can strand many runs at once, and this runs inside
#: `MAINTENANCE_LOCK`. Closing them all in one transaction would hold the lock
#: for as long as the backlog, which is the contention the tiered locks exist to
#: avoid; the next tick continues where this one stopped.
DEFAULT_BATCH_LIMIT = 200

#: Sweep exceptions worth isolating per row. A row that cannot be closed must not
#: stop the others, because the ones after it may be the ones an operator is
#: waiting on. `SQLAlchemyError` is included because a constraint or connection
#: failure on one row is as specific to that row as a bad value.
SWEEP_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)


def _utcnow() -> datetime:
    """Return a naive UTC timestamp matching the rest of the schema."""
    return datetime.now(UTC).replace(tzinfo=None)


def stale_seconds() -> int:
    """Return the configured stale threshold in seconds."""
    raw = os.getenv("CRAWL_RUN_STALE_SECONDS")
    if not raw:
        return DEFAULT_STALE_SECONDS
    try:
        value = int(raw)
    except ValueError:
        logger.warning("CRAWL_RUN_STALE_SECONDS=%r is not an integer; using %d", raw, DEFAULT_STALE_SECONDS)
        return DEFAULT_STALE_SECONDS
    if value <= 0:
        logger.warning("CRAWL_RUN_STALE_SECONDS=%d must be positive; using %d", value, DEFAULT_STALE_SECONDS)
        return DEFAULT_STALE_SECONDS
    return value


@dataclass(frozen=True)
class OrphanSweepResult:
    """What one sweep closed and what it deliberately left alone."""

    finalized: int
    skipped_replays: int
    failed: int
    stale_threshold_seconds: int

    def to_dict(self) -> dict[str, int]:
        """Return the result as a plain mapping for logging and metrics."""
        return {
            "finalized": self.finalized,
            "skipped_replays": self.skipped_replays,
            "failed": self.failed,
            "stale_threshold_seconds": self.stale_threshold_seconds,
        }

    def summary(self) -> str:
        """Return a one-line description for the job's alert text."""
        return f"finalized={self.finalized}, skipped_replays={self.skipped_replays}, failed={self.failed}"


def find_stale_running_runs(
    *,
    session: object,
    now: datetime,
    older_than_seconds: int,
    limit: int = DEFAULT_BATCH_LIMIT,
) -> list[CrawlExecutionRun]:
    """Return runs still marked ``running`` past the stale threshold.

    Replays are excluded here rather than filtered afterwards. A stranded replay
    belongs to `recover_stuck_retrying`, which can act on it -- its dead letter
    is still queued -- and two recovery systems must never write the same row.
    """
    cutoff = now - timedelta(seconds=older_than_seconds)
    return list(
        session.scalars(
            select(CrawlExecutionRun)
            .where(CrawlExecutionRun.status == RUN_STATUS_RUNNING)
            .where(CrawlExecutionRun.started_at < cutoff)
            .where(CrawlExecutionRun.replay_of_run_id.is_(None))
            .order_by(CrawlExecutionRun.started_at)
            .limit(limit),
        ),
    )


def sweep_orphaned_runs(
    *,
    session_factory: object | None = None,
    now: datetime | None = None,
    older_than_seconds: int | None = None,
    limit: int = DEFAULT_BATCH_LIMIT,
) -> OrphanSweepResult:
    """Close abandoned runs, going through the service so metrics still move.

    `CrawlRunService.failed` rather than the repository's `mark_failed`: the
    terminal transition is what projects the run onto Prometheus, and writing the
    row directly would leave the ledger and the alerts disagreeing -- a stale
    `running` row corrected in the database while the metrics still show it in
    flight.

    Args:
        session_factory: Optional session factory override, for tests.
        now: Optional instant pinned for the whole sweep.
        older_than_seconds: Optional stale threshold override.
        limit: Maximum rows closed in one sweep.

    Returns:
        What the sweep closed, along with the threshold it used.

    """
    factory = session_factory or SessionLocal
    moment = now or _utcnow()
    threshold = older_than_seconds if older_than_seconds is not None else stale_seconds()

    finalized = 0
    failed = 0
    skipped_replays = 0

    with factory() as session:
        stranded = find_stale_running_runs(
            session=session,
            now=moment,
            older_than_seconds=threshold,
            limit=limit,
        )
        service = CrawlRunService(session)
        for run in stranded:
            try:
                service.failed(
                    run,
                    error_code=FailureCode.RUN_INTERRUPTED.value,
                    error_message=(
                        f"Run left in '{RUN_STATUS_RUNNING}' for over {threshold}s with no terminal transition; "
                        f"the process that started it ({run.origin or 'origin not recorded'}) did not finish."
                    ),
                    finished_at=moment,
                )
                finalized += 1
                logger.warning(
                    "Closed orphaned run %s (crawler=%s target=%s started=%s origin=%s)",
                    run.run_id,
                    run.crawler,
                    run.target_id,
                    run.started_at,
                    run.origin,
                )
            except SWEEP_EXCEPTIONS:
                # Isolate the row: the ones after it may be what an operator is
                # waiting on, and this failure says nothing about them.
                failed += 1
                logger.exception("Failed to close orphaned run %s", run.run_id)
                session.rollback()
        session.commit()

    # Reported so a sweep that quietly stops closing rows is distinguishable from
    # one that found none.
    skipped_replays = _count_stranded_replays(factory, moment, threshold)
    return OrphanSweepResult(
        finalized=finalized,
        skipped_replays=skipped_replays,
        failed=failed,
        stale_threshold_seconds=threshold,
    )


def _count_stranded_replays(session_factory: object, now: datetime, older_than_seconds: int) -> int:
    """Count stranded replay runs, which this sweep leaves to the DLQ recovery.

    Logged rather than acted on. Their presence is normal after a crash and they
    are not this sweep's to close, but a count that grows unbounded is worth
    seeing next to the finalized count.
    """
    cutoff = now - timedelta(seconds=older_than_seconds)
    try:
        with session_factory() as session:  # type: ignore[operator]
            rows = session.scalars(
                select(CrawlExecutionRun)
                .where(CrawlExecutionRun.status == RUN_STATUS_RUNNING)
                .where(CrawlExecutionRun.started_at < cutoff)
                .where(CrawlExecutionRun.replay_of_run_id.is_not(None)),
            ).all()
            return len(rows)
    except SWEEP_EXCEPTIONS:
        logger.exception("Failed to count stranded replay runs")
        return 0


__all__ = [
    "DEFAULT_BATCH_LIMIT",
    "DEFAULT_STALE_SECONDS",
    "OrphanSweepResult",
    "find_stale_running_runs",
    "stale_seconds",
    "sweep_orphaned_runs",
]
