"""Operational statistics for the dead letter queue.

Gauges are a projection of the current database state, not a running counter, so
they are recomputed from a snapshot after each worker/recovery/operator batch.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from src.db.engine import SessionLocal
from src.models.crawl_dead_letter import DlqStatus
from src.repositories.crawl_dead_letter_repository import CrawlDeadLetterRepository
from src.utils.metrics import (
    record_dlq_sweep_failed,
    record_dlq_sweep_succeeded,
    refresh_dlq_state_metrics,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

DEFAULT_STALE_RETRYING_SECONDS = 1800


@dataclass(frozen=True)
class DlqStats:
    """Snapshot of dead letter queue state."""

    pending: int = 0
    due: int = 0
    retrying: int = 0
    stale_retrying: int = 0
    resolved: int = 0
    exhausted: int = 0
    ignored: int = 0
    oldest_due_at: datetime | None = None
    oldest_due_age_seconds: float = 0.0
    by_status_crawler: dict[tuple[str, str], int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        """Return the stats as a JSON-serializable mapping."""
        return {
            "pending": self.pending,
            "due": self.due,
            "retrying": self.retrying,
            "stale_retrying": self.stale_retrying,
            "resolved": self.resolved,
            "exhausted": self.exhausted,
            "ignored": self.ignored,
            "oldest_due_at": self.oldest_due_at.isoformat() if self.oldest_due_at else None,
            "oldest_due_age_seconds": self.oldest_due_age_seconds,
            "by_status_crawler": {
                f"{status}:{crawler}": count for (status, crawler), count in self.by_status_crawler.items()
            },
        }


def _utcnow() -> datetime:
    """Return a naive UTC timestamp matching the rest of the schema."""
    return datetime.now(UTC).replace(tzinfo=None)


def _env_seconds(name: str, default: int) -> int:
    """Read a positive integer env override, falling back to the default."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def stale_retry_seconds() -> int:
    """Return the `retrying` staleness cutoff this module reads the queue with.

    Public because the number appears in operator-facing messages: an incident
    saying "stuck for 1800s" is only actionable if it is the same cutoff the
    reading used. Reading the env var separately at each site would let the two
    drift and produce a message describing a different threshold than the one
    that selected the letters.
    """
    return _env_seconds("DLQ_STALE_RETRYING_SECONDS", DEFAULT_STALE_RETRYING_SECONDS)


def collect_dlq_stats(
    *,
    now: datetime | None = None,
    stale_before: datetime | None = None,
    session_factory: Callable[[], Session] | None = None,
) -> DlqStats:
    """Read a consistent snapshot of dead letter queue state."""
    factory: Callable[[], Session] = session_factory or SessionLocal
    reference = now or _utcnow()
    cutoff = stale_before or reference - timedelta(
        seconds=stale_retry_seconds(),
    )

    with factory() as session:
        repository = CrawlDeadLetterRepository(session)
        grouped = repository.count_by_status_crawler()
        due = repository.count_due(now=reference)
        stale_retrying = repository.count_stale_retrying(stale_before=cutoff)
        oldest_due_at = repository.oldest_due_next_retry_at(now=reference)

    by_status_crawler = {(status, crawler): count for status, crawler, count in grouped}
    totals: dict[str, int] = {}
    for (status, _crawler), count in by_status_crawler.items():
        totals[status] = totals.get(status, 0) + count

    age_seconds = (reference - oldest_due_at).total_seconds() if oldest_due_at is not None else 0.0
    return DlqStats(
        pending=totals.get(DlqStatus.PENDING.value, 0),
        due=due,
        retrying=totals.get(DlqStatus.RETRYING.value, 0),
        stale_retrying=stale_retrying,
        resolved=totals.get(DlqStatus.RESOLVED.value, 0),
        exhausted=totals.get(DlqStatus.EXHAUSTED.value, 0),
        ignored=totals.get(DlqStatus.IGNORED.value, 0),
        oldest_due_at=oldest_due_at,
        oldest_due_age_seconds=age_seconds,
        by_status_crawler=by_status_crawler,
    )


def publish_dlq_state_metrics(
    *,
    now: datetime | None = None,
    stale_before: datetime | None = None,
    session_factory: Callable[[], Session] | None = None,
) -> DlqStats:
    """Collect stats and refresh the Prometheus state gauges.

    Two different failure guarantees live here, and conflating them is what left
    the heartbeat ambiguous in the first place:

    * Refreshing the gauges never raises. They are a side channel, so a broken
      scrape registration cannot break the worker or recovery flow that
      triggered the sweep.
    * Reading the queue *does* raise, because a caller that could not read it
      cannot act on the answer. That is the path where the sweep heartbeat
      records a failure and leaves the last known good timestamp alone.

    The heartbeat is recorded here rather than at each call site because this is
    the one place that knows whether the queue was actually read. Separating the
    two paths is what makes "the queue is empty" and "nobody could look"
    distinguishable, since a sweep that cannot reach the database leaves the
    previous gauge values in place rather than zeroing them.

    Args:
        now: Optional instant pinned for the whole read.
        stale_before: Optional cutoff for the stale-retrying count.
        session_factory: Optional session factory override, for tests.

    Returns:
        The stats read from the queue.

    Raises:
        SQLAlchemyError: Propagated from the read, after recording a failed sweep.

    """
    try:
        stats = collect_dlq_stats(now=now, stale_before=stale_before, session_factory=session_factory)
    except Exception:
        # Broad on purpose: this is the single place every sweep passes through,
        # and the metric records the outcome regardless of which error the
        # database layer raised. The original exception is re-raised untouched so
        # the caller's own handling is unchanged.
        logger.exception("Failed to read dead letter queue state")
        record_dlq_sweep_failed()
        raise

    try:
        refresh_dlq_state_metrics(
            status_crawler_counts=stats.by_status_crawler,
            due=stats.due,
            stale_retrying=stats.stale_retrying,
            oldest_due_age_seconds=stats.oldest_due_age_seconds,
        )
    except Exception:
        logger.exception("Failed to refresh DLQ state metrics")
    else:
        # The read completed, so the gauges describe the queue as of now.
        # `started_at`-style columns are naive UTC across this schema, and
        # `.timestamp()` reads a naive datetime as local time -- up to nine hours
        # of error on the gauge that exists to say "how stale is this reading".
        moment = now or _utcnow()
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        record_dlq_sweep_succeeded(moment.timestamp())
    return stats


def record_dlq_sweep_failure() -> None:
    """Record a sweep that could not read the queue.

    `publish_dlq_state_metrics` calls this itself, so a caller that sweeps through
    it needs nothing. This exists for the one case it cannot cover: a caller that
    decides the sweep failed *after* the read returned -- a partial result, or a
    downstream check that rejected it -- where the read looked fine but the sweep
    as a whole did not happen.

    The success timestamp is never touched here, for the same reason it is not
    touched on a read failure: the last known good reading is the only evidence
    available when the current one is missing.
    """
    record_dlq_sweep_failed()
