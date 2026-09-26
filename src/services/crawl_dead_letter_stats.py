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
from src.utils.metrics import refresh_dlq_state_metrics

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
    oldest_pending_at: datetime | None = None
    oldest_pending_age_seconds: float | None = None
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
            "oldest_pending_at": self.oldest_pending_at.isoformat() if self.oldest_pending_at else None,
            "oldest_pending_age_seconds": self.oldest_pending_age_seconds,
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
        seconds=_env_seconds("DLQ_STALE_RETRYING_SECONDS", DEFAULT_STALE_RETRYING_SECONDS),
    )

    with factory() as session:
        repository = CrawlDeadLetterRepository(session)
        grouped = repository.count_by_status_crawler()
        due = repository.count_due(now=reference)
        stale_retrying = repository.count_stale_retrying(stale_before=cutoff)
        oldest_pending_at = repository.oldest_pending_created_at()

    by_status_crawler = {(status, crawler): count for status, crawler, count in grouped}
    totals: dict[str, int] = {}
    for (status, _crawler), count in by_status_crawler.items():
        totals[status] = totals.get(status, 0) + count

    age_seconds = (reference - oldest_pending_at).total_seconds() if oldest_pending_at is not None else None
    return DlqStats(
        pending=totals.get(DlqStatus.PENDING.value, 0),
        due=due,
        retrying=totals.get(DlqStatus.RETRYING.value, 0),
        stale_retrying=stale_retrying,
        resolved=totals.get(DlqStatus.RESOLVED.value, 0),
        exhausted=totals.get(DlqStatus.EXHAUSTED.value, 0),
        ignored=totals.get(DlqStatus.IGNORED.value, 0),
        oldest_pending_at=oldest_pending_at,
        oldest_pending_age_seconds=age_seconds,
        by_status_crawler=by_status_crawler,
    )


def publish_dlq_state_metrics(
    *,
    now: datetime | None = None,
    stale_before: datetime | None = None,
    session_factory: Callable[[], Session] | None = None,
) -> DlqStats:
    """Collect stats and refresh the Prometheus state gauges.

    Metrics are a side channel: a failure to refresh never raises, so it cannot
    break the worker/recovery/operator flow that triggered it.
    """
    stats = collect_dlq_stats(now=now, stale_before=stale_before, session_factory=session_factory)
    try:
        refresh_dlq_state_metrics(
            status_crawler_counts=stats.by_status_crawler,
            due=stats.due,
            stale_retrying=stats.stale_retrying,
            oldest_pending_age_seconds=stats.oldest_pending_age_seconds or 0.0,
        )
    except Exception:
        logger.exception("Failed to refresh DLQ state metrics")
    return stats
