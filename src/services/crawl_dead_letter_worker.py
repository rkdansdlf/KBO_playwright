"""Dead letter retry worker.

Consumes ``pending`` letters whose ``next_retry_at`` has elapsed and drives them
through :func:`retry_dead_letter`. Each letter is isolated so one failure cannot
stop the batch; crash recovery for letters stranded in ``retrying`` is handled
separately by :mod:`src.services.crawl_dead_letter_recovery`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from src.db.engine import SessionLocal
from src.models.crawl_dead_letter import DlqStatus
from src.repositories.crawl_dead_letter_repository import CrawlDeadLetterRepository
from src.services.crawl_dead_letter_service import retry_dead_letter
from src.services.crawl_replay_dispatcher import ReplayDispatcher, build_default_dispatcher

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DlqWorkerSummary:
    """Aggregate outcome of one worker pass."""

    attempted: int = 0
    resolved: int = 0
    pending: int = 0
    exhausted: int = 0
    errored: int = 0

    def to_dict(self) -> dict[str, int]:
        """Return the summary as a plain mapping."""
        return {
            "attempted": self.attempted,
            "resolved": self.resolved,
            "pending": self.pending,
            "exhausted": self.exhausted,
            "errored": self.errored,
        }


def _utcnow() -> datetime:
    """Return a naive UTC timestamp matching the rest of the schema."""
    return datetime.now(UTC).replace(tzinfo=None)


def retry_due_dead_letters(
    *,
    now: datetime | None = None,
    limit: int = 50,
    dispatcher: ReplayDispatcher | None = None,
    session_factory: Callable[[], Session] | None = None,
) -> DlqWorkerSummary:
    """Retry all due ``pending`` dead letters, isolating per-letter failures."""
    factory: Callable[[], Session] = session_factory or SessionLocal
    reference = now or _utcnow()

    with factory() as session:
        due = CrawlDeadLetterRepository(session).get_retryable(now=reference, limit=limit)
        dlq_ids = [letter.dlq_id for letter in due]

    active_dispatcher = dispatcher if dispatcher is not None else build_default_dispatcher()

    resolved = pending = exhausted = errored = 0
    for dlq_id in dlq_ids:
        try:
            result = retry_dead_letter(dlq_id, active_dispatcher, session_factory=factory)
        except Exception:
            errored += 1
            logger.exception("DLQ retry worker failed for dlq_id=%s", dlq_id)
            continue
        if result.status is DlqStatus.RESOLVED:
            resolved += 1
        elif result.status is DlqStatus.EXHAUSTED:
            exhausted += 1
        else:
            pending += 1

    return DlqWorkerSummary(
        attempted=len(dlq_ids),
        resolved=resolved,
        pending=pending,
        exhausted=exhausted,
        errored=errored,
    )
