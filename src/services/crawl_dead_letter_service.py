"""Service layer for the crawler dead letter queue.

Retry orchestration spans two short transactions around a non-transactional
replay execution:

    txn1: validate transition -> increment retry -> mark retrying -> allocate
          replay run_id -> link it, then commit
    exec: dispatcher.replay(...)            (no transaction)
    txn2: finalize as resolved / pending / exhausted, then commit
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Protocol
from uuid import uuid4

from src.db.engine import SessionLocal
from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus
from src.repositories.crawl_dead_letter_repository import (
    CrawlDeadLetterRepository,
    DeadLetterSpec,
)
from src.services.crawl_dead_letter_state import (
    ensure_transition,
    next_status_after_retry,
)
from src.services.crawl_retry_policy import MAX_RETRIES, decide

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class DlqNotFoundError(LookupError):
    """Raised when a dead letter id cannot be found."""

    def __init__(self, dlq_id: str) -> None:
        """Initialize with the missing dead letter id."""
        self.dlq_id = dlq_id
        super().__init__(f"Dead letter not found: {dlq_id}")


class ReplayOutcomeLike(Protocol):
    """Structural contract for a replay outcome."""

    success: bool
    replay_run_id: str
    status: str
    error_message: str | None
    error_code: str | None
    failure_stage: str | None


class ReplayDispatcherProtocol(Protocol):
    """Structural contract for a replay dispatcher."""

    def replay(self, dead_letter: CrawlDeadLetter, *, replay_run_id: str) -> ReplayOutcomeLike:
        """Replay a dead letter, returning the outcome."""
        ...


@dataclass(frozen=True)
class DlqRetryResult:
    """Result of one dead letter retry orchestration."""

    dlq_id: str
    replay_run_id: str
    status: DlqStatus
    success: bool


def _utcnow() -> datetime:
    """Return a naive UTC timestamp matching the rest of the schema."""
    return datetime.now(UTC).replace(tzinfo=None)


class CrawlDeadLetterService:
    """Own dead letter mutations for a single caller-managed session."""

    def __init__(self, session: Session) -> None:
        """Initialize the service with a caller-managed session."""
        self.session = session
        self.repository = CrawlDeadLetterRepository(session)

    def enqueue(self, spec: DeadLetterSpec) -> CrawlDeadLetter:
        """Create a letter, ignoring it immediately when non-retryable."""
        existing = self.repository.find_incident(
            crawler=spec.crawler,
            target_type=spec.target_type,
            target_id=spec.target_id,
            original_run_id=spec.original_run_id,
        )
        if existing is not None:
            return existing

        decision = decide(spec.error_code, retry_count=0)
        letter = self.repository.create_dead_letter(spec)
        if not decision.retryable:
            self.repository.set_next_retry_at(letter, None)
            self.repository.mark_ignored(letter)
        return letter

    def prepare_retry(self, dlq_id: str) -> tuple[CrawlDeadLetter, str]:
        """Begin a retry: validate, increment, and pre-link a replay run id."""
        letter = self.repository.get_by_dlq_id(dlq_id)
        if letter is None:
            raise DlqNotFoundError(dlq_id)
        ensure_transition(letter.status, DlqStatus.RETRYING)
        self.repository.mark_retrying(letter)
        self.repository.increment_retry(letter)
        replay_run_id = uuid4().hex
        self.repository.link_replay_run(letter, replay_run_id)
        return letter, replay_run_id

    def finalize_retry(self, dlq_id: str, outcome: ReplayOutcomeLike) -> DlqRetryResult:
        """Apply the replay outcome to the letter lifecycle."""
        letter = self.repository.get_by_dlq_id(dlq_id)
        if letter is None:
            raise DlqNotFoundError(dlq_id)

        status = next_status_after_retry(
            retry_count=letter.retry_count,
            max_retries=letter.max_retries,
            success=outcome.success,
        )
        if status is DlqStatus.RESOLVED:
            ensure_transition(letter.status, DlqStatus.RESOLVED)
            self.repository.mark_resolved(letter, replay_run_id=outcome.replay_run_id)
        elif status is DlqStatus.EXHAUSTED:
            ensure_transition(letter.status, DlqStatus.EXHAUSTED)
            self.repository.mark_exhausted(letter, error_message=outcome.error_message)
        else:
            self._schedule_next_attempt(letter, outcome)
            status = DlqStatus(letter.status)

        return DlqRetryResult(
            dlq_id=letter.dlq_id,
            replay_run_id=outcome.replay_run_id,
            status=status,
            success=outcome.success,
        )

    def _schedule_next_attempt(self, letter: CrawlDeadLetter, outcome: ReplayOutcomeLike) -> None:
        effective_error_code = outcome.error_code or letter.error_code
        decision = decide(effective_error_code, retry_count=letter.retry_count)
        if not decision.retryable:
            ensure_transition(letter.status, DlqStatus.EXHAUSTED)
            self.repository.mark_exhausted(letter, error_message=outcome.error_message)
            return
        ensure_transition(letter.status, DlqStatus.PENDING)
        delay = decision.delay_seconds or 0
        self.repository.mark_pending(letter, next_retry_at=_utcnow() + timedelta(seconds=delay))

    def mark_ignored(self, dlq_id: str, *, reason: str | None = None) -> CrawlDeadLetter:
        """Dismiss a letter from the retry queue."""
        letter = self.repository.get_by_dlq_id(dlq_id)
        if letter is None:
            raise DlqNotFoundError(dlq_id)
        ensure_transition(letter.status, DlqStatus.IGNORED)
        return self.repository.mark_ignored(letter, reason=reason)

    def requeue(self, dlq_id: str) -> CrawlDeadLetter:
        """Explicitly return an ignored/exhausted letter to ``pending``."""
        letter = self.repository.get_by_dlq_id(dlq_id)
        if letter is None:
            raise DlqNotFoundError(dlq_id)
        return self.repository.mark_pending(letter, next_retry_at=_utcnow())


def enqueue_failure(
    spec: DeadLetterSpec,
    *,
    session: Session | None = None,
) -> CrawlDeadLetter:
    """Enqueue a failure using a self-managed session when none is supplied."""
    owns_session = session is None
    active = session if session is not None else SessionLocal()
    try:
        letter = CrawlDeadLetterService(active).enqueue(spec)
        if owns_session:
            active.commit()
        else:
            active.flush()
    except Exception:
        if owns_session:
            active.rollback()
        raise
    else:
        return letter
    finally:
        if owns_session:
            active.close()


def retry_dead_letter(
    dlq_id: str,
    dispatcher: ReplayDispatcherProtocol,
    *,
    session_factory: Callable[[], Session] | None = None,
) -> DlqRetryResult:
    """Retry one dead letter through the replay dispatcher, twice-transactionally."""
    factory: Callable[[], Session] = session_factory or SessionLocal

    with factory() as session:
        letter, replay_run_id = CrawlDeadLetterService(session).prepare_retry(dlq_id)
        session.commit()
        detached = letter

    try:
        outcome = dispatcher.replay(detached, replay_run_id=replay_run_id)
    except Exception as exc:
        logger.exception("Replay dispatcher raised for dlq_id=%s", dlq_id)
        outcome = _FailedOutcome(
            replay_run_id=replay_run_id,
            error_message=str(exc),
            error_code=getattr(exc, "error_code", None),
            failure_stage=getattr(exc, "failure_stage", None),
        )

    with factory() as session:
        result = CrawlDeadLetterService(session).finalize_retry(dlq_id, outcome)
        session.commit()
        return result


@dataclass(frozen=True)
class _FailedOutcome:
    """Fallback outcome when the dispatcher itself raises."""

    replay_run_id: str
    error_message: str
    success: bool = False
    status: str = "failed"
    error_code: str | None = None
    failure_stage: str | None = None


__all__ = [
    "MAX_RETRIES",
    "CrawlDeadLetterService",
    "DlqNotFoundError",
    "DlqRetryResult",
    "ReplayDispatcherProtocol",
    "ReplayOutcomeLike",
    "enqueue_failure",
    "retry_dead_letter",
]
