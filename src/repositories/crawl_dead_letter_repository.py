"""Repository for the crawler dead letter queue.

Like every repository in this project it is caller-managed: it never commits,
rolls back, or opens a session. Retry orchestration that must be atomic across a
dead letter and a replayed run composes these calls inside one transaction.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import func, select

from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _utcnow() -> datetime:
    """Return a naive UTC timestamp matching the rest of the schema."""
    return datetime.now(UTC).replace(tzinfo=None)


@dataclass(frozen=True)
class DeadLetterSpec:
    """Immutable description of a failure to enqueue."""

    original_run_id: str
    crawler: str
    target_type: str
    failure_stage: str
    error_code: str
    target_id: str | None = None
    season: int | None = None
    game_id: str | None = None
    error_message: str | None = None
    error_type: str | None = None
    source_url: str | None = None
    payload_ref: str | None = None
    snapshot_id: int | None = None
    evidence_id: int | None = None
    max_retries: int = 5
    dlq_id: str | None = None


class CrawlDeadLetterRepository:
    """Persist dead letter lifecycle changes without owning the transaction."""

    def __init__(self, session: Session) -> None:
        """Initialize the repository with a caller-managed session."""
        self.session = session

    def create_dead_letter(self, spec: DeadLetterSpec) -> CrawlDeadLetter:
        """Insert a new dead letter in the ``pending`` state and flush it."""
        dead_letter = CrawlDeadLetter(
            dlq_id=spec.dlq_id or uuid4().hex,
            original_run_id=spec.original_run_id,
            crawler=spec.crawler,
            target_type=spec.target_type,
            target_id=spec.target_id,
            season=spec.season,
            game_id=spec.game_id,
            failure_stage=spec.failure_stage,
            error_code=spec.error_code,
            error_message=spec.error_message,
            error_type=spec.error_type,
            source_url=spec.source_url,
            payload_ref=spec.payload_ref,
            snapshot_id=spec.snapshot_id,
            evidence_id=spec.evidence_id,
            retry_count=0,
            max_retries=spec.max_retries,
            next_retry_at=_utcnow(),
            status=DlqStatus.PENDING.value,
        )
        self.session.add(dead_letter)
        self.session.flush()
        return dead_letter

    def get_by_dlq_id(self, dlq_id: str) -> CrawlDeadLetter | None:
        """Return the dead letter with the given ``dlq_id`` if it exists."""
        return self.session.execute(
            select(CrawlDeadLetter).where(CrawlDeadLetter.dlq_id == dlq_id),
        ).scalar_one_or_none()

    def find_incident(
        self,
        *,
        crawler: str,
        target_type: str,
        target_id: str | None,
        original_run_id: str,
    ) -> CrawlDeadLetter | None:
        """Return an existing letter for the same source failure, if any."""
        stmt = select(CrawlDeadLetter).where(
            CrawlDeadLetter.crawler == crawler,
            CrawlDeadLetter.target_type == target_type,
            CrawlDeadLetter.original_run_id == original_run_id,
        )
        stmt = (
            stmt.where(CrawlDeadLetter.target_id.is_(None))
            if target_id is None
            else stmt.where(CrawlDeadLetter.target_id == target_id)
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_pending(self, *, limit: int = 100) -> list[CrawlDeadLetter]:
        """Return pending letters ordered by their next retry time."""
        stmt = (
            select(CrawlDeadLetter)
            .where(CrawlDeadLetter.status == DlqStatus.PENDING.value)
            .order_by(CrawlDeadLetter.next_retry_at.asc(), CrawlDeadLetter.id.asc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_retryable(
        self,
        *,
        now: datetime | None = None,
        limit: int = 100,
    ) -> list[CrawlDeadLetter]:
        """Return pending letters whose ``next_retry_at`` has elapsed."""
        reference = now or _utcnow()
        stmt = (
            select(CrawlDeadLetter)
            .where(
                CrawlDeadLetter.status == DlqStatus.PENDING.value,
                (CrawlDeadLetter.next_retry_at.is_(None)) | (CrawlDeadLetter.next_retry_at <= reference),
            )
            .order_by(CrawlDeadLetter.next_retry_at.asc(), CrawlDeadLetter.id.asc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_stale_retrying(
        self,
        *,
        stale_before: datetime,
        limit: int = 100,
    ) -> list[CrawlDeadLetter]:
        """Return letters stuck in ``retrying`` whose update predates the cutoff."""
        stmt = (
            select(CrawlDeadLetter)
            .where(
                CrawlDeadLetter.status == DlqStatus.RETRYING.value,
                CrawlDeadLetter.updated_at <= stale_before,
            )
            .order_by(CrawlDeadLetter.updated_at.asc(), CrawlDeadLetter.id.asc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def mark_retrying(
        self,
        dead_letter: CrawlDeadLetter,
        *,
        next_retry_at: datetime | None = None,
    ) -> CrawlDeadLetter:
        """Move a letter into the transient ``retrying`` state."""
        dead_letter.status = DlqStatus.RETRYING.value
        if next_retry_at is not None:
            dead_letter.next_retry_at = next_retry_at
        self.session.flush()
        return dead_letter

    def mark_pending(
        self,
        dead_letter: CrawlDeadLetter,
        *,
        next_retry_at: datetime | None = None,
    ) -> CrawlDeadLetter:
        """Return a letter to the ``pending`` state for a future retry."""
        dead_letter.status = DlqStatus.PENDING.value
        if next_retry_at is not None:
            dead_letter.next_retry_at = next_retry_at
        self.session.flush()
        return dead_letter

    def mark_resolved(
        self,
        dead_letter: CrawlDeadLetter,
        *,
        replay_run_id: str | None = None,
        resolved_at: datetime | None = None,
    ) -> CrawlDeadLetter:
        """Mark a letter resolved after a successful replay."""
        dead_letter.status = DlqStatus.RESOLVED.value
        if replay_run_id is not None:
            dead_letter.replay_run_id = replay_run_id
        dead_letter.resolved_at = resolved_at or _utcnow()
        self.session.flush()
        return dead_letter

    def mark_exhausted(
        self,
        dead_letter: CrawlDeadLetter,
        *,
        error_message: str | None = None,
    ) -> CrawlDeadLetter:
        """Mark a letter exhausted after all retries failed."""
        dead_letter.status = DlqStatus.EXHAUSTED.value
        if error_message is not None:
            dead_letter.error_message = error_message
        self.session.flush()
        return dead_letter

    def mark_ignored(
        self,
        dead_letter: CrawlDeadLetter,
        *,
        reason: str | None = None,
    ) -> CrawlDeadLetter:
        """Mark a letter ignored (non-retryable or operator dismissal)."""
        dead_letter.status = DlqStatus.IGNORED.value
        if reason is not None:
            dead_letter.error_message = reason
        self.session.flush()
        return dead_letter

    def increment_retry(self, dead_letter: CrawlDeadLetter) -> CrawlDeadLetter:
        """Increment the replay attempt counter."""
        dead_letter.retry_count += 1
        self.session.flush()
        return dead_letter

    def link_replay_run(self, dead_letter: CrawlDeadLetter, replay_run_id: str) -> CrawlDeadLetter:
        """Record the ``run_id`` allocated for the replay of this letter."""
        dead_letter.replay_run_id = replay_run_id
        self.session.flush()
        return dead_letter

    def set_next_retry_at(self, dead_letter: CrawlDeadLetter, next_retry_at: datetime | None) -> CrawlDeadLetter:
        """Set when the letter becomes eligible for its next retry."""
        dead_letter.next_retry_at = next_retry_at
        self.session.flush()
        return dead_letter

    def count_by_status_crawler(self) -> list[tuple[str, str, int]]:
        """Return ``(status, crawler, count)`` rows for every non-empty group."""
        stmt = select(CrawlDeadLetter.status, CrawlDeadLetter.crawler, func.count()).group_by(
            CrawlDeadLetter.status,
            CrawlDeadLetter.crawler,
        )
        return [(row[0], row[1], int(row[2])) for row in self.session.execute(stmt).all()]

    def count_due(self, *, now: datetime) -> int:
        """Return the number of pending letters that are due for retry."""
        stmt = (
            select(func.count())
            .select_from(CrawlDeadLetter)
            .where(
                CrawlDeadLetter.status == DlqStatus.PENDING.value,
                (CrawlDeadLetter.next_retry_at.is_(None)) | (CrawlDeadLetter.next_retry_at <= now),
            )
        )
        return int(self.session.execute(stmt).scalar_one())

    def count_stale_retrying(self, *, stale_before: datetime) -> int:
        """Return the number of letters stuck in ``retrying`` past the cutoff."""
        stmt = (
            select(func.count())
            .select_from(CrawlDeadLetter)
            .where(
                CrawlDeadLetter.status == DlqStatus.RETRYING.value,
                CrawlDeadLetter.updated_at <= stale_before,
            )
        )
        return int(self.session.execute(stmt).scalar_one())

    def oldest_pending_created_at(self) -> datetime | None:
        """Return the creation time of the oldest pending letter, if any."""
        stmt = select(func.min(CrawlDeadLetter.created_at)).where(
            CrawlDeadLetter.status == DlqStatus.PENDING.value,
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def list_recent(
        self,
        *,
        crawler: str | None = None,
        status: str | None = None,
        error_code: str | None = None,
        limit: int = 50,
    ) -> list[CrawlDeadLetter]:
        """Return recent letters, optionally filtered by crawler, status, error code."""
        stmt = select(CrawlDeadLetter)
        if crawler is not None:
            stmt = stmt.where(CrawlDeadLetter.crawler == crawler)
        if status is not None:
            stmt = stmt.where(CrawlDeadLetter.status == status)
        if error_code is not None:
            stmt = stmt.where(CrawlDeadLetter.error_code == error_code)
        stmt = stmt.order_by(CrawlDeadLetter.id.desc()).limit(limit)
        return list(self.session.execute(stmt).scalars().all())
