"""Tests for dead letter operational statistics and metrics projection."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus
from src.repositories.crawl_dead_letter_repository import CrawlDeadLetterRepository, DeadLetterSpec
from src.services.crawl_dead_letter_stats import collect_dlq_stats, publish_dlq_state_metrics
from src.utils.metrics import (
    KBO_DLQ_DUE_LETTERS,
    KBO_DLQ_FAILURES_TOTAL,
    KBO_DLQ_LETTERS,
    KBO_DLQ_OLDEST_PENDING_AGE_SECONDS,
    KBO_DLQ_RECOVERY_ACTIONS_TOTAL,
    KBO_DLQ_RETRY_ATTEMPTS_TOTAL,
    KBO_DLQ_RETRY_OUTCOMES_TOTAL,
    KBO_DLQ_STALE_RETRYING_LETTERS,
    record_dlq_enqueued,
    record_dlq_recovery_action,
    record_dlq_retry_attempt,
    record_dlq_retry_outcome,
)


@pytest.fixture
def session_factory() -> sessionmaker:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlDeadLetter.__table__.create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def _spec(dlq_id: str, *, crawler: str = "awards", target_id: str | None = None) -> DeadLetterSpec:
    return DeadLetterSpec(
        original_run_id="run-a",
        crawler=crawler,
        target_type="award_history",
        target_id=target_id or dlq_id,
        failure_stage="fetch",
        error_code="FETCH_TIMEOUT",
        dlq_id=dlq_id,
    )


def _seed(
    session_factory: sessionmaker,
    dlq_id: str,
    *,
    status: str = DlqStatus.PENDING.value,
    crawler: str = "awards",
    next_retry_at: datetime | None = None,
    updated_at: datetime | None = None,
    created_at: datetime | None = None,
) -> None:
    with session_factory() as session:
        repo = CrawlDeadLetterRepository(session)
        letter = repo.create_dead_letter(_spec(dlq_id, crawler=crawler))
        if status == DlqStatus.RETRYING.value:
            repo.mark_retrying(letter)
        elif status == DlqStatus.RESOLVED.value:
            repo.mark_resolved(letter, replay_run_id="run-b")
        elif status == DlqStatus.EXHAUSTED.value:
            repo.mark_exhausted(letter)
        elif status == DlqStatus.IGNORED.value:
            repo.mark_ignored(letter)
        elif next_retry_at is not None:
            repo.set_next_retry_at(letter, next_retry_at)
        values: dict[str, datetime] = {}
        if next_retry_at is not None:
            values["next_retry_at"] = next_retry_at
        if updated_at is not None:
            values["updated_at"] = updated_at
        if created_at is not None:
            values["created_at"] = created_at
        if values:
            session.execute(update(CrawlDeadLetter).where(CrawlDeadLetter.dlq_id == dlq_id).values(**values))
        session.commit()


def test_collect_dlq_stats_snapshot(session_factory: sessionmaker) -> None:
    now = datetime(2026, 9, 26, 12, 0, 0)
    cutoff = now - timedelta(minutes=30)
    _seed(session_factory, "pending-old", next_retry_at=now - timedelta(minutes=5), created_at=now - timedelta(hours=3))
    _seed(
        session_factory, "pending-future", next_retry_at=now + timedelta(hours=1), created_at=now - timedelta(hours=1)
    )
    _seed(session_factory, "retrying-stale", status=DlqStatus.RETRYING.value, updated_at=cutoff - timedelta(minutes=5))
    _seed(session_factory, "retrying-fresh", status=DlqStatus.RETRYING.value, updated_at=now)
    _seed(session_factory, "resolved", status=DlqStatus.RESOLVED.value, crawler="schedule")
    _seed(session_factory, "exhausted", status=DlqStatus.EXHAUSTED.value)
    _seed(session_factory, "ignored", status=DlqStatus.IGNORED.value)

    stats = collect_dlq_stats(now=now, stale_before=cutoff, session_factory=session_factory)

    assert stats.pending == 2
    assert stats.due == 1
    assert stats.retrying == 2
    assert stats.stale_retrying == 1
    assert stats.resolved == 1
    assert stats.exhausted == 1
    assert stats.ignored == 1
    assert stats.oldest_pending_at == now - timedelta(hours=3)
    assert stats.oldest_pending_age_seconds == pytest.approx(3 * 3600)
    assert stats.by_status_crawler[("pending", "awards")] == 2
    assert stats.by_status_crawler[("resolved", "schedule")] == 1
    assert "pending:awards" in stats.to_dict()["by_status_crawler"]


def test_publish_dlq_state_metrics_sets_gauges(session_factory: sessionmaker) -> None:
    now = datetime(2026, 9, 26, 12, 0, 0)
    _seed(session_factory, "a", next_retry_at=now - timedelta(minutes=1), created_at=now - timedelta(hours=2))
    _seed(
        session_factory, "b", status=DlqStatus.RETRYING.value, crawler="schedule", updated_at=now - timedelta(hours=1)
    )

    publish_dlq_state_metrics(now=now, stale_before=now - timedelta(minutes=30), session_factory=session_factory)

    assert KBO_DLQ_LETTERS.labels(status="pending", crawler="awards")._value.get() == 1
    assert KBO_DLQ_LETTERS.labels(status="retrying", crawler="schedule")._value.get() == 1
    assert KBO_DLQ_DUE_LETTERS._value.get() == 1
    assert KBO_DLQ_STALE_RETRYING_LETTERS._value.get() == 1
    assert KBO_DLQ_OLDEST_PENDING_AGE_SECONDS._value.get() == pytest.approx(2 * 3600)


def test_event_counters_increment() -> None:
    failures_before = KBO_DLQ_FAILURES_TOTAL.labels(crawler="awards", error_code="FETCH_TIMEOUT")._value.get()
    record_dlq_enqueued("awards", "FETCH_TIMEOUT")
    record_dlq_enqueued("awards", "FETCH_TIMEOUT")
    assert (
        KBO_DLQ_FAILURES_TOTAL.labels(crawler="awards", error_code="FETCH_TIMEOUT")._value.get() == failures_before + 2
    )

    attempts_before = KBO_DLQ_RETRY_ATTEMPTS_TOTAL._value.get()
    record_dlq_retry_attempt()
    assert KBO_DLQ_RETRY_ATTEMPTS_TOTAL._value.get() == attempts_before + 1

    outcomes_before = KBO_DLQ_RETRY_OUTCOMES_TOTAL.labels(crawler="awards", outcome="resolved")._value.get()
    record_dlq_retry_outcome("awards", "resolved")
    assert KBO_DLQ_RETRY_OUTCOMES_TOTAL.labels(crawler="awards", outcome="resolved")._value.get() == outcomes_before + 1

    actions_before = KBO_DLQ_RECOVERY_ACTIONS_TOTAL.labels(action="resolved")._value.get()
    record_dlq_recovery_action("resolved")
    assert KBO_DLQ_RECOVERY_ACTIONS_TOTAL.labels(action="resolved")._value.get() == actions_before + 1
