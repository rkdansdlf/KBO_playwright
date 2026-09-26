"""Tests for stale-retrying dead letter recovery."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus
from src.models.crawl_execution import CrawlExecutionRun
from src.repositories.crawl_dead_letter_repository import CrawlDeadLetterRepository, DeadLetterSpec
from src.repositories.crawl_execution_repository import CrawlExecutionRepository, CrawlRunSpec
from src.services.crawl_dead_letter_recovery import (
    ACTION_EXHAUSTED,
    ACTION_FINALIZED_INTERRUPTED,
    ACTION_INVARIANT_MISSING,
    ACTION_ORPHAN_RESCHEDULED,
    ACTION_RESCHEDULED,
    ACTION_RESOLVED,
    ACTION_SKIPPED_ACTIVE,
    recover_stuck_retrying,
)


@pytest.fixture
def session_factory() -> sessionmaker:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    CrawlDeadLetter.__table__.create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def _letter_spec(**overrides: object) -> DeadLetterSpec:
    data: dict[str, object] = {
        "original_run_id": "run-a",
        "crawler": "awards",
        "target_type": "award_history",
        "target_id": "kbo_awards_yagoonara",
        "failure_stage": "fetch",
        "error_code": "SOURCE_PARTIAL",
    }
    data.update(overrides)
    return DeadLetterSpec(**data)  # type: ignore[arg-type]


def _seed_stuck_letter(
    session_factory: sessionmaker,
    *,
    replay_run_id: str | None = "run-b",
    retry_count: int = 1,
    error_code: str = "SOURCE_PARTIAL",
) -> str:
    with session_factory() as session:
        repo = CrawlDeadLetterRepository(session)
        letter = repo.create_dead_letter(_letter_spec(error_code=error_code))
        repo.mark_retrying(letter)
        for _ in range(retry_count):
            repo.increment_retry(letter)
        if replay_run_id is not None:
            repo.link_replay_run(letter, replay_run_id)
        session.commit()
        return letter.dlq_id


def _seed_run(
    session_factory: sessionmaker,
    *,
    run_id: str = "run-b",
    status: str = "running",
    error_code: str | None = None,
) -> None:
    with session_factory() as session:
        repo = CrawlExecutionRepository(session)
        run = repo.start_run(CrawlRunSpec(crawler="awards", target_type="award_history", run_id=run_id))
        if status == "success":
            repo.mark_success(run)
        elif status == "partial":
            repo.mark_partial(run)
        elif status == "failed":
            repo.mark_failed(run, error_code=error_code or "FETCH_TIMEOUT", error_message="replay failed")
        session.commit()


def _cutoff() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None) + timedelta(minutes=1)


def _stored(session_factory: sessionmaker, dlq_id: str) -> CrawlDeadLetter:
    with session_factory() as session:
        return session.query(CrawlDeadLetter).filter_by(dlq_id=dlq_id).one()


def test_successful_replay_resolves(session_factory: sessionmaker) -> None:
    dlq_id = _seed_stuck_letter(session_factory)
    _seed_run(session_factory, status="success")

    results = recover_stuck_retrying(stale_before=_cutoff(), session_factory=session_factory)

    assert len(results) == 1
    assert results[0].action == ACTION_RESOLVED
    stored = _stored(session_factory, dlq_id)
    assert stored.status == DlqStatus.RESOLVED.value
    assert stored.replay_run_id == "run-b"
    assert stored.resolved_at is not None


def test_non_retryable_replay_failure_exhausts(session_factory: sessionmaker) -> None:
    dlq_id = _seed_stuck_letter(session_factory)
    _seed_run(session_factory, status="failed", error_code="PERSIST_CONSTRAINT")

    results = recover_stuck_retrying(stale_before=_cutoff(), session_factory=session_factory)

    assert results[0].action == ACTION_EXHAUSTED
    assert _stored(session_factory, dlq_id).status == DlqStatus.EXHAUSTED.value


def test_retryable_replay_failure_reschedules(session_factory: sessionmaker) -> None:
    dlq_id = _seed_stuck_letter(session_factory, retry_count=1)
    _seed_run(session_factory, status="failed", error_code="FETCH_TIMEOUT")

    results = recover_stuck_retrying(stale_before=_cutoff(), session_factory=session_factory)

    assert results[0].action == ACTION_RESCHEDULED
    stored = _stored(session_factory, dlq_id)
    assert stored.status == DlqStatus.PENDING.value
    assert stored.next_retry_at is not None


def test_partial_replay_reschedules(session_factory: sessionmaker) -> None:
    dlq_id = _seed_stuck_letter(session_factory)
    _seed_run(session_factory, status="partial")

    results = recover_stuck_retrying(stale_before=_cutoff(), session_factory=session_factory)

    assert results[0].action == ACTION_RESCHEDULED
    assert _stored(session_factory, dlq_id).status == DlqStatus.PENDING.value


def test_missing_run_is_orphan_rescheduled(session_factory: sessionmaker) -> None:
    dlq_id = _seed_stuck_letter(session_factory)

    results = recover_stuck_retrying(stale_before=_cutoff(), session_factory=session_factory)

    assert results[0].action == ACTION_ORPHAN_RESCHEDULED
    stored = _stored(session_factory, dlq_id)
    assert stored.status == DlqStatus.PENDING.value


def test_null_replay_run_id_is_invariant_missing(session_factory: sessionmaker) -> None:
    dlq_id = _seed_stuck_letter(session_factory, replay_run_id=None)

    results = recover_stuck_retrying(stale_before=_cutoff(), session_factory=session_factory)

    assert results[0].action == ACTION_INVARIANT_MISSING
    assert _stored(session_factory, dlq_id).status == DlqStatus.PENDING.value


def test_running_replay_is_skipped(session_factory: sessionmaker) -> None:
    dlq_id = _seed_stuck_letter(session_factory)
    _seed_run(session_factory, status="running")

    results = recover_stuck_retrying(stale_before=_cutoff(), session_factory=session_factory)

    assert results[0].action == ACTION_SKIPPED_ACTIVE
    assert _stored(session_factory, dlq_id).status == DlqStatus.RETRYING.value


def test_stale_running_replay_is_finalized_interrupted(session_factory: sessionmaker) -> None:
    dlq_id = _seed_stuck_letter(session_factory)
    _seed_run(session_factory, status="running")
    now = datetime.now(UTC).replace(tzinfo=None)
    with session_factory() as session:
        session.execute(update(CrawlExecutionRun).values(started_at=now - timedelta(hours=2)))
        session.commit()

    results = recover_stuck_retrying(
        stale_before=_cutoff(),
        run_stale_before=now - timedelta(minutes=1),
        session_factory=session_factory,
    )

    assert results[0].action == ACTION_FINALIZED_INTERRUPTED
    assert _stored(session_factory, dlq_id).status == DlqStatus.PENDING.value
    with session_factory() as session:
        run = session.query(CrawlExecutionRun).filter_by(run_id="run-b").one()
        assert run.status == "failed"
        assert run.error_code == "REPLAY_INTERRUPTED"


def test_fresh_letter_is_not_selected(session_factory: sessionmaker) -> None:
    scene_cutoff = datetime.now(UTC).replace(tzinfo=None)
    _seed_stuck_letter(session_factory)
    with session_factory() as session:
        session.execute(update(CrawlDeadLetter).values(updated_at=scene_cutoff + timedelta(minutes=10)))
        session.commit()

    results = recover_stuck_retrying(stale_before=scene_cutoff + timedelta(minutes=1), session_factory=session_factory)

    assert results == []
