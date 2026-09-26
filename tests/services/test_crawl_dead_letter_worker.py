"""Tests for the dead letter retry worker."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus
from src.models.crawl_execution import CrawlExecutionRun
from src.repositories.crawl_dead_letter_repository import CrawlDeadLetterRepository, DeadLetterSpec
from src.services.crawl_dead_letter_service import CrawlDeadLetterService, DlqNotFoundError
from src.services.crawl_dead_letter_state import InvalidDlqTransitionError
from src.services.crawl_dead_letter_worker import retry_due_dead_letters


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


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _seed_pending(session_factory: sessionmaker, *, error_code: str = "SOURCE_PARTIAL") -> str:
    with session_factory() as session:
        letter = CrawlDeadLetterService(session).enqueue(
            DeadLetterSpec(
                original_run_id="run-a",
                crawler="awards",
                target_type="award_history",
                target_id="kbo_awards_yagoonara",
                failure_stage="fetch",
                error_code=error_code,
            ),
        )
        session.commit()
        return letter.dlq_id


def _stored(session_factory: sessionmaker, dlq_id: str) -> CrawlDeadLetter:
    with session_factory() as session:
        return session.query(CrawlDeadLetter).filter_by(dlq_id=dlq_id).one()


@dataclass
class _Outcome:
    success: bool
    replay_run_id: str
    status: str = "success"
    error_message: str | None = None
    error_code: str | None = None
    failure_stage: str | None = None


@dataclass
class _FakeDispatcher:
    success: bool = True
    error_code: str | None = None
    raises: Exception | None = None
    calls: list[str] = field(default_factory=list)

    def replay(self, dead_letter: CrawlDeadLetter, *, replay_run_id: str) -> _Outcome:
        self.calls.append(dead_letter.dlq_id)
        if self.raises is not None:
            raise self.raises
        return _Outcome(
            success=self.success,
            replay_run_id=replay_run_id,
            status="success" if self.success else "failed",
            error_message=None if self.success else "replay failed",
            error_code=self.error_code,
        )


def test_no_due_letters_is_noop(session_factory: sessionmaker) -> None:
    summary = retry_due_dead_letters(session_factory=session_factory, dispatcher=_FakeDispatcher())
    assert summary.attempted == 0
    assert summary.to_dict() == {
        "attempted": 0,
        "resolved": 0,
        "pending": 0,
        "exhausted": 0,
        "conflicted": 0,
        "errored": 0,
    }


def test_successful_retry_resolves(session_factory: sessionmaker) -> None:
    dlq_id = _seed_pending(session_factory)
    dispatcher = _FakeDispatcher(success=True)

    summary = retry_due_dead_letters(session_factory=session_factory, dispatcher=dispatcher)

    assert summary.attempted == 1
    assert summary.resolved == 1
    assert dispatcher.calls == [dlq_id]
    assert _stored(session_factory, dlq_id).status == DlqStatus.RESOLVED.value


def test_future_next_retry_is_not_due(session_factory: sessionmaker) -> None:
    dlq_id = _seed_pending(session_factory)
    with session_factory() as session:
        repo = CrawlDeadLetterRepository(session)
        repo.set_next_retry_at(repo.get_by_dlq_id(dlq_id), _utcnow() + timedelta(hours=1))
        session.commit()

    summary = retry_due_dead_letters(session_factory=session_factory, dispatcher=_FakeDispatcher())

    assert summary.attempted == 0
    assert _stored(session_factory, dlq_id).status == DlqStatus.PENDING.value


def test_retryable_replay_failure_returns_to_pending(session_factory: sessionmaker) -> None:
    dlq_id = _seed_pending(session_factory)
    dispatcher = _FakeDispatcher(success=False, error_code="FETCH_TIMEOUT")

    summary = retry_due_dead_letters(session_factory=session_factory, dispatcher=dispatcher)

    assert summary.pending == 1
    stored = _stored(session_factory, dlq_id)
    assert stored.status == DlqStatus.PENDING.value
    assert stored.retry_count == 1


def test_non_retryable_replay_failure_exhausts(session_factory: sessionmaker) -> None:
    dlq_id = _seed_pending(session_factory)
    dispatcher = _FakeDispatcher(success=False, error_code="PERSIST_CONSTRAINT")

    summary = retry_due_dead_letters(session_factory=session_factory, dispatcher=dispatcher)

    assert summary.exhausted == 1
    assert _stored(session_factory, dlq_id).status == DlqStatus.EXHAUSTED.value


def test_non_retryable_letter_is_never_attempted(session_factory: sessionmaker) -> None:
    dlq_id = _seed_pending(session_factory, error_code="VALIDATION_SCHEMA")
    dispatcher = _FakeDispatcher()

    summary = retry_due_dead_letters(session_factory=session_factory, dispatcher=dispatcher)

    assert summary.attempted == 0
    assert dispatcher.calls == []
    assert _stored(session_factory, dlq_id).status == DlqStatus.IGNORED.value


def test_worker_isolates_and_counts_errors(
    session_factory: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dlq_id = _seed_pending(session_factory)

    def _boom(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("worker blew up")

    monkeypatch.setattr("src.services.crawl_dead_letter_worker.retry_dead_letter", _boom)

    summary = retry_due_dead_letters(session_factory=session_factory, dispatcher=_FakeDispatcher())

    assert summary.attempted == 1
    assert summary.errored == 1
    assert summary.conflicted == 0
    # The letter is untouched and remains eligible for a later pass.
    assert _stored(session_factory, dlq_id).status == DlqStatus.PENDING.value


@pytest.mark.parametrize(
    "error",
    [
        InvalidDlqTransitionError("retrying", "retrying"),
        DlqNotFoundError("gone"),
    ],
)
def test_already_claimed_letter_counts_as_conflict(
    session_factory: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
) -> None:
    dlq_id = _seed_pending(session_factory)

    def _conflict(*_args: object, **_kwargs: object) -> object:
        raise error

    monkeypatch.setattr("src.services.crawl_dead_letter_worker.retry_dead_letter", _conflict)

    summary = retry_due_dead_letters(session_factory=session_factory, dispatcher=_FakeDispatcher())

    assert summary.attempted == 1
    assert summary.conflicted == 1
    assert summary.errored == 0
    assert _stored(session_factory, dlq_id).status == DlqStatus.PENDING.value
