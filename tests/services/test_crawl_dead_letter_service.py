"""Tests for the crawler dead letter service and retry orchestration."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus
from src.repositories.crawl_dead_letter_repository import CrawlDeadLetterRepository, DeadLetterSpec
from src.services.crawl_dead_letter_service import (
    CrawlDeadLetterService,
    DlqNotFoundError,
    enqueue_failure,
    retry_dead_letter,
)
from src.services.crawl_dead_letter_state import InvalidDlqTransitionError


@pytest.fixture
def session_factory() -> sessionmaker:
    engine = create_engine("sqlite:///:memory:")
    CrawlDeadLetter.__table__.create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture
def session(session_factory: sessionmaker) -> Iterator[Session]:
    active = session_factory()
    try:
        yield active
    finally:
        active.close()


def _spec(**overrides: object) -> DeadLetterSpec:
    data: dict[str, object] = {
        "original_run_id": "run-a",
        "crawler": "awards",
        "target_type": "award_history",
        "target_id": "kbo_awards_yagoonara",
        "failure_stage": "fetch",
        "error_code": "FETCH_TIMEOUT",
    }
    data.update(overrides)
    return DeadLetterSpec(**data)  # type: ignore[arg-type]


@dataclass
class _Outcome:
    success: bool
    replay_run_id: str
    status: str = "success"
    error_message: str | None = None


@dataclass
class _FakeDispatcher:
    success: bool = True
    error_message: str | None = None
    raises: Exception | None = None
    calls: list[tuple[str, str]] = field(default_factory=list)

    def replay(self, dead_letter: CrawlDeadLetter, *, replay_run_id: str) -> _Outcome:
        self.calls.append((dead_letter.dlq_id, replay_run_id))
        if self.raises is not None:
            raise self.raises
        return _Outcome(success=self.success, replay_run_id=replay_run_id, error_message=self.error_message)


class TestEnqueue:
    def test_retryable_failure_is_pending(self, session: Session) -> None:
        service = CrawlDeadLetterService(session)
        letter = service.enqueue(_spec())
        assert letter.status == DlqStatus.PENDING.value
        assert letter.retry_count == 0
        assert letter.next_retry_at is not None

    def test_non_retryable_failure_is_ignored(self, session: Session) -> None:
        service = CrawlDeadLetterService(session)
        letter = service.enqueue(_spec(error_code="VALIDATION_SCHEMA"))
        assert letter.status == DlqStatus.IGNORED.value
        assert letter.next_retry_at is None

    def test_same_incident_is_absorbed(self, session: Session) -> None:
        service = CrawlDeadLetterService(session)
        first = service.enqueue(_spec())
        second = service.enqueue(_spec(error_code="FETCH_HTTP_ERROR"))
        assert first.id == second.id
        assert session.query(CrawlDeadLetter).count() == 1

    def test_enqueue_failure_uses_self_managed_session(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("src.services.crawl_dead_letter_service.SessionLocal", session_factory)
        letter = enqueue_failure(_spec())
        assert letter.id is not None
        with session_factory() as check:
            assert check.query(CrawlDeadLetter).count() == 1


class TestPrepareRetry:
    def test_prepare_increments_and_links(self, session: Session) -> None:
        service = CrawlDeadLetterService(session)
        letter = service.enqueue(_spec())
        prepared, replay_run_id = service.prepare_retry(letter.dlq_id)
        assert prepared.status == DlqStatus.RETRYING.value
        assert prepared.retry_count == 1
        assert prepared.replay_run_id == replay_run_id
        assert replay_run_id

    def test_prepare_missing_raises(self, session: Session) -> None:
        with pytest.raises(DlqNotFoundError):
            CrawlDeadLetterService(session).prepare_retry("nope")

    def test_prepare_from_resolved_rejected(self, session: Session) -> None:
        service = CrawlDeadLetterService(session)
        letter = service.enqueue(_spec())
        letter.status = DlqStatus.RESOLVED.value
        session.flush()
        with pytest.raises(InvalidDlqTransitionError):
            service.prepare_retry(letter.dlq_id)


class TestFinalizeRetry:
    def _retrying(self, session: Session, *, retry_count: int = 1) -> CrawlDeadLetter:
        repo = CrawlDeadLetterRepository(session)
        letter = repo.create_dead_letter(_spec())
        repo.mark_retrying(letter)
        for _ in range(retry_count):
            repo.increment_retry(letter)
        repo.link_replay_run(letter, "run-b")
        return letter

    def test_success_resolves(self, session: Session) -> None:
        service = CrawlDeadLetterService(session)
        letter = self._retrying(session)
        result = service.finalize_retry(letter.dlq_id, _Outcome(success=True, replay_run_id="run-b"))
        assert result.status is DlqStatus.RESOLVED
        assert letter.status == DlqStatus.RESOLVED.value
        assert letter.replay_run_id == "run-b"
        assert letter.resolved_at is not None

    def test_failure_schedules_backoff(self, session: Session) -> None:
        service = CrawlDeadLetterService(session)
        letter = self._retrying(session, retry_count=1)
        before = datetime.now(UTC).replace(tzinfo=None)
        service.finalize_retry(letter.dlq_id, _Outcome(success=False, replay_run_id="run-b"))
        assert letter.status == DlqStatus.PENDING.value
        assert letter.next_retry_at is not None
        delta = (letter.next_retry_at - before).total_seconds()
        assert 55 <= delta <= 65  # RETRY_SCHEDULE[0] == 60

    def test_failure_at_max_retries_exhausts(self, session: Session) -> None:
        service = CrawlDeadLetterService(session)
        letter = self._retrying(session, retry_count=5)
        service.finalize_retry(
            letter.dlq_id,
            _Outcome(success=False, replay_run_id="run-b", error_message="still failing"),
        )
        assert letter.status == DlqStatus.EXHAUSTED.value
        assert letter.error_message == "still failing"


class TestRetryDeadLetter:
    def test_success_flow(self, session_factory: sessionmaker) -> None:
        with session_factory() as setup:
            letter = CrawlDeadLetterService(setup).enqueue(_spec())
            setup.commit()
            dlq_id = letter.dlq_id

        dispatcher = _FakeDispatcher(success=True)
        result = retry_dead_letter(dlq_id, dispatcher, session_factory=session_factory)

        assert result.status is DlqStatus.RESOLVED
        assert dispatcher.calls == [(dlq_id, result.replay_run_id)]
        with session_factory() as check:
            stored = check.query(CrawlDeadLetter).one()
        assert stored.status == DlqStatus.RESOLVED.value
        assert stored.replay_run_id == result.replay_run_id

    def test_dispatcher_exception_becomes_pending(self, session_factory: sessionmaker) -> None:
        with session_factory() as setup:
            letter = CrawlDeadLetterService(setup).enqueue(_spec())
            setup.commit()
            dlq_id = letter.dlq_id

        dispatcher = _FakeDispatcher(raises=RuntimeError("network down"))
        result = retry_dead_letter(dlq_id, dispatcher, session_factory=session_factory)

        assert result.success is False
        with session_factory() as check:
            stored = check.query(CrawlDeadLetter).one()
        assert stored.status == DlqStatus.PENDING.value
        assert stored.retry_count == 1

    def test_non_retryable_is_never_retried(self, session_factory: sessionmaker) -> None:
        with session_factory() as setup:
            letter = CrawlDeadLetterService(setup).enqueue(_spec(error_code="PERSIST_CONSTRAINT"))
            setup.commit()
            dlq_id = letter.dlq_id
        with session_factory() as check:
            assert check.query(CrawlDeadLetter).one().status == DlqStatus.IGNORED.value
        dispatcher = _FakeDispatcher()
        with pytest.raises(InvalidDlqTransitionError):
            retry_dead_letter(dlq_id, dispatcher, session_factory=session_factory)


class TestManualActions:
    def test_mark_ignored(self, session: Session) -> None:
        service = CrawlDeadLetterService(session)
        letter = service.enqueue(_spec())
        service.mark_ignored(letter.dlq_id, reason="operator")
        assert letter.status == DlqStatus.IGNORED.value

    def test_requeue_returns_to_pending(self, session: Session) -> None:
        service = CrawlDeadLetterService(session)
        letter = service.enqueue(_spec(error_code="VALIDATION_SCHEMA"))
        assert letter.status == DlqStatus.IGNORED.value
        service.requeue(letter.dlq_id)
        assert letter.status == DlqStatus.PENDING.value
        assert letter.next_retry_at is not None
