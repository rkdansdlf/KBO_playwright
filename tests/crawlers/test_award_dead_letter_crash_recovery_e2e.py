"""Phase C Definition of Done: crash-window recovery for the dead letter queue.

Simulates the intermediate database states a crash can leave behind (rather than
killing a process) and asserts deterministic convergence.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.award_crawler import AwardCrawler
from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus
from src.models.crawl_execution import CrawlExecutionRun
from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
from src.repositories.crawl_execution_repository import CrawlExecutionRepository, CrawlRunSpec
from src.services.crawl_dead_letter_recovery import recover_stuck_retrying
from src.services.crawl_dead_letter_service import CrawlDeadLetterService, retry_dead_letter
from src.services.crawl_replay_dispatcher import build_default_dispatcher

TARGET_ID = "kbo_awards_yagoonara"


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


def _wire_sessions(monkeypatch: pytest.MonkeyPatch, session_factory: sessionmaker) -> None:
    monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", session_factory)
    monkeypatch.setattr("src.services.crawl_dead_letter_service.SessionLocal", session_factory)
    monkeypatch.setattr("src.services.crawl_replay_dispatcher.SessionLocal", session_factory)
    monkeypatch.setattr("src.crawlers.award_crawler.SessionLocal", session_factory)


def _seed_pending(session_factory: sessionmaker) -> str:
    with session_factory() as session:
        letter = CrawlDeadLetterService(session).enqueue(
            DeadLetterSpec(
                original_run_id="run-a",
                crawler="awards",
                target_type="award_history",
                target_id=TARGET_ID,
                failure_stage="fetch",
                error_code="SOURCE_PARTIAL",
            ),
        )
        session.commit()
        return letter.dlq_id


def _seed_linked_run(
    session_factory: sessionmaker,
    replay_run_id: str,
    *,
    status: str,
    started_at: datetime | None = None,
) -> None:
    with session_factory() as session:
        repo = CrawlExecutionRepository(session)
        run = repo.start_run(CrawlRunSpec(crawler="awards", target_type="award_history", run_id=replay_run_id))
        if status == "success":
            repo.mark_success(run)
        session.commit()
    if started_at is not None:
        with session_factory() as session:
            session.execute(
                update(CrawlExecutionRun)
                .where(CrawlExecutionRun.run_id == replay_run_id)
                .values(started_at=started_at),
            )
            session.commit()


def _cutoff() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None) + timedelta(minutes=1)


@pytest.mark.asyncio
async def test_crash_after_txn1_recovers_then_replays_to_resolved(
    session_factory: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_sessions(monkeypatch, session_factory)
    dlq_id = _seed_pending(session_factory)

    # Simulate crash right after txn1: retrying + pre-linked run_id, no run row.
    with session_factory() as session:
        _, replay_run_id = CrawlDeadLetterService(session).prepare_retry(dlq_id)
        session.commit()
    assert _stored(session_factory, dlq_id).status == DlqStatus.RETRYING.value

    recovered = recover_stuck_retrying(stale_before=_cutoff(), session_factory=session_factory)
    assert recovered[0].action == "orphan_rescheduled"
    assert _stored(session_factory, dlq_id).status == DlqStatus.PENDING.value

    # Network-free replay: the failed source is re-run and succeeds.
    async def empty_crawl(self: AwardCrawler, types: set[str] | None = None, source_key: str | None = None) -> list:
        return []

    monkeypatch.setattr(AwardCrawler, "crawl", empty_crawl)

    result = retry_dead_letter(dlq_id, build_default_dispatcher(), session_factory=session_factory)

    assert result.success is True
    with session_factory() as session:
        stored = session.query(CrawlDeadLetter).filter_by(dlq_id=dlq_id).one()
        run_c = session.query(CrawlExecutionRun).filter_by(run_id=result.replay_run_id).one()
        assert stored.status == DlqStatus.RESOLVED.value
        assert run_c.status == "success"
        assert run_c.replay_of_run_id == "run-a"
    assert replay_run_id  # pre-allocated id from the crashed attempt is superseded


@pytest.mark.asyncio
async def test_crash_after_run_b_success_resolves(
    session_factory: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_sessions(monkeypatch, session_factory)
    dlq_id = _seed_pending(session_factory)

    with session_factory() as session:
        _, replay_run_id = CrawlDeadLetterService(session).prepare_retry(dlq_id)
        session.commit()

    # RUN-B completed successfully but the process crashed before txn2.
    _seed_linked_run(session_factory, replay_run_id, status="success")

    recovered = recover_stuck_retrying(stale_before=_cutoff(), session_factory=session_factory)

    assert recovered[0].action == "resolved"
    stored = _stored(session_factory, dlq_id)
    assert stored.status == DlqStatus.RESOLVED.value
    assert stored.replay_run_id == replay_run_id
    assert stored.resolved_at is not None


@pytest.mark.asyncio
async def test_stale_running_run_b_is_finalized_and_rescheduled(
    session_factory: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_sessions(monkeypatch, session_factory)
    dlq_id = _seed_pending(session_factory)

    with session_factory() as session:
        _, replay_run_id = CrawlDeadLetterService(session).prepare_retry(dlq_id)
        session.commit()

    now = datetime.now(UTC).replace(tzinfo=None)
    _seed_linked_run(session_factory, replay_run_id, status="running", started_at=now - timedelta(hours=2))

    recovered = recover_stuck_retrying(
        stale_before=_cutoff(),
        run_stale_before=now - timedelta(minutes=1),
        session_factory=session_factory,
    )

    assert recovered[0].action == "finalized_interrupted"
    assert _stored(session_factory, dlq_id).status == DlqStatus.PENDING.value
    with session_factory() as session:
        run_b = session.query(CrawlExecutionRun).filter_by(run_id=replay_run_id).one()
        assert run_b.status == "failed"
        assert run_b.error_code == "REPLAY_INTERRUPTED"


def _stored(session_factory: sessionmaker, dlq_id: str) -> CrawlDeadLetter:
    with session_factory() as session:
        return session.query(CrawlDeadLetter).filter_by(dlq_id=dlq_id).one()
