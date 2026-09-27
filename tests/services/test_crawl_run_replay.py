"""Tests for independent crawl run replay (D4-1)."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus
from src.models.crawl_execution import CrawlExecutionRun
from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
from src.repositories.crawl_execution_repository import CrawlExecutionRepository, CrawlRunSpec
from src.services.crawl_dead_letter_service import CrawlDeadLetterService
from src.services.crawl_run_replay import (
    CrawlRunNotFoundError,
    CrawlRunNotReplayableError,
    UnsupportedReplayCrawlerError,
    replay_crawl_run,
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


def _seed_run(session_factory: sessionmaker, *, run_id: str = "run-a", status: str = "partial") -> None:
    with session_factory() as session:
        repo = CrawlExecutionRepository(session)
        run = repo.start_run(
            CrawlRunSpec(
                crawler="awards", target_type="award_history", run_id=run_id, target_id="kbo_awards_yagoonara"
            ),
        )
        if status == "partial":
            repo.mark_partial(run)
        elif status == "success":
            repo.mark_success(run)
        elif status == "failed":
            repo.mark_failed(run, error_code="FETCH_TIMEOUT", error_message="x")
        session.commit()


def _executor(session_factory: sessionmaker, *, status: str = "success", raises: bool = False) -> Callable:
    def _run(snapshot, replay_run_id: str) -> None:
        with session_factory() as session:
            repo = CrawlExecutionRepository(session)
            run = repo.start_run(
                CrawlRunSpec(
                    crawler=snapshot.crawler,
                    target_type=snapshot.target_type,
                    target_id=snapshot.target_id,
                    parent_run_id=snapshot.run_id,
                    replay_of_run_id=snapshot.run_id,
                    run_id=replay_run_id,
                ),
            )
            if status == "success":
                repo.mark_success(run)
            else:
                repo.mark_failed(run, error_code="PERSIST_CONNECTION", error_message="down")
            session.commit()
        if raises:
            raise RuntimeError("boom")

    return _run


def test_replay_creates_linked_run_and_preserves_original(session_factory: sessionmaker) -> None:
    _seed_run(session_factory, status="partial")
    executors = {"awards": _executor(session_factory, status="success")}

    result = replay_crawl_run("run-a", executors=executors, session_factory=session_factory)

    assert result.success is True
    assert result.status == "success"
    with session_factory() as session:
        original = session.query(CrawlExecutionRun).filter_by(run_id="run-a").one()
        replay = session.query(CrawlExecutionRun).filter_by(run_id=result.replay_run_id).one()
        assert original.status == "partial"  # original untouched
        assert replay.replay_of_run_id == "run-a"
        assert replay.parent_run_id == "run-a"
        assert replay.run_id == result.replay_run_id


def test_replay_does_not_touch_dead_letters(session_factory: sessionmaker) -> None:
    _seed_run(session_factory, status="failed")
    with session_factory() as session:
        letter = CrawlDeadLetterService(session).enqueue(
            DeadLetterSpec(
                original_run_id="run-a",
                crawler="awards",
                target_type="award_history",
                target_id="kbo_awards_yagoonara",
                failure_stage="fetch",
                error_code="FETCH_TIMEOUT",
            ),
        )
        session.commit()
        before = (letter.status, letter.retry_count)

    executors = {"awards": _executor(session_factory, status="success")}
    replay_crawl_run("run-a", executors=executors, session_factory=session_factory)

    with session_factory() as session:
        stored = session.query(CrawlDeadLetter).one()
        assert (stored.status, stored.retry_count) == before
        assert stored.status == DlqStatus.PENDING.value


def test_replay_rejects_running_run(session_factory: sessionmaker) -> None:
    _seed_run(session_factory, status="running")
    with pytest.raises(CrawlRunNotReplayableError):
        replay_crawl_run("run-a", executors={"awards": _executor(session_factory)}, session_factory=session_factory)


def test_replay_rejects_unknown_run(session_factory: sessionmaker) -> None:
    with pytest.raises(CrawlRunNotFoundError):
        replay_crawl_run("missing", executors={"awards": _executor(session_factory)}, session_factory=session_factory)


def test_replay_rejects_unsupported_crawler(session_factory: sessionmaker) -> None:
    _seed_run(session_factory, status="success")
    with pytest.raises(UnsupportedReplayCrawlerError):
        replay_crawl_run("run-a", executors={}, session_factory=session_factory)


def test_replay_executor_failure_returns_failure(session_factory: sessionmaker) -> None:
    _seed_run(session_factory, status="failed")
    executors = {"awards": _executor(session_factory, status="failed", raises=True)}

    result = replay_crawl_run("run-a", executors=executors, session_factory=session_factory)

    assert result.success is False
    assert result.status == "failed"
    with session_factory() as session:
        replay = session.query(CrawlExecutionRun).filter_by(run_id=result.replay_run_id).one()
        assert replay.replay_of_run_id == "run-a"
