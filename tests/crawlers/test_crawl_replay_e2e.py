"""D4-3 AwardCrawler canary E2E: independent replay preserves original and DLQ."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.award_crawler import AwardCrawler
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
from src.repositories.crawl_execution_repository import CrawlExecutionRepository, CrawlRunSpec
from src.services.crawl_dead_letter_service import CrawlDeadLetterService
from src.services.crawl_run_replay import replay_crawl_run


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
    monkeypatch.setattr("src.crawlers.award_crawler.SessionLocal", session_factory)


def _seed_run_a(session_factory: sessionmaker) -> None:
    with session_factory() as session:
        repo = CrawlExecutionRepository(session)
        run = repo.start_run(
            CrawlRunSpec(
                crawler="awards",
                target_type="award_history",
                target_id="kbo_awards_yagoonara",
                run_id="run-a",
            ),
        )
        repo.mark_partial(run)
        session.commit()


def _seed_dead_letter(session_factory: sessionmaker) -> tuple[str, int]:
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
        return letter.status, letter.retry_count


def test_award_canary_replay_links_run_and_preserves_dlq(
    session_factory: sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_sessions(monkeypatch, session_factory)
    _seed_run_a(session_factory)
    dlq_before = _seed_dead_letter(session_factory)

    async def empty_crawl(self: AwardCrawler, types: set[str] | None = None, source_key: str | None = None) -> list:
        return []

    monkeypatch.setattr(AwardCrawler, "crawl", empty_crawl)

    result = replay_crawl_run("run-a", session_factory=session_factory)

    assert result.success is True
    assert result.status == "success"

    with session_factory() as session:
        original = session.query(CrawlExecutionRun).filter_by(run_id="run-a").one()
        replay = session.query(CrawlExecutionRun).filter_by(run_id=result.replay_run_id).one()
        letters = session.query(CrawlDeadLetter).all()

        # RUN-A untouched.
        assert original.status == "partial"
        # RUN-X lineage.
        assert replay.replay_of_run_id == "run-a"
        assert replay.parent_run_id == "run-a"
        assert replay.crawler == "awards"
        # DLQ untouched (no new letters, existing one unchanged).
        assert len(letters) == 1
        assert (letters[0].status, letters[0].retry_count) == dlq_before
