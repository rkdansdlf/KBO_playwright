"""Phase B Definition of Done: RUN-A partial -> DLQ -> replay RUN-B -> resolved.

This is the single contract test that must hold for the dead letter queue to be
considered complete. It exercises the real ledger, the real DLQ service, the
real replay dispatcher and the AwardCrawler canary with network calls faked.
"""

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.award_crawler import AwardCrawler
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.services.crawl_dead_letter_service import retry_dead_letter
from src.services.crawl_replay_dispatcher import build_default_dispatcher
from src.utils.request_policy import RequestPolicy

EMPTY_HTML = "<html></html>"


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


@pytest.fixture
def session(session_factory: sessionmaker) -> Iterator[Session]:
    active = session_factory()
    try:
        yield active
    finally:
        active.close()


@pytest.fixture
def patched_award_crawler(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """Fake award source fetching with a mutable yagoonara availability flag."""
    state: dict[str, object] = {"yagoonara_ok": False, "wiki_calls": 0}

    async def fake_wiki(self: AwardCrawler, title: str) -> BeautifulSoup:
        state["wiki_calls"] = int(state["wiki_calls"]) + 1
        return BeautifulSoup(EMPTY_HTML, "html.parser")

    async def fake_yagoonara(self: AwardCrawler) -> BeautifulSoup:
        if not state["yagoonara_ok"]:
            raise httpx.ConnectError("offline")
        return BeautifulSoup(EMPTY_HTML, "html.parser")

    async def no_delay(self: RequestPolicy, *, host: str) -> None:
        return None

    monkeypatch.setattr(AwardCrawler, "_fetch_wiki_page", fake_wiki)
    monkeypatch.setattr(AwardCrawler, "_fetch_yagoonara", fake_yagoonara)
    monkeypatch.setattr(RequestPolicy, "delay_async", no_delay)
    return state


def _wire_sessions(monkeypatch: pytest.MonkeyPatch, session_factory: sessionmaker) -> None:
    monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", session_factory)
    monkeypatch.setattr("src.services.crawl_dead_letter_service.SessionLocal", session_factory)
    monkeypatch.setattr("src.services.crawl_replay_dispatcher.SessionLocal", session_factory)
    monkeypatch.setattr("src.crawlers.award_crawler.SessionLocal", session_factory)


@pytest.mark.asyncio
async def test_run_a_partial_replays_to_run_b_and_resolves(
    session_factory: sessionmaker,
    patched_award_crawler: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_sessions(monkeypatch, session_factory)

    # --- RUN-A: wiki succeeds, yagoonara fails -> partial + one DLQ entry ---
    count_a = await AwardCrawler().run(save=False)
    assert count_a == 0

    with session_factory() as check:
        run_a = check.query(CrawlExecutionRun).one()
        letter = check.query(CrawlDeadLetter).one()
        assert run_a.status == "partial"
        assert run_a.crawler == "awards"
        assert letter.status == "pending"
        assert letter.target_id == "kbo_awards_yagoonara"
        assert letter.original_run_id == run_a.run_id
        dlq_id = letter.dlq_id
        run_a_id = run_a.run_id

    # --- Replay: yagoonara recovers; only the failed source is re-run ---
    patched_award_crawler["yagoonara_ok"] = True
    wiki_calls_before = int(patched_award_crawler["wiki_calls"])
    dispatcher = build_default_dispatcher()

    result = retry_dead_letter(dlq_id, dispatcher, session_factory=session_factory)

    assert result.success is True
    assert result.status.value == "resolved"
    assert int(patched_award_crawler["wiki_calls"]) == wiki_calls_before  # source-scoped replay

    with session_factory() as check:
        run_a = check.query(CrawlExecutionRun).filter_by(run_id=run_a_id).one()
        run_b = check.query(CrawlExecutionRun).filter_by(run_id=result.replay_run_id).one()
        letter = check.query(CrawlDeadLetter).filter_by(dlq_id=dlq_id).one()

        # Lineage: RUN-A -> RUN-B
        assert run_a.status == "partial"
        assert run_b.status == "success"
        assert run_b.replay_of_run_id == run_a.run_id
        assert run_b.parent_run_id == run_a.run_id
        assert letter.status == "resolved"
        assert letter.replay_run_id == run_b.run_id
        assert letter.resolved_at is not None
        assert run_b.records_read == 0
