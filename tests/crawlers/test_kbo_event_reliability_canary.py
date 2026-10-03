"""The KBO official events reliability canary.

Third port of the ledger + dead letter chain, and the first multi-page sweep.
Before this port a single failing page ended all seven, so one flaky KBO page
could cost the whole event feed with nothing but a traceback to show for it.

The contract under test:

    one failing page is isolated -- the sweep continues, the run is ``partial``,
    and the page is enqueued as its own dead letter (a page is the replay unit);
    when every page fails the run is ``failed``;
    a persistence failure is still raised (our own write, the CLI must not exit
    successfully) but is classified, enqueued, and its taxonomy reaches the
    ledger through the propagating exception;
    a compliance skip is not a failure.

The crawler, the ledger, the DLQ, and the taxonomy run for real. Only the page
fetch is replaced.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.failure_taxonomy import FailureCode, FailureStage, stage_for_code
from src.crawlers.kbo_event_crawler import (
    KBO_EVENT_SOURCE_KEY,
    KBO_EVENT_CRAWLER_NAME,
    KBO_EVENT_TARGET_TYPE,
    KboEventCrawler,
)
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.monitoring import crawler_metrics as cm

PAGE_HTML = "<html><head><title>KBO 공식 행사</title></head></html>"
PAGE_A = "https://www.koreabaseball.com/Kbo/BusinessAndEvent/Mvp.aspx"
PAGE_B = "https://www.koreabaseball.com/Kbo/BusinessAndEvent/Draft.aspx"


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


@pytest.fixture(autouse=True)
def _fresh_metric_state() -> Iterator[None]:
    cm.reset_initialized_crawlers()
    yield
    cm.reset_initialized_crawlers()


@pytest.fixture(autouse=True)
def _wire_sessions(monkeypatch: pytest.MonkeyPatch, session_factory: sessionmaker) -> None:
    monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", session_factory)
    monkeypatch.setattr("src.services.crawl_dead_letter_service.SessionLocal", session_factory)
    monkeypatch.setattr("src.crawlers.kbo_event_crawler.compliance.is_allowed", AsyncMock(return_value=True))


def _crawler(*urls: str) -> KboEventCrawler:
    crawler = KboEventCrawler(base_url=urls[0])
    crawler.urls = urls
    return crawler


def _fetch_ok():
    async def _fetch(_crawler: KboEventCrawler, url: str) -> tuple[str, str]:
        return PAGE_HTML, url

    return _fetch


def _fetch_failing(failing_url: str, exc: BaseException):
    async def _fetch(_crawler: KboEventCrawler, url: str) -> tuple[str, str]:
        if url == failing_url:
            raise exc
        return PAGE_HTML, url

    return _fetch


def _only_run(session_factory: sessionmaker) -> CrawlExecutionRun:
    with session_factory() as check:
        return check.query(CrawlExecutionRun).one()


def _letters(session_factory: sessionmaker) -> list[CrawlDeadLetter]:
    with session_factory() as check:
        return check.query(CrawlDeadLetter).all()


class TestMultiPageSweeps:
    @pytest.mark.asyncio
    async def test_a_failing_page_does_not_end_the_sweep(self, session_factory: sessionmaker, monkeypatch) -> None:
        monkeypatch.setattr(KboEventCrawler, "_fetch_html", _fetch_failing(PAGE_A, TimeoutError("slow")))

        events = await _crawler(PAGE_A, PAGE_B).run()

        # The healthy page still contributed its event.
        assert [event["title"] for event in events] == ["KBO 공식 행사"]
        run = _only_run(session_factory)
        assert run.status == "partial"
        assert run.records_read == 1
        assert run.crawler == KBO_EVENT_CRAWLER_NAME
        assert run.target_type == KBO_EVENT_TARGET_TYPE
        assert run.target_id == KBO_EVENT_SOURCE_KEY

        letter = _letters(session_factory)[0]
        assert letter.target_id == "Mvp.aspx"
        assert letter.source_url == PAGE_A
        assert letter.error_code == FailureCode.FETCH_TIMEOUT.value
        assert letter.original_run_id == run.run_id
        assert letter.failure_stage == stage_for_code(letter.error_code).value

    @pytest.mark.asyncio
    async def test_every_failing_page_gets_its_own_dead_letter(
        self,
        session_factory: sessionmaker,
        monkeypatch,
    ) -> None:
        async def _fetch(_crawler: KboEventCrawler, _url: str) -> tuple[str, str]:
            raise TimeoutError("slow")

        monkeypatch.setattr(KboEventCrawler, "_fetch_html", _fetch)

        events = await _crawler(PAGE_A, PAGE_B).run()

        assert events == []
        run = _only_run(session_factory)
        assert run.status == "failed"
        assert run.error_code == FailureCode.FETCH_TIMEOUT.value
        assert sorted(letter.target_id for letter in _letters(session_factory)) == ["Draft.aspx", "Mvp.aspx"]

    @pytest.mark.asyncio
    async def test_a_healthy_sweep_is_a_successful_run(self, session_factory: sessionmaker, monkeypatch) -> None:
        monkeypatch.setattr(KboEventCrawler, "_fetch_html", _fetch_ok())

        await _crawler(PAGE_A, PAGE_B).run()

        run = _only_run(session_factory)
        assert run.status == "success"
        assert run.error_code is None
        assert _letters(session_factory) == []

    @pytest.mark.asyncio
    async def test_no_dead_letter_is_enqueued_when_recording_is_disabled(
        self,
        session_factory: sessionmaker,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(KboEventCrawler, "_fetch_html", _fetch_failing(PAGE_A, TimeoutError("slow")))

        await _crawler(PAGE_A, PAGE_B).run(record_dead_letters=False)

        assert _only_run(session_factory).status == "partial"
        assert _letters(session_factory) == []


class TestPersistFailures:
    @pytest.mark.asyncio
    async def test_a_persist_failure_propagates_with_its_taxonomy(
        self,
        session_factory: sessionmaker,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(KboEventCrawler, "_fetch_html", _fetch_ok())

        def _boom(_crawler: KboEventCrawler, _events: list[dict]) -> int:
            raise SQLAlchemyError("connection reset")

        monkeypatch.setattr(KboEventCrawler, "_save_to_db", _boom)

        with pytest.raises(SQLAlchemyError):
            await _crawler(PAGE_A).run(save=True)

        run = _only_run(session_factory)
        assert run.status == "failed"
        # The taxonomy reaches the ledger even though the original exception type
        # is what propagates.
        assert run.error_code == FailureCode.PERSIST_CONNECTION.value

        letter = _letters(session_factory)[0]
        assert letter.target_id == KBO_EVENT_SOURCE_KEY
        assert letter.error_code == FailureCode.PERSIST_CONNECTION.value
        assert letter.failure_stage == FailureStage.PERSIST.value


class TestComplianceSkips:
    @pytest.mark.asyncio
    async def test_a_compliance_skip_is_not_a_failure(self, session_factory: sessionmaker, monkeypatch) -> None:
        monkeypatch.setattr("src.crawlers.kbo_event_crawler.compliance.is_allowed", AsyncMock(return_value=False))

        events = await _crawler(PAGE_A).run()

        assert events == []
        run = _only_run(session_factory)
        assert run.status == "success"
        assert run.error_code is None
        assert run.checkpoint is not None
        assert run.checkpoint["outcome"] == "source_limited"
        assert _letters(session_factory) == []
