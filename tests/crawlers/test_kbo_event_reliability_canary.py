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

#: A page the crawler accepts: the title carries the event, and the site frame
#: is present. The frame is what separates "this guide has nothing running"
#: from "this is no longer a KBO page" -- a synthetic fixture without it is
#: drift, which is the whole point of the tests below.
KBO_SITE_FRAME = "<header></header><nav></nav><footer></footer>"
PAGE_HTML = f"<html><head><title>KBO 공식 행사</title></head><body>{KBO_SITE_FRAME}</body></html>"
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


def _fetch_frame_less(failing_url: str):
    """Serve a real captured page with the site frame removed.

    Built by deleting the elements rather than by writing a small document, so
    the only difference from the healthy case is the frame itself.
    """
    from bs4 import BeautifulSoup

    from tests.crawlers.test_kbo_event_page_outcome import _without_frame, _html

    drifted = _without_frame(_html("kbo_business_event_safeguide"))

    async def _fetch(_crawler: KboEventCrawler, url: str) -> tuple[str, str]:
        if url == failing_url:
            return drifted, url
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


class TestAPageThatStoppedBeingAPageIsAnIncident:
    """Drift used to arrive as an empty candidate list and be recorded as success.

    Nothing raised, nothing was enqueued, and the run said it had read a page
    that carried nothing. These assertions are about the queue, because that is
    the only durable trace a redesigned page leaves behind.
    """

    @pytest.mark.asyncio
    async def test_a_frameless_page_is_queued_as_a_selector_failure(
        self, session_factory: sessionmaker, monkeypatch
    ) -> None:
        monkeypatch.setattr(KboEventCrawler, "_fetch_html", _fetch_frame_less(PAGE_A))

        await _crawler(PAGE_A, PAGE_B).run()

        letters = _letters(session_factory)
        drift = [letter for letter in letters if letter.error_code == FailureCode.PARSE_SELECTOR_MISSING.value]
        assert len(drift) == 1, "a redesigned page must reach the queue"
        # The target is the page slug, not the URL: `CrawlDeadLetter.target_id` is
        # 128 characters and the replay handler narrows the sweep to this page,
        # so the slug is what has to survive into the queue.
        assert drift[0].target_id == "Mvp.aspx"
        assert drift[0].source_url == PAGE_A
        assert drift[0].failure_stage == FailureStage.PARSE.value

    @pytest.mark.asyncio
    async def test_it_does_not_end_the_sweep(self, session_factory: sessionmaker, monkeypatch) -> None:
        """One redesigned page is a partial sweep, not a lost one."""
        monkeypatch.setattr(KboEventCrawler, "_fetch_html", _fetch_frame_less(PAGE_A))

        events = await _crawler(PAGE_A, PAGE_B).run()

        assert events, "the readable page must still contribute its events"
        assert _only_run(session_factory).status == "partial"

    @pytest.mark.asyncio
    async def test_the_raw_page_is_kept_as_evidence(self, session_factory: sessionmaker, monkeypatch) -> None:
        """The redesigned document is what a re-parse would start from.

        Keeping it is the whole reason a drift is worth enqueuing: without the
        body there is nothing to re-read once the site is fixed.
        """
        monkeypatch.setattr(KboEventCrawler, "_fetch_html", _fetch_frame_less(PAGE_A))

        crawler = _crawler(PAGE_A, PAGE_B)
        await crawler.run()

        assert [page["url"] for page in crawler._raw_pages] == [PAGE_A, PAGE_B]

    @pytest.mark.asyncio
    async def test_a_drift_letter_is_never_retried(self, session_factory: sessionmaker, monkeypatch) -> None:
        """Retrying drift returns the same redesigned document, five times over."""
        from src.services.crawl_retry_policy import decide

        monkeypatch.setattr(KboEventCrawler, "_fetch_html", _fetch_frame_less(PAGE_A))

        await _crawler(PAGE_A, PAGE_B).run()

        drift = next(
            letter
            for letter in _letters(session_factory)
            if letter.error_code == FailureCode.PARSE_SELECTOR_MISSING.value
        )
        decision = decide(drift.error_code, retry_count=drift.retry_count, max_retries=drift.max_retries)
        assert decision.retryable is False

    @pytest.mark.asyncio
    async def test_the_recording_switch_covers_drift_too(self, session_factory: sessionmaker, monkeypatch) -> None:
        """A switch that covered only exceptions would silently miss drift."""
        monkeypatch.setattr(KboEventCrawler, "_fetch_html", _fetch_frame_less(PAGE_A))

        await _crawler(PAGE_A, PAGE_B).run(record_dead_letters=False)

        assert _letters(session_factory) == []
        assert _only_run(session_factory).status == "partial"
