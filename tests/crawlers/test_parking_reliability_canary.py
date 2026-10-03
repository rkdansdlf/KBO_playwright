"""주차장 크롤러 신뢰성 canary.

원장+DLQ 체인의 네 번째 이식이며, 처음으로 **실패가 예외가 아니라 빈 결과로**
나타나는 크롤러입니다. 이전에는 읽지 못한 팀 페이지가 로그 한 줄과 빈 리스트로
끝나 아무도 재처리할 수 없었습니다.

검증하는 계약:

    읽지 못한 팀은 격리되어 스윕이 계속되고, 실행은 ``partial``이 되며,
    팀 단위 DLQ 항목이 생긴다(팀이 재처리 단위);
    모든 팀이 실패하면 실행은 ``failed``가 된다;
    정상 스윕은 ``success``이며 DLQ 항목이 없다.

크롤러·원장·DLQ·taxonomy는 실제로 동작하고, HTTP 경계만 대체합니다.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.failure_taxonomy import FailureCode, stage_for_code
from src.crawlers.parking_crawler import (
    PARKING_CRAWLER_NAME,
    PARKING_TARGET_TYPE,
    TEAM_PARKING_SOURCES,
    ParkingCrawler,
)
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.monitoring import crawler_metrics as cm

SK_URL = TEAM_PARKING_SOURCES["SK"]["url"]
HTML = "<html><body>기본 요금: 5,000원</body></html>"


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


def _crawler(failing_urls: set[str] | None = None) -> ParkingCrawler:
    failing = failing_urls or set()

    async def _fetch_text(url: str) -> CrawlResult:
        if url in failing:
            return CrawlResult.failure(
                CrawlOutcome.PERMANENT_ERROR,
                error="page unavailable",
                error_code=FailureCode.FETCH_HTTP_ERROR.value,
                http_status=503,
                url=url,
            )
        return CrawlResult.success(HTML, http_status=200, url=url)

    crawler = ParkingCrawler()
    crawler._http.fetch_text = AsyncMock(side_effect=_fetch_text)
    return crawler


def _only_run(session_factory: sessionmaker) -> CrawlExecutionRun:
    with session_factory() as check:
        return check.query(CrawlExecutionRun).one()


def _letters(session_factory: sessionmaker) -> list[CrawlDeadLetter]:
    with session_factory() as check:
        return check.query(CrawlDeadLetter).all()


class TestTeamFailures:
    @pytest.mark.asyncio
    async def test_a_failing_team_is_isolated_and_enqueued(
        self,
        session_factory: sessionmaker,
    ) -> None:
        lots = await _crawler({SK_URL}).run()

        # 나머지 두 팀은 정상적으로 수집된다.
        assert len(lots) == 2
        run = _only_run(session_factory)
        assert run.status == "partial"
        assert run.records_read == 2
        assert run.crawler == PARKING_CRAWLER_NAME
        assert run.target_type == PARKING_TARGET_TYPE

        letter = _letters(session_factory)[0]
        assert letter.target_id == "SK"
        assert letter.source_url == SK_URL
        assert letter.error_code == FailureCode.FETCH_HTTP_ERROR.value
        assert letter.original_run_id == run.run_id
        # failure_stage는 code에서만 파생된다.
        assert letter.failure_stage == stage_for_code(letter.error_code).value

    @pytest.mark.asyncio
    async def test_every_failing_team_marks_the_run_failed(self, session_factory: sessionmaker) -> None:
        all_urls = {info["url"] for info in TEAM_PARKING_SOURCES.values()}

        lots = await _crawler(all_urls).run()

        assert lots == []
        run = _only_run(session_factory)
        assert run.status == "failed"
        assert run.error_code == FailureCode.FETCH_HTTP_ERROR.value
        assert sorted(letter.target_id for letter in _letters(session_factory)) == ["LG", "SK", "SS"]

    @pytest.mark.asyncio
    async def test_no_dead_letter_is_enqueued_when_recording_is_disabled(
        self,
        session_factory: sessionmaker,
    ) -> None:
        await _crawler({SK_URL}).run(record_dead_letters=False)

        assert _only_run(session_factory).status == "partial"
        assert _letters(session_factory) == []


class TestHealthyAndFilteredRuns:
    @pytest.mark.asyncio
    async def test_a_healthy_sweep_is_a_successful_run(self, session_factory: sessionmaker) -> None:
        lots = await _crawler().run()

        assert len(lots) == 3
        run = _only_run(session_factory)
        assert run.status == "success"
        assert run.error_code is None
        assert _letters(session_factory) == []

    @pytest.mark.asyncio
    async def test_a_team_filter_narrows_the_ledger_target(self, session_factory: sessionmaker) -> None:
        lots = await _crawler().run(team_filter="LG")

        assert len(lots) == 1
        run = _only_run(session_factory)
        assert run.target_id == "LG"
        assert run.source_url == TEAM_PARKING_SOURCES["LG"]["url"]
