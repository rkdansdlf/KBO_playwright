"""구장 음식 크롤러 신뢰성 canary.

원장+DLQ 체인의 다섯 번째 이식이며, ``parking``과 동일하게 **실패가 예외가
아니라 빈 결과로** 나타나던 크롤러입니다.

검증하는 계약:

    읽지 못한 팀은 격리되어 스윕이 계속되고, 실행은 ``partial``이 되며,
    팀 단위 DLQ 항목이 생긴다(팀이 재처리 단위);
    모든 팀이 실패하면 실행은 ``failed``가 된다;
    정상 스윕은 ``success``이며 DLQ 항목이 없다.

크롤러·원장·DLQ·taxonomy는 실제로 동작하고, HTTP 경계만 대체합니다.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.failure_taxonomy import CrawlPersistError, FailureCode, stage_for_code
from src.crawlers.food_crawler import (
    FOOD_CRAWLER_NAME,
    FOOD_TARGET_TYPE,
    TEAM_FOOD_SOURCES,
    FoodCrawler,
)
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.models.source_registry import DataSource, RawSourceSnapshot
from src.models.stadium_food_menu_item import StadiumFoodMenuItem
from src.models.stadium_food_vendor import StadiumFoodVendor
from src.monitoring import crawler_metrics as cm
from src.repositories.stadium_food_repository import StadiumFoodVendorRepository

LT_URL = TEAM_FOOD_SOURCES["LT"]["url"]
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


def _crawler(failing_urls: set[str] | None = None) -> FoodCrawler:
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

    crawler = FoodCrawler()
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
    async def test_a_failing_team_is_isolated_and_enqueued(self, session_factory: sessionmaker) -> None:
        vendors = await _crawler({LT_URL}).run()

        # Every other source still answers, so only the failing team's rows are
        # missing. Counting from the table keeps this from going stale when a
        # source is added or retired.
        assert len(vendors) == len(TEAM_FOOD_SOURCES) - 1
        run = _only_run(session_factory)
        assert run.status == "partial"
        assert run.records_read == len(TEAM_FOOD_SOURCES) - 1
        assert run.crawler == FOOD_CRAWLER_NAME
        assert run.target_type == FOOD_TARGET_TYPE

        letter = _letters(session_factory)[0]
        assert letter.target_id == "LT"
        assert letter.source_url == LT_URL
        assert letter.error_code == FailureCode.FETCH_HTTP_ERROR.value
        assert letter.original_run_id == run.run_id
        assert letter.failure_stage == stage_for_code(letter.error_code).value

    @pytest.mark.asyncio
    async def test_every_failing_team_marks_the_run_failed(self, session_factory: sessionmaker) -> None:
        all_urls = {info["url"] for info in TEAM_FOOD_SOURCES.values()}

        vendors = await _crawler(all_urls).run()

        assert vendors == []
        run = _only_run(session_factory)
        assert run.status == "failed"
        assert run.error_code == FailureCode.FETCH_HTTP_ERROR.value
        assert sorted(letter.target_id for letter in _letters(session_factory)) == sorted(TEAM_FOOD_SOURCES)

    @pytest.mark.asyncio
    async def test_no_dead_letter_is_enqueued_when_recording_is_disabled(
        self,
        session_factory: sessionmaker,
    ) -> None:
        await _crawler({LT_URL}).run(record_dead_letters=False)

        assert _only_run(session_factory).status == "partial"
        assert _letters(session_factory) == []


class TestHealthyAndFilteredRuns:
    @pytest.mark.asyncio
    async def test_a_healthy_sweep_is_a_successful_run(self, session_factory: sessionmaker) -> None:
        vendors = await _crawler().run()

        assert len(vendors) == len(TEAM_FOOD_SOURCES)
        run = _only_run(session_factory)
        assert run.status == "success"
        assert run.error_code is None
        assert _letters(session_factory) == []

    @pytest.mark.asyncio
    async def test_a_team_filter_narrows_the_ledger_target(self, session_factory: sessionmaker) -> None:
        vendors = await _crawler().run(team_filter="NC")

        assert len(vendors) == 1
        run = _only_run(session_factory)
        assert run.target_id == "NC"
        assert run.source_url == TEAM_FOOD_SOURCES["NC"]["url"]


class TestAPageThatAnswersWithNothing:
    """A 200 with no menu is not a success.

    Every source this crawler shipped with turned out to be unusable: one dead
    domain, one removed page that still answered 200 with the site's own error
    screen, and one page that was never about food. None of them raised, so the
    parse returned an empty list, and an empty list was indistinguishable from a
    page that simply had nothing to say. The run closed as `success` with
    `records_written=0` and the crawler looked healthy for a month while
    producing nothing at all.
    """

    #: The body `https://www.giantsclub.com/food` actually serves: the site's
    #: "page removed" screen, delivered with HTTP 200.
    REMOVED_PAGE = (
        "<html><body><div class='error_nopage'>"
        "<p class='t1'>서비스 이용에 불편을 드려 죄송합니다.</p>"
        "<p class='t2'>잘못된 주소입력, 주소변경, 주소삭제로 접근이 안되시거나,<br />"
        "예기치 못한 에러가 발생하여 해당 페이지로 접근이 불가능합니다.</p>"
        "</div></body></html>"
    )

    def _empty_pages(self) -> FoodCrawler:
        crawler = FoodCrawler()
        crawler._http.fetch_text = AsyncMock(
            side_effect=lambda url: CrawlResult.success(self.REMOVED_PAGE, http_status=200, url=url),
        )
        return crawler

    @pytest.mark.asyncio
    async def test_an_empty_parse_is_a_failure_not_a_quiet_success(self, session_factory: sessionmaker) -> None:
        vendors = await self._empty_pages().run()

        assert vendors == []
        run = _only_run(session_factory)
        assert run.status == "failed"
        assert run.error_code == FailureCode.PARSE_EMPTY.value
        assert run.records_read == 0
        assert run.records_written == 0

    @pytest.mark.asyncio
    async def test_every_team_is_named_in_the_queue(self, session_factory: sessionmaker) -> None:
        await self._empty_pages().run()

        letters = _letters(session_factory)
        assert sorted(letter.target_id for letter in letters) == sorted(TEAM_FOOD_SOURCES)
        assert {letter.error_code for letter in letters} == {FailureCode.PARSE_EMPTY.value}

    @pytest.mark.asyncio
    async def test_a_page_with_a_menu_is_still_a_success(self, session_factory: sessionmaker) -> None:
        """The control: the new check must not fire on a readable page."""
        vendors = await _crawler().run()

        run = _only_run(session_factory)
        assert len(vendors) == len(TEAM_FOOD_SOURCES)
        assert run.status == "success"
        assert _letters(session_factory) == []
