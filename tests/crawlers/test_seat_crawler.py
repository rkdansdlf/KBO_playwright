from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers import seat_crawler

from src.crawlers.http_client import CrawlerHttpClient
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.crawlers.seat_crawler import TEAM_SEAT_SOURCES, SeatCrawler
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.models.stadium_seat_section import StadiumSeatSection


@pytest.fixture(autouse=True)
def _ledger_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give the run ledger a database, which the crawler started writing to.

    The crawler classifies outcomes but recorded no run at all, which is why the
    seat table could age for weeks with nothing in the ledger to explain it. Now
    that it writes, the tests need somewhere for those rows to go -- otherwise the
    real engine opens and the run fails on a table the test database never made.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    CrawlDeadLetter.__table__.create(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", factory)
    monkeypatch.setattr("src.services.crawl_dead_letter_service.SessionLocal", factory)


def _ok(html: str, status: int = 200) -> CrawlResult[str]:
    """A successful fetch, as `CrawlerHttpClient` would return one."""
    return CrawlResult.success(html, http_status=status)


def _failure(code: str = "fetch_http_error", status: int | None = 503) -> CrawlResult[str]:
    """A failed fetch carrying the taxonomy code the transport assigned."""
    return CrawlResult.failure(
        CrawlOutcome.RETRYABLE_ERROR,
        error="seat page unavailable",
        error_code=code,
        http_status=status,
        url="https://www.lgtwins.com/ticket/seatinfo",
    )


class TestParseSeatPage:
    def setup_method(self):
        self.crawler = SeatCrawler()

    def test_parses_seat_sections(self):
        html = "<html><body>블루석 레드존 골드석</body></html>"
        result = self.crawler._parse_seat_page(html, "LG", {"stadium_id": "JAMSIL"})
        assert len(result) >= 1
        assert all(s["stadium_id"] == "JAMSIL" for s in result)

    def test_deduplicates_sections(self):
        html = "<html><body>블루석 블루석 블루석</body></html>"
        result = self.crawler._parse_seat_page(html, "LG", {"stadium_id": "JAMSIL"})
        blues = [s for s in result if s["section_name"] == "블루석"]
        assert len(blues) == 1

    def test_empty_html_returns_empty_list(self):
        result = self.crawler._parse_seat_page("", "LG", {"stadium_id": "JAMSIL"})
        assert result == []


class TestSeatCrawlerOperations:
    @pytest.mark.asyncio
    async def test_crawl_team_fetches_and_tracks_raw_page(self):
        crawler = SeatCrawler()
        info = TEAM_SEAT_SOURCES["LG"]
        crawler._http.fetch_text = AsyncMock(return_value=_ok("<p>블루석</p>"))  # type: ignore[method-assign]

        sections = await crawler._crawl_team_seats("LG", info)

        crawler._http.fetch_text.assert_awaited_once_with(info["url"])
        assert sections[0]["stadium_id"] == "JAMSIL"
        assert crawler._raw_pages[0]["source_key"] == "lg_twins_seat"
        assert crawler._raw_pages[0]["status_code"] == 200

    @pytest.mark.asyncio
    async def test_a_failed_fetch_is_not_an_empty_page(self):
        """A 503 and a page with no seat names both used to arrive as `[]`.

        Only one of them is worth looking at again, and the difference is the
        whole reason the fetch reports a classified result instead of a list.
        """
        crawler = SeatCrawler()
        info = TEAM_SEAT_SOURCES["LG"]
        crawler._http.fetch_text = AsyncMock(return_value=_failure("fetch_http_error", 503))  # type: ignore[method-assign]

        assert await crawler._crawl_team_seats("LG", info) == []

        read = await crawler._fetch_seat_page(info["url"])
        assert read.ok is False
        assert read.error_code == "fetch_http_error"
        assert read.http_status == 503
        # No snapshot is kept for a page that never arrived: there is no
        # evidence to replay, and an empty snapshot would read as "the page had
        # no seats on it".
        assert crawler._raw_pages == []

    @pytest.mark.asyncio
    async def test_a_readable_page_with_no_seats_is_not_a_failure(self):
        """The other half of the distinction: readable, and genuinely empty."""
        html = "<html><body>빈 페이지</body></html>"
        crawler = SeatCrawler()
        info = TEAM_SEAT_SOURCES["LG"]
        crawler._http.fetch_text = AsyncMock(return_value=_ok(html))  # type: ignore[method-assign]

        sections = await crawler._crawl_team_seats("LG", info)

        read = await crawler._fetch_seat_page(info["url"])
        assert read.ok is True
        assert sections == []
        # The page *was* read, so it is kept as evidence. The body itself is
        # asserted because the snapshot is what `kbo snapshot replay` re-parses:
        # a snapshot record with no `html` in it replays successfully and
        # reconstructs nothing, which is the one outcome worse than having no
        # snapshot at all.
        assert crawler._raw_pages[0]["html"] == html

    def test_the_crawler_does_not_build_its_own_client(self):
        """A hand-rolled `httpx.AsyncClient` is what the migration removed.

        Checked on the module text rather than on an attribute, because
        `BaseHttpCrawler` sets `self.throttle` in its constructor whether or not
        this crawler uses it -- so the attribute can never go away and asserting
        on it would pass no matter what this crawler did. The source check is
        also what the adoption matrix's transport axis measures, so the two
        cannot drift apart.
        """
        source = inspect.getsource(seat_crawler)

        assert "httpx.AsyncClient(" not in source
        assert isinstance(SeatCrawler()._http, CrawlerHttpClient)

    @pytest.mark.asyncio
    async def test_run_continues_after_team_failure_and_respects_filter(self):
        crawler = SeatCrawler()
        crawler._crawl_team_seats = AsyncMock(side_effect=RuntimeError("LG unavailable"))

        records = await crawler.run(team_filter="LG")

        assert records == []
        crawler._crawl_team_seats.assert_awaited_once_with("LG", TEAM_SEAT_SOURCES["LG"])

    def test_save_to_db_persists_sections_and_clears_raw_pages(self):
        session = MagicMock()
        repo = MagicMock()
        crawler = SeatCrawler()
        crawler._raw_pages = [{"source_key": "lg_twins_seat"}]
        section = {"section_name": "블루석"}

        with (
            patch("src.crawlers.seat_crawler.SessionLocal") as session_local,
            patch("src.crawlers.seat_crawler.save_raw_snapshots", return_value=1),
            patch("src.crawlers.seat_crawler.StadiumSeatSectionRepository", return_value=repo),
        ):
            session_local.return_value.__enter__.return_value = session
            crawler._save_to_db([section])

        repo.save.assert_called_once_with(section)
        session.commit.assert_called_once()
        assert crawler._raw_pages == []

    def test_a_section_carrying_the_team_code_really_reaches_the_table(self):
        """The attribution key is not a column, and the save path caught the error.

        The crawler stamps ``team_code`` onto every section so the rows can be
        grouped per team and the raw snapshots can be keyed. The table has no
        such column, so handing the collected dict to the ORM raised
        ``TypeError``, ``SEAT_SAVE_EXCEPTIONS`` swallowed it as one of its own,
        and the run reported zero rows written while every section had failed.
        """
        engine = create_engine("sqlite:///:memory:")
        StadiumSeatSection.__table__.create(engine)
        session = sessionmaker(bind=engine)()

        crawler = SeatCrawler()
        crawler._raw_pages = []

        with (
            patch("src.crawlers.seat_crawler.SessionLocal", return_value=session),
            patch("src.crawlers.seat_crawler.save_raw_snapshots", return_value=0),
        ):
            saved, failed = crawler._save_to_db(
                [
                    {
                        "stadium_id": "JAMSIL",
                        "section_name": "블루석",
                        "seat_grade": "블루석",
                        "team_code": "LG",
                    },
                ],
            )

        assert (saved, failed) == (1, 0)
        rows = session.execute(select(StadiumSeatSection)).scalars().all()
        assert [row.section_name for row in rows] == ["블루석"]
