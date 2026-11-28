from __future__ import annotations

import inspect

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest import mark
import httpx

from src.crawlers.http_client import CrawlerHttpClient
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.crawlers import team_event_crawler as event_module
from src.crawlers.team_event_crawler import TEAM_NEWS_SOURCES, TeamEventCrawler


@pytest.fixture
def crawler():
    return TeamEventCrawler(days_back=30)


def _ok(html: str, status: int = 200) -> CrawlResult[str]:
    """A successful fetch, as `CrawlerHttpClient` would return one."""
    return CrawlResult.success(html, http_status=status)


def _failure(outcome: CrawlOutcome, code: str, status: int) -> CrawlResult[str]:
    """A failed fetch carrying the taxonomy code the transport assigned."""
    return CrawlResult.failure(
        outcome,
        error="team page unavailable",
        error_code=code,
        http_status=status,
        url="https://lg.com/page=1",
    )


class TestCrawlTeam:
    @mark.asyncio
    async def test_fetches_and_parses_events(self, crawler):
        with (
            patch.object(crawler, "_raw_pages", []),
            patch("src.crawlers.team_event_crawler.parse_team_events") as mock_parse,
        ):
            mock_parse.return_value = [
                {"team_id": "LG", "title": "Event 1", "source_url": "https://lg.com/1"},
                {"team_id": "LG", "title": "Event 2", "source_url": "https://lg.com/2"},
            ]
            crawler._fetch_page = AsyncMock(return_value=_ok("<html><div>event</div></html>"))
            config = {"url": "https://lg.com/page={page}", "link_prefix": "https://lg.com"}

            result = await crawler._crawl_team("LG", config)

        assert len(result) == 2
        assert result[0]["title"] == "Event 1"

    @mark.asyncio
    async def test_deduplicates_events(self, crawler):
        with (
            patch.object(crawler, "_raw_pages", []),
            patch("src.crawlers.team_event_crawler.parse_team_events") as mock_parse,
        ):
            mock_parse.return_value = [
                {"team_id": "LG", "title": "Same Event", "source_url": "https://lg.com/1"},
                {"team_id": "LG", "title": "Same Event", "source_url": "https://lg.com/1"},
            ]
            crawler._fetch_page = AsyncMock(return_value=_ok("<html>data</html>"))
            config = {"url": "https://lg.com/page={page}", "link_prefix": "https://lg.com"}

            result = await crawler._crawl_team("LG", config)

        assert len(result) == 1

    @mark.asyncio
    async def test_an_unreachable_page_does_not_end_the_sweep(self, crawler):
        """A retryable failure must not take the remaining pages with it.

        The shared client has already retried and backed off by the time it
        returns, so this is the last word on that page -- but pages 2 and 3 are
        different requests. The old code broke out of the pagination on *any*
        non-200, so a single 503 on page 1 silently truncated the team.
        """
        pages = [
            _failure(CrawlOutcome.RETRYABLE_ERROR, "fetch_http_error", 503),
            _failure(CrawlOutcome.RETRYABLE_ERROR, "fetch_http_error", 503),
            _ok("<html><div>event</div></html>"),
        ]
        crawler._fetch_page = AsyncMock(side_effect=pages)
        config = {"url": "https://lg.com/page={page}", "link_prefix": "https://lg.com"}

        with (
            patch.object(crawler, "_raw_pages", []),
            patch("src.crawlers.team_event_crawler.parse_team_events") as mock_parse,
        ):
            mock_parse.return_value = [{"team_id": "LG", "title": "Event 3", "source_url": "https://lg.com/3"}]
            result = await crawler._crawl_team("LG", config)

        assert [event["title"] for event in result] == ["Event 3"]
        assert crawler._fetch_page.await_count == 3

    @mark.asyncio
    async def test_a_page_that_does_not_exist_ends_the_sweep(self, crawler):
        """The other half: a 404 means there is nothing past this page.

        Both branches used to be one `status_code != 200` check that broke the
        pagination, so a permanent failure and a blip were treated alike.
        """
        crawler._fetch_page = AsyncMock(
            return_value=_failure(CrawlOutcome.PERMANENT_ERROR, "fetch_http_permanent", 404),
        )
        config = {"url": "https://lg.com/page={page}", "link_prefix": "https://lg.com"}

        with patch.object(crawler, "_raw_pages", []):
            result = await crawler._crawl_team("LG", config)

        assert result == []
        crawler._fetch_page.assert_awaited_once_with("https://lg.com/page=1")

    @mark.asyncio
    async def test_a_whole_sweep_of_failures_reads_nothing(self, crawler):
        crawler._fetch_page = AsyncMock(
            return_value=_failure(CrawlOutcome.RETRYABLE_ERROR, "fetch_http_error", 503),
        )
        config = {"url": "https://lg.com/page={page}", "link_prefix": "https://lg.com"}

        with patch.object(crawler, "_raw_pages", []):
            assert await crawler._crawl_team("LG", config) == []

        assert crawler._fetch_page.await_count == 3
        assert crawler._raw_pages == []


class TestTheTransportIsGoverned:
    def test_no_team_disables_certificate_verification(self):
        """Doosan's `verify_ssl: False` is gone, and must not come back.

        It was added when Doosan's public API served an incomplete TLS chain.
        The chain now validates under the bundle the shared client uses, so the
        override only opened this one team to a man in the middle -- the defect
        Phase 54 removed from `ticket_crawler`.

        Asserted on the config rather than on the transport, because the shared
        client has no `verify` argument at all: re-enabling TLS bypass here means
        composing a raw client again, and the transport axis would catch that.
        This checks the intent that survives the migration.
        """
        offenders = {
            code: config["verify_ssl"]
            for code, config in TEAM_NEWS_SOURCES.items()
            if config.get("verify_ssl") is False
        }

        assert offenders == {}, f"certificate verification disabled for {offenders}"

    def test_the_crawler_does_not_build_its_own_client(self):
        """The hand-rolled `httpx.AsyncClient` is what the migration removed."""
        source = inspect.getsource(event_module)

        assert "httpx.AsyncClient(" not in source
        assert "verify=" not in source
        assert isinstance(TeamEventCrawler()._http, CrawlerHttpClient)


class TestRun:
    @mark.asyncio
    async def test_iterates_all_teams(self, crawler):
        with patch.object(crawler, "_crawl_team", AsyncMock(return_value=[{"title": "event"}])):
            result = await crawler.run(save=False)
        assert len(result) == len(TEAM_NEWS_SOURCES)

    @mark.asyncio
    async def test_filters_by_team(self, crawler):
        with patch.object(crawler, "_crawl_team", AsyncMock(return_value=[{"title": "event"}])):
            result = await crawler.run(save=False, team_filter="LG")
        assert len(result) == 1

    @mark.asyncio
    async def test_saves_to_db(self, crawler):
        with (
            patch.object(crawler, "_crawl_team", AsyncMock(return_value=[{"title": "event"}])),
            patch.object(crawler, "_save_to_db") as mock_save,
        ):
            await crawler.run(save=True)
        mock_save.assert_called_once()


class TestSaveToDb:
    def test_saves_events_and_snapshots(self, crawler):
        with (
            patch("src.crawlers.team_event_crawler.SessionLocal") as mock_sl,
            patch("src.crawlers.team_event_crawler.save_raw_snapshots") as mock_snap,
            patch("src.crawlers.team_event_crawler.TeamEventRepository") as mock_repo_cls,
        ):
            mock_session = MagicMock()
            mock_sl.return_value.__enter__.return_value = mock_session
            mock_repo = MagicMock()
            mock_repo_cls.return_value = mock_repo
            mock_snap.return_value = 2

            crawler._raw_pages = [{"url": "test", "html": "<html/>"}]
            crawler._save_to_db([{"team_id": "LG", "title": "Event"}])

            mock_repo.save.assert_called_once()
            mock_session.commit.assert_called_once()
            assert crawler._raw_pages == []

    def test_rolls_back_on_snapshot_save_error(self, crawler):
        with (
            patch("src.crawlers.team_event_crawler.SessionLocal") as mock_sl,
            patch("src.crawlers.team_event_crawler.save_raw_snapshots", side_effect=RuntimeError("db unavailable")),
        ):
            mock_session = MagicMock()
            mock_sl.return_value.__enter__.return_value = mock_session
            crawler._save_to_db([{"title": "Event"}])

        mock_session.rollback.assert_called_once()
        assert crawler._raw_pages == []


class TestTheSnapshotCarriesTheParserOutcome:
    """``parsed_records`` has to be written where the drift check reads it.

    ``snapshot_drift_check`` re-parses a stored artifact and compares the record count
    against ``capture_metadata["parsed_records"]``. Without that number the row is
    reported as ``unknown_baseline`` and the drift is never measured -- measured
    2026-11-01: **0 of the most recent 100 snapshots had one**, and 76 of those 100
    came from this crawler. The check was asking a question no row could answer.

    The count belongs to the page it came from, so it is written onto that page's
    snapshot dict rather than once at the end: a two-page sweep has two artifacts
    with two different counts.
    """

    def _config(self) -> dict:
        return {"url": "https://lg.com/page={page}", "link_prefix": "https://lg.com"}

    @mark.asyncio
    async def test_the_count_lands_on_the_page_it_came_from(self, crawler):
        pages: list[dict] = []
        with (
            patch.object(crawler, "_raw_pages", pages),
            patch("src.crawlers.team_event_crawler.parse_team_events") as mock_parse,
        ):
            mock_parse.side_effect = [
                [{"team_id": "LG", "title": "A", "source_url": "https://lg.com/1"}],
                [
                    {"team_id": "LG", "title": "B", "source_url": "https://lg.com/2"},
                    {"team_id": "LG", "title": "C", "source_url": "https://lg.com/3"},
                ],
                [],
            ]
            crawler._fetch_page = AsyncMock(return_value=_ok("<html><div>event</div></html>"))

            await crawler._crawl_team("LG", self._config())

        counts = [page["capture_metadata"]["parsed_records"] for page in pages]
        # Three pages were fetched: two carried events and the third parsed to an
        # empty list, which is a real count (0) rather than a missing one -- the
        # sweep ends after it, but the artifact was captured and its parse happened.
        assert counts == [1, 2, 0]

    @mark.asyncio
    async def test_every_captured_page_gets_one(self, crawler):
        pages: list[dict] = []
        with (
            patch.object(crawler, "_raw_pages", pages),
            patch("src.crawlers.team_event_crawler.parse_team_events") as mock_parse,
        ):
            mock_parse.return_value = []
            crawler._fetch_page = AsyncMock(return_value=_ok("<html/>"))

            await crawler._crawl_team("LG", self._config())

        assert pages, "the page was captured"
        assert all("capture_metadata" in page for page in pages)

    @mark.asyncio
    async def test_a_parse_that_raises_records_no_count(self, crawler):
        """Absence is the honest record: there was no count to compare against."""
        pages: list[dict] = []
        with (
            patch.object(crawler, "_raw_pages", pages),
            patch("src.crawlers.team_event_crawler.parse_team_events", side_effect=ValueError("bad html")),
        ):
            crawler._fetch_page = AsyncMock(return_value=_ok("<html/>"))

            with pytest.raises(ValueError, match="bad html"):
                await crawler._crawl_team("LG", self._config())

        assert pages, "the artifact itself is still captured"
        assert "capture_metadata" not in pages[-1]

    @mark.asyncio
    async def test_a_failed_page_records_nothing(self, crawler):
        """A page that never arrived has no artifact to attach a baseline to."""
        pages: list[dict] = []
        with patch.object(crawler, "_raw_pages", pages):
            crawler._fetch_page = AsyncMock(
                return_value=_failure(CrawlOutcome.RETRYABLE_ERROR, "fetch_http_error", 503)
            )

            await crawler._crawl_team("LG", self._config())

        assert crawler._raw_pages == []


class TestTheScheduleTableWaitSignature:
    """``Page.wait_for_function`` takes the script argument as a keyword in this
    Playwright version, and passing it positionally raises ``TypeError`` — which
    ``SCHEDULE_CRAWLER_EXCEPTIONS`` swallows and turns into
    ``return False, "schedule_empty"``. The KBO schedule fallback therefore could
    never pass its month check: every run logged "[WARN] Schedule table wait
    failed" and came back empty, which is how the November 2026 Korean-Series
    schedule was never captured while the Naver api-gw outage was running.
    """

    def test_the_month_reaches_the_script_as_a_keyword_argument(self) -> None:
        from src.crawlers.schedule_crawler import ScheduleCrawler

        recorded: dict[str, object] = {}

        class _FakePage:
            async def wait_for_selector(self, selector: str, *, timeout: int) -> bool:
                return True

            async def wait_for_function(self, expression: str, **kwargs: object) -> None:
                recorded["expression"] = expression
                recorded["kwargs"] = kwargs

        crawler = ScheduleCrawler()
        import asyncio

        ok, reason = asyncio.run(crawler._wait_for_schedule_table(_FakePage(), expected_month=11))

        assert (ok, reason) == (True, "ok")
        assert recorded["kwargs"].get("arg") == "11"
        assert "timeout" in recorded["kwargs"]


class TestTheParseWindowTravelsWithTheCount:
    """The conditions a count was produced under have to be stored beside it.

    The drift check re-parses weeks later. Without the window the parser falls back
    to ``datetime.now(KST)`` and the parser's own ``CUTOFF_DAYS`` default (60) — the
    crawler uses ``days_back`` (30) — so a page whose events are all older than 30
    days re-parses as a different count and the gate calls the calendar drift.
    """

    def _config(self) -> dict:
        return {"url": "https://lg.com/page={page}", "link_prefix": "https://lg.com"}

    @mark.asyncio
    async def test_the_cutoff_days_are_recorded(self, crawler):
        pages: list[dict] = []
        with (
            patch.object(crawler, "_raw_pages", pages),
            patch("src.crawlers.team_event_crawler.parse_team_events", return_value=[]),
        ):
            crawler._fetch_page = AsyncMock(return_value=_ok("<html/>"))

            await crawler._crawl_team("LG", self._config())

        assert pages[0]["capture_metadata"]["cutoff_days"] == crawler.days_back

    @mark.asyncio
    async def test_the_fetch_time_is_recorded(self, crawler):
        pages: list[dict] = []
        with (
            patch.object(crawler, "_raw_pages", pages),
            patch("src.crawlers.team_event_crawler.parse_team_events", return_value=[]),
        ):
            crawler._fetch_page = AsyncMock(return_value=_ok("<html/>"))

            await crawler._crawl_team("LG", self._config())

        fetched = pages[0]["capture_metadata"]["fetched_at"]
        assert isinstance(fetched, str) and fetched, "the window's anchor is a timestamp, not a flag"

    @mark.asyncio
    async def test_the_stored_time_is_the_one_the_parser_was_given(self, crawler):
        """A different timestamp on the row than in the parse would anchor nothing."""
        pages: list[dict] = []
        seen: list[dict] = []

        def _capture(html, source_key, metadata):
            seen.append(dict(metadata))
            return []

        with (
            patch.object(crawler, "_raw_pages", pages),
            patch("src.crawlers.team_event_crawler.parse_team_events", side_effect=_capture),
        ):
            crawler._fetch_page = AsyncMock(return_value=_ok("<html/>"))

            await crawler._crawl_team("LG", self._config())

        assert pages[0]["capture_metadata"]["fetched_at"] == seen[0]["fetched_at"]
        assert pages[0]["capture_metadata"]["cutoff_days"] == seen[0]["cutoff_days"], (
            "the stored window must be the parsed window, not a second expression of it"
        )
