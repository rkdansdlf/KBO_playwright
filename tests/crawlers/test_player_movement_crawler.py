from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest import mark

from src.crawlers.player_movement_crawler import PlayerMovementCrawler
from src.crawlers.player_movement_outcome import (
    CONTROLS_MISSING_REASON,
    PlayerMovementPageRead,
    PlayerMovementStatus,
)


@pytest.fixture
def crawler():
    return PlayerMovementCrawler()


@pytest.fixture(autouse=True)
def allow_kbo_source(monkeypatch):
    monkeypatch.setattr("src.crawlers.player_movement_crawler.compliance.is_allowed", AsyncMock(return_value=True))


def _read(*rows: dict) -> PlayerMovementPageRead:
    """A page that carried rows."""
    return PlayerMovementPageRead(status=PlayerMovementStatus.SUCCESS, rows=list(rows))


def _quiet_read() -> PlayerMovementPageRead:
    """A page that was readable and carried none."""
    return PlayerMovementPageRead(status=PlayerMovementStatus.EMPTY)


class TestExtractTable:
    """A table that is absent and a table that is empty are different facts.

    Both used to arrive as `[]`, which made a drifted page indistinguishable from
    a year that recorded no transfers. The read is now typed so the difference
    survives all the way to the dead letter queue.
    """

    @mark.asyncio
    async def test_a_missing_table_is_drift(self, crawler):
        mock_page = MagicMock()
        mock_page.evaluate = AsyncMock(return_value={"table_present": False, "rows": []})

        read = await crawler._extract_table(mock_page)

        assert read.status is PlayerMovementStatus.SCHEMA_CHANGED
        assert read.reason == CONTROLS_MISSING_REASON
        assert read.rows == []

    @mark.asyncio
    async def test_drift_is_terminal(self, crawler):
        mock_page = MagicMock()
        mock_page.evaluate = AsyncMock(return_value={"table_present": False, "rows": []})

        read = await crawler._extract_table(mock_page)

        # Retryable and drift are different failures: retrying a page whose
        # table is gone returns the same page, and spends the budget that would
        # have cleared a genuine outage.
        assert read.is_terminal is True

    @mark.asyncio
    async def test_a_present_but_empty_table_is_quiet(self, crawler):
        mock_page = MagicMock()
        mock_page.evaluate = AsyncMock(return_value={"table_present": True, "rows": []})

        read = await crawler._extract_table(mock_page)

        assert read.status is PlayerMovementStatus.EMPTY
        assert read.rows == []

    @mark.asyncio
    async def test_filters_empty_date_rows(self, crawler):
        mock_page = MagicMock()
        mock_page.evaluate = AsyncMock(
            return_value={
                "table_present": True,
                "rows": [
                    {"date": "2024-03-15", "section": "Trade", "team_code": "LG", "player_name": "Kim", "remarks": ""},
                    {"date": "", "section": "Trade", "team_code": "SS", "player_name": "Park", "remarks": ""},
                    {"date": "2024-04-01", "section": "", "team_code": "NC", "player_name": "Lee", "remarks": ""},
                ],
            },
        )

        read = await crawler._extract_table(mock_page)

        # Two rows were dropped for missing key fields, so the remaining one is
        # a real result and not an empty page.
        assert read.status is PlayerMovementStatus.SUCCESS
        assert len(read.rows) == 1
        assert read.rows[0]["player_name"] == "Kim"

    @mark.asyncio
    async def test_returns_valid_data(self, crawler):
        mock_page = MagicMock()
        mock_page.evaluate = AsyncMock(
            return_value={
                "table_present": True,
                "rows": [
                    {
                        "date": "2024-03-15",
                        "section": "Trade",
                        "team_code": "LG",
                        "player_name": "Kim",
                        "remarks": "cash",
                    },
                    {"date": "2024-04-01", "section": "FA", "team_code": "SS", "player_name": "Park", "remarks": ""},
                ],
            },
        )

        read = await crawler._extract_table(mock_page)

        assert read.status is PlayerMovementStatus.SUCCESS
        assert len(read.rows) == 2


class TestCrawlYear:
    @mark.asyncio
    @patch("src.crawlers.player_movement_crawler.AsyncRetrying")
    async def test_calls_extract_table_and_paginates(self, mock_retrying_cls, crawler):
        mock_page = MagicMock()
        mock_page.goto = AsyncMock()
        mock_page.select_option = AsyncMock()
        mock_page.click = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html>movement</html>")

        mock_retrying = MagicMock()
        mock_retrying.__aiter__.return_value = [MagicMock()]
        mock_retrying_cls.return_value = mock_retrying

        crawler._extract_table = AsyncMock(
            side_effect=[
                _read(
                    {"date": "2024-03-15", "section": "Trade", "team_code": "LG", "player_name": "Kim", "remarks": ""},
                ),
                _quiet_read(),
            ],
        )
        mock_page.get_by_role.return_value.count = AsyncMock(return_value=0)
        mock_page.locator.return_value.count = AsyncMock(return_value=0)

        result = await crawler._crawl_year(mock_page, 2024)

        assert len(result) == 1
        mock_page.select_option.assert_called_with("#selYear", "2024")
        mock_page.click.assert_called_with("#btnSearch")


class TestCrawlYears:
    @mark.asyncio
    @patch("src.crawlers.player_movement_crawler.AsyncPlaywrightPool")
    async def test_crawls_year_range(self, mock_pool_cls, crawler):
        mock_pool = MagicMock()
        mock_pool_cls.return_value = mock_pool
        mock_pool.start = AsyncMock()
        mock_pool.release = AsyncMock()
        mock_pool.close = AsyncMock()
        mock_page = MagicMock()
        mock_pool.acquire = AsyncMock(return_value=mock_page)
        mock_page.goto = AsyncMock()
        mock_page.select_option = AsyncMock()
        mock_page.click = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html>movement</html>")

        crawler._extract_table = AsyncMock(
            side_effect=[
                _read({"date": "2023-01-01", "section": "Trade", "team_code": "LG", "player_name": "A", "remarks": ""}),
                _read({"date": "2024-01-01", "section": "FA", "team_code": "SS", "player_name": "B", "remarks": ""}),
            ],
        )
        mock_page.get_by_role.return_value.count = AsyncMock(return_value=0)
        mock_page.locator.return_value.count = AsyncMock(return_value=0)

        result = await crawler.crawl_years(2023, 2024)

        assert len(result) == 2
        mock_pool.close.assert_awaited_once()

    @mark.asyncio
    @patch("src.crawlers.player_movement_crawler.AsyncPlaywrightPool")
    async def test_cleans_up_pool_on_exception(self, mock_pool_cls, crawler):
        mock_pool = MagicMock()
        mock_pool_cls.return_value = mock_pool
        mock_pool.start = AsyncMock()
        mock_pool.release = AsyncMock()
        mock_pool.close = AsyncMock()
        mock_page = MagicMock()
        mock_pool.acquire = AsyncMock(return_value=mock_page)
        mock_page.goto = AsyncMock()
        mock_page.select_option = AsyncMock()
        mock_page.click = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html>movement</html>")

        crawler._extract_table = AsyncMock(side_effect=RuntimeError("boom"))

        result = await crawler.crawl_years(2023, 2023)

        assert result == []
        mock_pool.close.assert_awaited_once()

    @mark.asyncio
    @patch("src.crawlers.player_movement_crawler.save_raw_snapshots", return_value=1)
    @patch("src.crawlers.player_movement_crawler.SessionLocal")
    @patch("src.crawlers.player_movement_crawler.AsyncPlaywrightPool")
    async def test_save_snapshots_tracks_kbo_player_movement_source(
        self,
        mock_pool_cls,
        mock_session_cls,
        mock_save_raw_snapshots,
        crawler,
    ):
        mock_pool = MagicMock()
        mock_pool_cls.return_value = mock_pool
        mock_pool.start = AsyncMock()
        mock_pool.release = AsyncMock()
        mock_pool.close = AsyncMock()
        mock_page = MagicMock()
        mock_pool.acquire = AsyncMock(return_value=mock_page)
        mock_page.goto = AsyncMock()
        mock_page.select_option = AsyncMock()
        mock_page.click = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html>movement</html>")
        mock_page.get_by_role.return_value.count = AsyncMock(return_value=0)
        mock_page.locator.return_value.count = AsyncMock(return_value=0)
        crawler._extract_table = AsyncMock(return_value=_quiet_read())

        mock_session = MagicMock()
        mock_session_cls.return_value.__enter__.return_value = mock_session

        await crawler.crawl_years(2025, 2025, save_snapshots=True)

        pages = mock_save_raw_snapshots.call_args.args[1]
        assert pages[0]["source_key"] == "kbo_player_movement"
        assert pages[0]["status_code"] == 200
        assert len(pages) == 2
        mock_session.commit.assert_called_once()


class TestYearFailureCapture:
    @mark.asyncio
    @patch("src.crawlers.player_movement_crawler.AsyncPlaywrightPool")
    async def test_every_failed_year_is_kept_for_the_ledger(self, mock_pool_cls, crawler):
        """A swallowed year failure must still be recoverable by the run ledger."""
        mock_pool = MagicMock()
        mock_pool_cls.return_value = mock_pool
        mock_pool.start = AsyncMock()
        mock_pool.release = AsyncMock()
        mock_pool.close = AsyncMock()
        mock_page = MagicMock()
        mock_pool.acquire = AsyncMock(return_value=mock_page)
        mock_page.goto = AsyncMock()
        mock_page.select_option = AsyncMock()
        mock_page.click = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html>movement</html>")

        crawler._extract_table = AsyncMock(side_effect=RuntimeError("boom"))

        result = await crawler.crawl_years(2023, 2024)

        assert result == []
        assert [year for year, _ in crawler._year_failures] == [2023, 2024]
        assert all(isinstance(exc, RuntimeError) for _, exc in crawler._year_failures)

    @mark.asyncio
    @patch("src.crawlers.player_movement_crawler.AsyncPlaywrightPool")
    async def test_a_fresh_call_does_not_accumulate_stale_failures(self, mock_pool_cls, crawler):
        mock_pool = MagicMock()
        mock_pool_cls.return_value = mock_pool
        mock_pool.start = AsyncMock()
        mock_pool.release = AsyncMock()
        mock_pool.close = AsyncMock()
        mock_page = MagicMock()
        mock_pool.acquire = AsyncMock(return_value=mock_page)
        mock_page.goto = AsyncMock()
        mock_page.select_option = AsyncMock()
        mock_page.click = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html>movement</html>")
        mock_page.get_by_role.return_value.count = AsyncMock(return_value=0)
        mock_page.locator.return_value.count = AsyncMock(return_value=0)

        crawler._extract_table = AsyncMock(side_effect=RuntimeError("boom"))
        await crawler.crawl_years(2023, 2023)
        assert len(crawler._year_failures) == 1

        crawler._extract_table = AsyncMock(return_value=_quiet_read())
        await crawler.crawl_years(2024, 2024)

        assert crawler._year_failures == []


def _page() -> MagicMock:
    """A page stub that satisfies everything `_crawl_year` awaits."""
    page = MagicMock()
    page.goto = AsyncMock()
    page.select_option = AsyncMock()
    page.click = AsyncMock()
    page.wait_for_load_state = AsyncMock()
    page.wait_for_timeout = AsyncMock()
    page.content = AsyncMock(return_value="<html>movement</html>")
    page.get_by_role.return_value.count = AsyncMock(return_value=0)
    page.locator.return_value.count = AsyncMock(return_value=0)
    return page


def _drifted() -> PlayerMovementPageRead:
    """A page that answered but is no longer the document the crawl asks for."""
    return PlayerMovementPageRead(status=PlayerMovementStatus.SCHEMA_CHANGED, reason=CONTROLS_MISSING_REASON)


class TestADriftedYearIsRecordedAsDrift:
    """A page that lost its table is not a year that recorded nothing.

    The distinction is the whole point of typing the read. Handled as an empty
    result, a site change looks like a quiet year for every year of the sweep and
    the ledger stays green; handled as drift, the year is queued under a terminal
    code so a retry does not spend its budget re-reading the same broken page.
    """

    @mark.asyncio
    async def test_a_drifted_year_raises_nothing(self, crawler):
        crawler._extract_table = AsyncMock(return_value=_drifted())

        rows = await crawler._crawl_year(_page(), 2024)

        assert rows == []
        # Nothing was raised, so nothing belongs in the failure list -- which is
        # exactly why the typed read is held separately from it.
        assert crawler._year_failures == []

    @mark.asyncio
    async def test_and_it_is_terminal_rather_than_retryable(self, crawler):
        crawler._extract_table = AsyncMock(return_value=_drifted())

        await crawler._crawl_year(_page(), 2024)

        assert crawler._year_reads[2024].is_terminal is True

    @mark.asyncio
    async def test_paging_stops_at_the_first_drifted_page(self, crawler):
        crawler._extract_table = AsyncMock(return_value=_drifted())

        await crawler._crawl_year(_page(), 2024)

        # One call, not the pagination loop: paging on would walk the same wrong
        # document for every remaining page.
        assert crawler._extract_table.await_count == 1

    @mark.asyncio
    async def test_a_quiet_year_records_nothing_to_replay(self, crawler):
        crawler._extract_table = AsyncMock(return_value=_quiet_read())

        await crawler._crawl_year(_page(), 2024)

        assert crawler._year_failures == []
        assert crawler._year_reads == {}
