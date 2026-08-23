from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

import src.crawlers.baserunning_stats_crawler as baserunning_module
import src.crawlers.fielding_stats_crawler as fielding_module
import src.crawlers.team_batting_stats_crawler as batting_module
import src.crawlers.team_pitching_stats_crawler as pitching_module
from src.crawlers.baserunning_stats_crawler import crawl_baserunning_stats
from src.crawlers.fielding_stats_crawler import crawl_all_fielding_stats
from src.crawlers.team_batting_stats_crawler import TeamBattingStatsCrawler
from src.crawlers.team_pitching_stats_crawler import TeamPitchingStatsCrawler


def test_team_batting_site_collection_is_source_limited_before_browser_start() -> None:
    crawler = TeamBattingStatsCrawler()
    with (
        patch.object(batting_module.compliance, "is_allowed_sync", return_value=False),
        patch.object(batting_module, "sync_playwright") as playwright,
    ):
        result = crawler._collect_from_site(2026, {"LG": "LG"}, headless=True)

    assert result == []
    playwright.assert_not_called()


def test_team_batting_public_crawl_does_not_use_db_fallback_when_source_is_blocked() -> None:
    crawler = TeamBattingStatsCrawler()
    with (
        patch.object(batting_module, "get_team_mapping_for_year", return_value={"LG": "LG"}),
        patch.object(batting_module.compliance, "is_allowed_sync", return_value=False),
        patch.object(batting_module, "SessionLocal") as session_local,
    ):
        result = crawler.crawl(2026, persist=False)

    assert result == []
    assert crawler.get_last_failure_reason() == "kbo_robots_blocked"
    session_local.assert_not_called()


def test_team_pitching_site_collection_is_source_limited_before_browser_start() -> None:
    crawler = TeamPitchingStatsCrawler()
    with (
        patch.object(pitching_module.compliance, "is_allowed_sync", return_value=False),
        patch.object(pitching_module, "sync_playwright") as playwright,
    ):
        result = crawler._collect_from_site(2026, {"LG": "LG"}, headless=True)

    assert result == []
    playwright.assert_not_called()


def test_team_pitching_public_crawl_does_not_use_db_fallback_when_source_is_blocked() -> None:
    crawler = TeamPitchingStatsCrawler()
    with (
        patch.object(pitching_module, "get_team_mapping_for_year", return_value={"LG": "LG"}),
        patch.object(pitching_module.compliance, "is_allowed_sync", return_value=False),
        patch.object(pitching_module, "SessionLocal") as session_local,
    ):
        result = crawler.crawl(2026, persist=False)

    assert result == []
    assert crawler.get_last_failure_reason() == "kbo_robots_blocked"
    session_local.assert_not_called()


def test_fielding_is_source_limited_before_browser_start() -> None:
    with (
        patch.object(fielding_module.compliance, "is_allowed_sync", return_value=False),
        patch.object(fielding_module, "sync_playwright") as playwright,
    ):
        result = crawl_all_fielding_stats(2026)

    assert result == []
    playwright.assert_not_called()


def test_baserunning_is_source_limited_before_browser_start() -> None:
    with (
        patch.object(baserunning_module.compliance, "is_allowed_sync", return_value=False),
        patch.object(baserunning_module, "sync_playwright") as playwright,
    ):
        result = crawl_baserunning_stats(2026)

    assert result == []
    playwright.assert_not_called()


@pytest.mark.asyncio
async def test_player_movement_is_source_limited_before_pool_start() -> None:
    from src.crawlers.player_movement_crawler import PlayerMovementCrawler

    crawler = PlayerMovementCrawler()
    with (
        patch("src.crawlers.player_movement_crawler.compliance.is_allowed", new=AsyncMock(return_value=False)),
        patch("src.crawlers.player_movement_crawler.AsyncPlaywrightPool") as pool,
    ):
        result = await crawler.crawl_years(2025, 2026)

    assert result == []
    assert crawler.get_last_failure_reason() == "kbo_robots_blocked"
    pool.assert_not_called()


@pytest.mark.asyncio
async def test_team_history_is_source_limited_before_browser_start() -> None:
    from src.crawlers.team_history_crawler import TeamHistoryCrawler

    crawler = TeamHistoryCrawler()
    with (
        patch("src.crawlers.team_history_crawler.compliance.is_allowed", new=AsyncMock(return_value=False)),
        patch("src.crawlers.team_history_crawler.async_playwright") as playwright,
    ):
        result = await crawler.crawl()

    assert result == []
    assert crawler.get_last_failure_reason() == "kbo_robots_blocked"
    playwright.assert_not_called()


@pytest.mark.asyncio
async def test_ticket_map_is_source_limited_before_http_request() -> None:
    from src.crawlers.ticket_crawler import TicketCrawler

    crawler = TicketCrawler()
    with (
        patch("src.crawlers.ticket_crawler.compliance.is_allowed", new=AsyncMock(return_value=False)),
        patch("src.crawlers.ticket_crawler.httpx.AsyncClient") as client,
    ):
        result = await crawler._crawl_kbo_ticket_map()

    assert result == []
    assert crawler.get_last_failure_reason() == "kbo_robots_blocked"
    client.assert_not_called()


@pytest.mark.asyncio
async def test_futures_schedule_is_source_limited_before_pool_start() -> None:
    from src.crawlers.futures_schedule_crawler import FuturesScheduleCrawler

    with (
        patch("src.crawlers.futures_schedule_crawler.compliance.is_allowed", new=AsyncMock(return_value=False)),
        patch("src.crawlers.futures_schedule_crawler.AsyncPlaywrightPool") as pool,
    ):
        result = await FuturesScheduleCrawler().crawl_futures_schedule(2026, 8)

    assert result == []
    pool.assert_not_called()


@pytest.mark.asyncio
async def test_draft_history_is_source_limited_before_pool_start() -> None:
    from src.crawlers.draft_history_crawler import DraftHistoryCrawler

    with (
        patch("src.crawlers.draft_history_crawler.compliance.is_allowed", new=AsyncMock(return_value=False)),
        patch("src.crawlers.draft_history_crawler.AsyncPlaywrightPool") as pool,
    ):
        result = await DraftHistoryCrawler().crawl_draft_history(2026)

    assert result == []
    pool.assert_not_called()


@pytest.mark.asyncio
async def test_milestones_are_source_limited_before_pool_start() -> None:
    from src.crawlers.milestone_crawler import MilestoneCrawler

    with (
        patch("src.crawlers.milestone_crawler.compliance.is_allowed", new=AsyncMock(return_value=False)),
        patch("src.crawlers.milestone_crawler.AsyncPlaywrightPool") as pool,
    ):
        result = await MilestoneCrawler().crawl_upcoming_milestones(2026)

    assert result == []
    pool.assert_not_called()


@pytest.mark.asyncio
async def test_player_splits_are_source_limited_before_pool_start() -> None:
    from src.crawlers.player_splits_crawler import PlayerSplitsCrawler

    with (
        patch("src.crawlers.player_splits_crawler.compliance.is_allowed", new=AsyncMock(return_value=False)),
        patch("src.crawlers.player_splits_crawler.AsyncPlaywrightPool") as pool,
    ):
        result = await PlayerSplitsCrawler().crawl_player_splits(2026)

    assert result == []
    pool.assert_not_called()


@pytest.mark.asyncio
async def test_press_releases_are_source_limited_before_pool_start() -> None:
    from src.crawlers.press_release_crawler import PressReleaseCrawler

    with (
        patch("src.crawlers.press_release_crawler.compliance.is_allowed", new=AsyncMock(return_value=False)),
        patch("src.crawlers.base.AsyncPlaywrightPool") as pool,
    ):
        result = await PressReleaseCrawler().crawl_press_releases()

    assert result == []
    pool.assert_not_called()


@pytest.mark.asyncio
async def test_daily_roster_is_source_limited_before_pool_start() -> None:
    from src.crawlers.daily_roster_crawler import DailyRosterCrawler

    with patch("src.crawlers.daily_roster_crawler.compliance.is_allowed", new=AsyncMock(return_value=False)):
        result = await DailyRosterCrawler().crawl_date_range("2026-08-01", "2026-08-01")

    assert result == []


@pytest.mark.asyncio
async def test_staff_register_is_source_limited_before_browser_start() -> None:
    from src.crawlers.staff_register_crawler import StaffRegisterCrawler

    with (
        patch("src.crawlers.staff_register_crawler.compliance.is_allowed", new=AsyncMock(return_value=False)),
        patch("src.crawlers.staff_register_crawler.async_playwright") as playwright,
    ):
        result = await StaffRegisterCrawler().crawl_all_teams()

    assert result == []
    playwright.assert_not_called()


@pytest.mark.asyncio
async def test_roster_transaction_is_source_limited_before_http_request() -> None:
    from src.crawlers.roster_transaction_crawler import RosterTransactionCrawler

    with (
        patch("src.crawlers.roster_transaction_crawler.compliance.is_allowed", new=AsyncMock(return_value=False)),
        patch("src.crawlers.roster_transaction_crawler.httpx.AsyncClient") as client,
    ):
        result = await RosterTransactionCrawler().run(target_date="2026-08-01")

    assert result == []
    client.assert_not_called()


@pytest.mark.asyncio
async def test_kbo_events_are_source_limited_before_pool_start() -> None:
    from src.crawlers.kbo_event_crawler import KboEventCrawler

    with (
        patch("src.crawlers.kbo_event_crawler.compliance.is_allowed", new=AsyncMock(return_value=False)),
        patch("src.crawlers.kbo_event_crawler.AsyncPlaywrightPool") as pool,
    ):
        result = await KboEventCrawler().run()

    assert result == []
    pool.assert_not_called()


@pytest.mark.asyncio
async def test_team_info_is_source_limited_before_browser_start() -> None:
    from src.crawlers.team_info_crawler import TeamInfoCrawler

    with (
        patch("src.crawlers.team_info_crawler.compliance.is_allowed", new=AsyncMock(return_value=False)),
        patch("src.crawlers.team_info_crawler.async_playwright") as playwright,
    ):
        result = await TeamInfoCrawler().crawl()

    assert result == []
    playwright.assert_not_called()
