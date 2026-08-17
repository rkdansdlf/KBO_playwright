"""Unit tests for scheduler jobs and backward compatibility re-exports."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import scripts.scheduler
from src.scheduler.jobs.daily import _compact_date, _from_compact_date, _to_compact_date
from src.scheduler.jobs.live import _pregame_preview_detail_has_starters
from src.scheduler.jobs.stadium import crawl_congestion_job, crawl_transit_time_job


def test_scripts_scheduler_reexports_all():
    assert hasattr(scripts.scheduler, "main")
    assert hasattr(scripts.scheduler, "crawl_daily_games")
    assert hasattr(scripts.scheduler, "crawl_live_refresh")
    assert hasattr(scripts.scheduler, "crawl_pregame_refresh")
    assert hasattr(scripts.scheduler, "LIVE_LOCK")
    assert hasattr(scripts.scheduler, "DAILY_LOCK")
    assert hasattr(scripts.scheduler, "MAINTENANCE_LOCK")
    assert hasattr(scripts.scheduler, "SQLITE_WRITE_LOCK")


def test_compact_date_helpers():
    assert _to_compact_date("20260401") == "20260401"
    d = _from_compact_date("20260401")
    assert d.year == 2026 and d.month == 4 and d.day == 1
    assert _compact_date(d) == "20260401"
    assert _compact_date("2026-04-01") == "20260401"


def test_pregame_preview_detail_has_starters():
    assert _pregame_preview_detail_has_starters(None) is False
    assert _pregame_preview_detail_has_starters("") is False
    assert _pregame_preview_detail_has_starters("invalid json") is False
    assert _pregame_preview_detail_has_starters('{"away_starter": "", "home_starter": ""}') is False
    assert _pregame_preview_detail_has_starters('{"away_starter": "Kim", "home_starter": "Lee"}') is True


def test_stadium_jobs_execution():
    with (
        patch("src.crawlers.transit_time_crawler.TransitTimeCrawler.run", new_callable=AsyncMock) as mock_transit,
        patch("src.scheduler.jobs.stadium._sqlite_writer_lock") as mock_lock,
    ):
        mock_lock.return_value.__enter__.return_value = True
        crawl_transit_time_job()
        mock_transit.assert_called_once_with(save=True)

    with (
        patch("src.crawlers.congestion_crawler.CongestionCrawler.run", new_callable=AsyncMock) as mock_congestion,
        patch("src.scheduler.jobs.stadium._sqlite_writer_lock") as mock_lock,
    ):
        mock_lock.return_value.__enter__.return_value = True
        crawl_congestion_job()
        mock_congestion.assert_called_once_with(save=True)
