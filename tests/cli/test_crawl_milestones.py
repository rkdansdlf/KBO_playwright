"""Tests for crawl_milestones CLI entrypoint and guards."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import src.cli.collection.crawl_milestones as crawl_cli
from src.config.milestone_flags import MILESTONE_CRAWL_ENABLED_ENV


def test_crawl_milestones_refused_when_save_and_flag_off(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When --save is passed and flag is off, return cleanly without crawling or opening DB."""
    monkeypatch.delenv(MILESTONE_CRAWL_ENABLED_ENV, raising=False)

    def _exploding_crawler(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("MilestoneCrawler must not be instantiated when crawl is refused")

    def _exploding_db_session(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("get_db_session must not be called when crawl is refused")

    monkeypatch.setattr(crawl_cli, "MilestoneCrawler", _exploding_crawler)
    monkeypatch.setattr(crawl_cli, "get_db_session", _exploding_db_session)

    # main must return normally (no raise, exit 0)
    with caplog.at_level("WARNING"):
        crawl_cli.main(["--save", "--season", "2026"])

    assert "milestone crawl --save refused: MILESTONE_CRAWL_ENABLED is off" in caplog.text


def test_crawl_milestones_dry_run_allowed_when_flag_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dry run (without --save) still executes crawler even if flag is off."""
    monkeypatch.delenv(MILESTONE_CRAWL_ENABLED_ENV, raising=False)

    fake_crawler = MagicMock()
    fake_crawler.crawl_upcoming_milestones = AsyncMock(return_value=[])
    monkeypatch.setattr(crawl_cli, "MilestoneCrawler", lambda: fake_crawler)

    def _exploding_db_session(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("get_db_session must not be called in dry run")

    monkeypatch.setattr(crawl_cli, "get_db_session", _exploding_db_session)

    crawl_cli.main(["--season", "2026"])
    fake_crawler.crawl_upcoming_milestones.assert_awaited_once_with(season=2026)
