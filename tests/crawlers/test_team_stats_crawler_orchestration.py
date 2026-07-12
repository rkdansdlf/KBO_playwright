from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import src.crawlers.team_batting_stats_crawler as batting_module
import src.crawlers.team_pitching_stats_crawler as pitching_module
from src.crawlers.team_batting_stats_crawler import TeamBattingStatsCrawler
from src.crawlers.team_pitching_stats_crawler import TeamPitchingStatsCrawler


@pytest.mark.parametrize("crawler_cls", [TeamBattingStatsCrawler, TeamPitchingStatsCrawler])
def test_select_season_uses_later_selector_when_primary_is_missing(crawler_cls) -> None:
    page = MagicMock()
    page.query_selector.side_effect = [None, object()]

    assert crawler_cls._select_season(page, 2026) is True
    page.select_option.assert_called_once()
    page.wait_for_load_state.assert_called_once_with("networkidle")


@pytest.mark.parametrize(
    ("module", "crawler_cls", "parser_name"),
    [
        (batting_module, TeamBattingStatsCrawler, "parse_team_batting_html"),
        (pitching_module, TeamPitchingStatsCrawler, "parse_team_pitching_html"),
    ],
)
def test_collect_tries_fallback_url_after_empty_primary_result(monkeypatch, module, crawler_cls, parser_name) -> None:
    page = MagicMock()
    context = MagicMock()
    context.new_page.return_value = page
    browser = MagicMock()
    browser.new_context.return_value = context
    playwright = MagicMock()
    playwright.chromium.launch.return_value = browser
    manager = MagicMock()
    manager.__enter__.return_value = playwright
    manager.__exit__.return_value = False
    policy = MagicMock()
    policy.build_context_kwargs.return_value = {}
    policy.run_with_retry.side_effect = lambda operation, *args, **kwargs: operation(*args, **kwargs)
    parser = MagicMock(side_effect=[[], [{"team_id": "LG"}]])
    crawler = crawler_cls(policy=policy)
    monkeypatch.setattr(module, "sync_playwright", lambda: manager)
    monkeypatch.setattr(module, "install_sync_resource_blocking", MagicMock())
    monkeypatch.setattr(module, parser_name, parser)
    monkeypatch.setattr(crawler, "_select_season", MagicMock(return_value=True))

    result = crawler._collect_from_site(2026, {"LG": "LG"}, headless=True)

    assert result == [{"team_id": "LG"}]
    assert page.goto.call_count == 2
    context.close.assert_called_once()
    browser.close.assert_called_once()


@pytest.mark.parametrize(
    ("module", "crawler_cls"),
    [
        (batting_module, TeamBattingStatsCrawler),
        (pitching_module, TeamPitchingStatsCrawler),
    ],
)
def test_crawl_persists_site_rows_only_when_requested(monkeypatch, module, crawler_cls) -> None:
    crawler = crawler_cls()
    crawler._collect_from_site = MagicMock(return_value=[{"team_id": "LG"}])
    crawler.repo = MagicMock()
    monkeypatch.setattr(module, "get_team_mapping_for_year", MagicMock(return_value={"LG": "LG"}))

    persisted = crawler.crawl(2026, persist=True)
    preview = crawler.crawl(2026, persist=False)

    assert persisted == preview == [{"team_id": "LG"}]
    crawler.repo.upsert_many.assert_called_once_with(persisted)


@pytest.mark.parametrize(
    ("module", "crawler_cls", "aggregate_method"),
    [
        (batting_module, TeamBattingStatsCrawler, "aggregate_batting"),
        (pitching_module, TeamPitchingStatsCrawler, "aggregate_pitching"),
    ],
)
def test_crawl_uses_in_memory_aggregation_when_site_collection_fails(
    monkeypatch, module, crawler_cls, aggregate_method
) -> None:
    crawler = crawler_cls()
    crawler._collect_from_site = MagicMock(side_effect=RuntimeError("site unavailable"))
    session = MagicMock()
    session_local = MagicMock()
    session_local.return_value.__enter__.return_value = session
    aggregator = MagicMock()
    getattr(aggregator, aggregate_method).return_value = [{"team_id": "LG"}]
    standings_calculator = MagicMock()
    monkeypatch.setattr(module, "SessionLocal", session_local)
    monkeypatch.setattr(module, "TeamStatAggregator", MagicMock(return_value=aggregator))
    monkeypatch.setattr(module, "get_team_mapping_for_year", MagicMock(return_value={"LG Twins": "LG"}))
    monkeypatch.setattr("src.cli.calculate_standings.StandingsCalculator", standings_calculator)

    result = crawler.crawl(2026, persist=False)

    assert result == [{"team_id": "LG", "team_name": "LG Twins"}]
    getattr(aggregator, aggregate_method).assert_called_once()
    standings_calculator.return_value.calculate_year.assert_called_once_with(2026)
