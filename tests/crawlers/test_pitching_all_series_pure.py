from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.crawlers.player_pitching_all_series_crawler import (
    PitcherStats,
    Basic2AdditionalContext,
    Basic2PageContext,
    PitcherBasic1Context,
    _collect_pitcher_basic2_additional,
    _collect_pitcher_basic1_loop,
    _get_pitcher_team_options,
    _select_pitcher_team_if_needed,
    _apply_sort_by_label,
    _extract_basic2_row_info,
    _map_pitcher_basic1_stats,
    _update_pitcher_basic2_stats,
    _apply_sort_by_code,
    build_pitching_crawl_summary,
    extract_player_id,
    normalize_header,
    parse_basic1_page,
    parse_basic2_page,
    crawl_pitcher_series,
    go_to_next_page,
    setup_pitcher_page,
)


class TestNormalizeHeader:
    def test_basic(self):
        assert normalize_header("ERA") == "ERA"

    def test_none(self):
        assert normalize_header(None) == ""

    def test_whitespace(self):
        assert normalize_header("  ERA  ") == "ERA"

    def test_newline(self):
        assert normalize_header("ERA\n2.50") == "ERA"

    def test_multiple_words(self):
        assert normalize_header("Home Runs") == "Home"

    def test_nbsp(self):
        assert normalize_header("ERA\xa0") == "ERA"

    def test_korean(self):
        assert normalize_header("평균자책") == "평균자책"

    def test_empty(self):
        assert normalize_header("") == ""


class TestExtractPlayerId:
    def test_basic(self):
        assert extract_player_id("/Player.aspx?playerId=12345") == 12345

    def test_with_extra_params(self):
        assert extract_player_id("/Player.aspx?playerId=67890&season=2023") == 67890

    def test_none(self):
        assert extract_player_id(None) is None

    def test_empty(self):
        assert extract_player_id("") is None

    def test_no_player_id(self):
        assert extract_player_id("/Player.aspx?other=value") is None

    def test_large_id(self):
        assert extract_player_id("?playerId=999999") == 999999


class TestExtractBasic2RowInfo:
    def test_fast_path_valid(self):
        row = {"cells": ["1", "홍길동", "LG", "0.250"], "linkHref": "/Player.aspx?playerId=12345"}
        header_index = {"순위": 0, "선수명": 1, "팀": 2, "타율": 3}
        result = _extract_basic2_row_info(row, header_index, use_fast=True)
        assert result is not None
        pid, cell_fn = result
        assert pid == 12345
        assert callable(cell_fn)

    def test_fast_path_cell_fn_access(self):
        row = {"cells": ["1", "홍길동", "LG", "0.250"], "linkHref": "/Player.aspx?playerId=12345"}
        header_index = {"순위": 0, "선수명": 1, "팀": 2, "타율": 3}
        _, cell_fn = _extract_basic2_row_info(row, header_index, use_fast=True)
        assert cell_fn(0) == "1"
        assert cell_fn(1) == "홍길동"
        assert cell_fn(5) is None

    def test_fast_path_too_few_cells(self):
        row = {"cells": ["1", "홍길동"], "linkHref": "/Player.aspx?playerId=12345"}
        header_index = {"순위": 0, "선수명": 1, "팀": 2, "타율": 3}
        result = _extract_basic2_row_info(row, header_index, use_fast=True)
        assert result is None

    def test_fast_path_no_player_id(self):
        row = {"cells": ["1", "홍길동", "LG", "0.250"], "linkHref": "/Player.aspx?other=value"}
        header_index = {"순위": 0, "선수명": 1, "팀": 2, "타율": 3}
        result = _extract_basic2_row_info(row, header_index, use_fast=True)
        assert result is None

    def test_fast_path_no_link_href(self):
        row = {"cells": ["1", "홍길동", "LG", "0.250"]}
        header_index = {"순위": 0, "선수명": 1, "팀": 2, "타율": 3}
        result = _extract_basic2_row_info(row, header_index, use_fast=True)
        assert result is None


class TestUpdatePitcherBasic2Stats:
    def test_complete_games(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"CG": 0}
        cell_text_fn = lambda idx: "5"
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "sort1")
        assert stats.extra_stats["metrics"]["complete_games"] == 5

    def test_shutouts(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"SHO": 0}
        cell_text_fn = lambda idx: "2"
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "sort1")
        assert stats.extra_stats["metrics"]["shutouts"] == 2

    def test_avg_against(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"AVG": 0}
        cell_text_fn = lambda idx: "0.250"
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "sort1")
        assert stats.extra_stats["metrics"]["avg_against"] == 0.25

    def test_wild_pitches(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"WP": 0}
        cell_text_fn = lambda idx: "3"
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "sort1")
        assert stats.wild_pitches == 3

    def test_intentional_walks(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"IBB": 0}
        cell_text_fn = lambda idx: "2"
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "sort1")
        assert stats.intentional_walks == 2

    def test_balks(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"BK": 0}
        cell_text_fn = lambda idx: "1"
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "sort1")
        assert stats.balks == 1

    def test_ranking(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"순위": 0}
        cell_text_fn = lambda idx: "5"
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "era")
        assert stats.extra_stats["rankings"]["era"] == 5

    def test_none_value_skipped(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"CG": 0}
        cell_text_fn = lambda idx: None
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "sort1")
        assert "complete_games" not in stats.extra_stats.get("metrics", {})

    def test_multiple_metrics(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"CG": 0, "SHO": 1, "TBF": 2}
        cell_text_fn = lambda idx: ["5", "2", "100"][idx]
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "sort1")
        assert stats.extra_stats["metrics"]["complete_games"] == 5
        assert stats.extra_stats["metrics"]["shutouts"] == 2
        assert stats.extra_stats["metrics"]["tbf"] == 100


class TestMapPitcherBasic1Stats:
    def test_basic_mapping(self):
        row = {
            "player_id": 12345,
            "player_name": "홍길동",
            "team_name": "LG",
            "raw": {"G": "25", "W": "10", "L": "5", "ERA": "3.50"},
        }
        pitchers = {}
        result = _map_pitcher_basic1_stats(row, 2023, "REGULAR", pitchers, None)
        assert result is True
        assert 12345 in pitchers
        stats = pitchers[12345]
        assert stats.player_name == "홍길동"
        assert stats.games == 25
        assert stats.wins == 10
        assert stats.losses == 5
        assert stats.era == 3.5

    def test_max_players_limit(self):
        row = {
            "player_id": 99999,
            "player_name": "New Player",
            "team_name": "LG",
            "raw": {"G": "10"},
        }
        pitchers = {1: PitcherStats(player_id=1, season=2023, league="REGULAR")}
        result = _map_pitcher_basic1_stats(row, 2023, "REGULAR", pitchers, max_players=1)
        assert result is False
        assert 99999 not in pitchers

    def test_existing_player_updated(self):
        row = {
            "player_id": 12345,
            "player_name": "홍길동",
            "team_name": "LG",
            "raw": {"G": "30", "SO": "150"},
        }
        pitchers = {12345: PitcherStats(player_id=12345, season=2023, league="REGULAR", games=25)}
        result = _map_pitcher_basic1_stats(row, 2023, "REGULAR", pitchers, None)
        assert result is True
        assert pitchers[12345].games == 30
        assert pitchers[12345].strikeouts == 150

    @patch("src.crawlers.player_pitching_all_series_crawler.resolve_team_code")
    def test_team_code_resolution(self, mock_resolve):
        mock_resolve.return_value = "LG"
        row = {
            "player_id": 12345,
            "player_name": "홍길동",
            "team_name": "LG Twins",
            "raw": {"G": "10"},
        }
        pitchers = {}
        _map_pitcher_basic1_stats(row, 2023, "REGULAR", pitchers, None)
        assert pitchers[12345].team_code == "LG"

    def test_innings_pitched(self):
        row = {
            "player_id": 12345,
            "player_name": "홍길동",
            "team_name": "LG",
            "raw": {"IP": "50.1"},
        }
        pitchers = {}
        _map_pitcher_basic1_stats(row, 2023, "REGULAR", pitchers, None)
        assert pitchers[12345].innings_outs is not None

    def test_missing_fields_preserved(self):
        row = {
            "player_id": 12345,
            "player_name": "홍길동",
            "team_name": "LG",
            "raw": {"G": "10"},
        }
        pitchers = {12345: PitcherStats(player_id=12345, season=2023, league="REGULAR", wins=5)}
        _map_pitcher_basic1_stats(row, 2023, "REGULAR", pitchers, None)
        assert pitchers[12345].wins == 5


class TestBuildPitchingCrawlSummary:
    def test_all_valid(self):
        stats_list = [
            PitcherStats(
                player_id=1,
                season=2023,
                league="REGULAR",
                player_name="A",
                team_code="LG",
                games=25,
                wins=10,
            ),
            PitcherStats(player_id=2, season=2023, league="REGULAR", player_name="B", team_code="SS", games=20, wins=8),
        ]
        summary, valid = build_pitching_crawl_summary(stats_list)
        assert summary["processed_rows"] == 2
        assert summary["valid_rows"] == 2
        assert len(valid) == 2

    def test_empty_list(self):
        summary, valid = build_pitching_crawl_summary([])
        assert summary["processed_rows"] == 0
        assert summary["valid_rows"] == 0
        assert len(valid) == 0


class TestPitcherStats:
    def test_default_values(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        assert stats.level == "KBO1"
        assert stats.source == "CRAWLER"
        assert stats.games is None
        assert stats.extra_stats == {"rankings": {}}

    def test_to_repository_payload(self):
        stats = PitcherStats(
            player_id=1,
            season=2023,
            league="REGULAR",
            player_name="홍길동",
            team_code="LG",
            games=25,
            wins=10,
        )
        payload = stats.to_repository_payload()
        assert payload["player_id"] == 1
        assert payload["player_name"] == "홍길동"
        assert payload["season"] == 2023
        assert payload["games"] == 25
        assert payload["wins"] == 10

    def test_to_repository_payload_with_innings_outs(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR", innings_outs=150)
        payload = stats.to_repository_payload()
        assert payload["innings_outs"] == 150
        assert payload["extra_stats"]["innings_outs"] == 150


class TestPitchingPageParsers:
    def test_parse_basic1_page_maps_extracted_rows(self):
        page = MagicMock()
        headers = ["순위", "선수명", "팀명", "IP", "G", "ERA", "W", "SO"]
        rows = [
            {
                "player_id": 123,
                "player_name": "홍길동",
                "team_name": "LG",
                "raw": {
                    "순위": "1",
                    "IP": "10.2",
                    "G": "5",
                    "ERA": "2.53",
                    "W": "2",
                    "SO": "12",
                },
            },
        ]
        page.evaluate.side_effect = [headers, rows]
        pitchers = {}

        with patch("src.crawlers.player_pitching_all_series_crawler.get_team_mapping_for_year"):
            processed = parse_basic1_page(page, 2025, "REGULAR", pitchers)

        assert processed == 1
        stats = pitchers[123]
        assert stats.team_code == "LG"
        assert stats.innings_outs == 32
        assert stats.wins == 2
        assert stats.strikeouts == 12
        assert stats.extra_stats["rankings"]["basic1"] == 1

    def test_parse_basic1_page_returns_zero_when_headers_are_invalid(self):
        page = MagicMock()
        page.evaluate.return_value = ["선수명", "팀명"]

        processed = parse_basic1_page(page, 2025, "REGULAR", {})

        assert processed == 0

    def test_parse_basic2_page_uses_slow_dom_path_when_fast_parse_is_disabled(self, monkeypatch):
        class Cell:
            def __init__(self, text, href=None):
                self.text = text
                self.href = href

            def get_attribute(self, name):
                assert name == "href"
                return self.href

            def query_selector(self, selector):
                assert selector == "a"
                return self if self.href else None

            def text_content(self):
                return self.text

        headers = [Cell("순위"), Cell("선수명"), Cell("팀명"), Cell("NP"), Cell("IBB")]
        row = MagicMock()
        row.query_selector_all.return_value = [
            Cell("1"),
            Cell("홍길동", "/Player/Detail.aspx?playerId=123"),
            Cell("LG"),
            Cell("55"),
            Cell("2"),
        ]
        page = MagicMock()
        page.query_selector_all.side_effect = lambda selector: headers if "thead" in selector else [row]
        pitchers = {123: PitcherStats(player_id=123, season=2025, league="REGULAR")}
        monkeypatch.setenv("KBO_FAST_PARSE", "0")
        ctx = Basic2PageContext(
            page=page,
            season=2025,
            league="REGULAR",
            pitchers=pitchers,
            sort_key="NP",
        )

        with (
            patch("src.crawlers.player_pitching_all_series_crawler.retry_wait_for_selector", return_value=True),
            patch("src.crawlers.player_pitching_all_series_crawler.get_team_mapping_for_year"),
        ):
            processed = parse_basic2_page(ctx)

        assert processed == 1
        assert pitchers[123].extra_stats["metrics"]["np"] == 55
        assert pitchers[123].intentional_walks == 2
        assert pitchers[123].extra_stats["rankings"]["NP"] == 1

    def test_collect_basic2_adapts_context_and_stops_at_last_page(self):
        pitchers = {123: PitcherStats(player_id=123, season=2025, league="REGULAR")}
        ctx = Basic2AdditionalContext(
            page=MagicMock(),
            year=2025,
            league_name="REGULAR",
            series_info={"value": "0", "league": "POSTSEASON"},
            limit=10,
            policy=MagicMock(),
            pitchers=pitchers,
        )

        with (
            patch("src.crawlers.player_pitching_all_series_crawler.setup_pitcher_page", return_value=True),
            patch("src.crawlers.player_pitching_all_series_crawler.apply_sort", return_value=True),
            patch("src.crawlers.player_pitching_all_series_crawler.wait_for_table"),
            patch("src.crawlers.player_pitching_all_series_crawler.parse_basic2_page", return_value=1) as parse_page,
            patch("src.crawlers.player_pitching_all_series_crawler.go_to_next_page", return_value=False),
        ):
            _collect_pitcher_basic2_additional(ctx)

        page_ctx = parse_page.call_args.args[0]
        assert page_ctx.season == 2025
        assert page_ctx.league == "POSTSEASON"
        assert page_ctx.sort_key == "NP"
        assert page_ctx.max_players == 10

    def test_setup_pitcher_page_selects_season_and_series(self):
        page = MagicMock()
        page.title.return_value = "KBO Record"
        policy = MagicMock()

        ready = setup_pitcher_page(page, "https://example.test/basic1", 2025, "0", policy)

        assert ready is True
        page.select_option.assert_any_call('select[name*="ddlSeason"]', "2025")
        page.select_option.assert_any_call('select[name*="ddlSeries"]', value="0")
        assert policy.delay.call_count == 4

    def test_setup_pitcher_page_returns_false_for_kbo_error_page(self):
        page = MagicMock()
        page.title.return_value = "Error"

        ready = setup_pitcher_page(page, "https://example.test/basic1", 2025, "0")

        assert ready is False

    def test_apply_sort_by_code_uses_javascript_fallback(self):
        page = MagicMock()
        page.wait_for_selector.side_effect = RuntimeError("selector missing")
        page.evaluate.side_effect = [True, None]
        policy = MagicMock()

        with patch("src.crawlers.player_pitching_all_series_crawler.PlaywrightError", RuntimeError):
            applied = _apply_sort_by_code(page, "PIT_CN", policy)

        assert applied is True
        page.evaluate.assert_any_call("typeof sort === 'function'")
        page.evaluate.assert_any_call("sort('PIT_CN')")

    def test_go_to_next_page_clicks_enabled_page_button(self):
        page = MagicMock()
        button = MagicMock()
        button.get_attribute.return_value = None
        page.query_selector.side_effect = [MagicMock(), button]

        with patch("src.crawlers.player_pitching_all_series_crawler.wait_for_table"):
            moved = go_to_next_page(page, 1)

        assert moved is True
        page.click.assert_called_once_with('a[href*="btnNo2"]', timeout=15000)

    def test_collect_basic1_loop_stops_when_player_limit_is_reached(self):
        pitchers = {123: PitcherStats(player_id=123, season=2025, league="REGULAR")}
        ctx = PitcherBasic1Context(
            page=MagicMock(),
            year=2025,
            league_name="REGULAR",
            iteration_targets=[{"value": "", "text": "전체"}],
            by_team=False,
            limit=1,
            policy=MagicMock(),
            pitchers=pitchers,
        )

        with (
            patch("src.crawlers.player_pitching_all_series_crawler._select_pitcher_team_if_needed", return_value=True),
            patch("src.crawlers.player_pitching_all_series_crawler.wait_for_table"),
            patch("src.crawlers.player_pitching_all_series_crawler.parse_basic1_page", return_value=1) as parse_page,
        ):
            _collect_pitcher_basic1_loop(ctx)

        parse_page.assert_called_once()

    def test_crawl_pitcher_series_orchestrates_basic1_and_basic2(self):
        page = MagicMock()
        browser = MagicMock()
        context = MagicMock()
        context.new_page.return_value = page
        browser.new_context.return_value = context
        playwright = MagicMock()
        playwright.chromium.launch.return_value = browser
        manager = MagicMock()
        manager.__enter__.return_value = playwright
        manager.__exit__.return_value = False
        stats = PitcherStats(player_id=123, season=2025, league="REGULAR", player_name="홍길동")

        def _collect_basic1(ctx):
            ctx.pitchers[123] = stats

        with (
            patch("src.crawlers.player_pitching_all_series_crawler.sync_playwright", return_value=manager),
            patch("src.crawlers.player_pitching_all_series_crawler.setup_pitcher_page", return_value=True),
            patch(
                "src.crawlers.player_pitching_all_series_crawler._get_pitcher_team_options",
                return_value=[{"value": "", "text": "전체"}],
            ),
            patch(
                "src.crawlers.player_pitching_all_series_crawler._collect_pitcher_basic1_loop",
                side_effect=_collect_basic1,
            ),
            patch(
                "src.crawlers.player_pitching_all_series_crawler._collect_pitcher_basic2_additional"
            ) as collect_basic2,
            patch(
                "src.crawlers.player_pitching_all_series_crawler.build_pitching_crawl_summary",
                return_value=({"filtered_rows": 0}, [stats]),
            ),
            patch("src.crawlers.player_pitching_all_series_crawler.save_pitching_stats_to_db", return_value=1) as save,
        ):
            result = crawl_pitcher_series(2025, "regular", save_to_db=True, headless=False)

        assert result == [stats]
        playwright.chromium.launch.assert_called_once_with(headless=False)
        collect_basic2.assert_called_once()
        save.assert_called_once_with([stats.to_repository_payload()])
        browser.close.assert_called_once()

    def test_crawl_pitcher_series_rejects_unknown_series(self):
        with pytest.raises(ValueError, match="지원하지 않는 시리즈"):
            crawl_pitcher_series(2025, "unknown")

    def test_team_option_helpers_select_available_team(self):
        page = MagicMock()
        page.query_selector.return_value = MagicMock()
        page.eval_on_selector_all.return_value = [{"value": "LG", "text": "LG"}, {"value": "", "text": "전체"}]
        policy = MagicMock()

        options = _get_pitcher_team_options(page, by_team=True)
        selected = _select_pitcher_team_if_needed(page, options[0], by_team=True, policy=policy)

        assert options == [{"value": "LG", "text": "LG"}]
        assert selected is True
        page.select_option.assert_called_once()
        policy.delay.assert_called_once()

    def test_apply_sort_by_label_clicks_visible_matching_header(self):
        target = MagicMock()
        target.is_visible.return_value = True
        target.text_content.return_value = "NP"
        page = MagicMock()
        page.query_selector_all.return_value = [target]

        applied = _apply_sort_by_label(page, "NP")

        assert applied is True
        target.click.assert_called_once()
