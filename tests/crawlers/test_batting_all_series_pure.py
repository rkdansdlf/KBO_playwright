from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.crawlers.player_batting_all_series_crawler import (
    BattingRowData,
    _build_batting_data,
    _extract_basic2_stat_by_header,
    _extract_player_id_from_href,
    _finalize_batting_summary,
    _collect_basic2_pages,
    _collect_batting_stats_loop,
    _get_team_options,
    _handle_batting_fallback,
    _navigate_to_basic2,
    _apply_pa_sorting,
    _parse_basic2_header_data_legacy,
    _select_team_if_needed,
    _select_year_option,
    _select_series_option,
    _save_batting_if_needed,
    _is_basic2_headers,
    _merge_basic2_data,
    _parse_basic2_header_data_fast,
    parse_batting_stats_table,
    crawl_series_batting_stats,
    crawl_all_series,
    BattingCrawlContext,
    go_to_next_page,
    _parse_fast_row,
    build_batting_crawl_summary,
    get_series_mapping,
    safe_parse_number,
)


class TestGetSeriesMapping:
    def test_returns_dict(self):
        result = get_series_mapping()
        assert isinstance(result, dict)
        assert len(result) == 6

    def test_regular_mapping(self):
        result = get_series_mapping()
        assert result["regular"] == {"name": "KBO 정규시즌", "value": "0", "league": "REGULAR"}

    def test_all_keys_present(self):
        result = get_series_mapping()
        expected_keys = {"regular", "exhibition", "wildcard", "semi_playoff", "playoff", "korean_series"}
        assert set(result.keys()) == expected_keys

    def test_all_values_have_required_fields(self):
        result = get_series_mapping()
        for key, value in result.items():
            assert "name" in value
            assert "value" in value
            assert "league" in value


class TestSafeParseNumber:
    def test_integer(self):
        assert safe_parse_number("42", int) == 42

    def test_float(self):
        assert safe_parse_number("0.300", float) == 0.3

    def test_none_for_empty(self):
        assert safe_parse_number("", int) is None

    def test_none_for_none(self):
        assert safe_parse_number(None, int) is None

    def test_none_for_dash(self):
        assert safe_parse_number("-", int) is None

    def test_none_for_na(self):
        assert safe_parse_number("N/A", float) is None

    def test_whitespace_stripped(self):
        assert safe_parse_number("  42  ", int) == 42

    def test_invalid_string(self):
        assert safe_parse_number("abc", int) is None

    def test_zero_allowed(self):
        assert safe_parse_number("0", int) == 0

    def test_negative(self):
        assert safe_parse_number("-5", int) == -5

    def test_float_precision(self):
        result = safe_parse_number("0.333", float)
        assert result == 0.333


class TestExtractPlayerIdFromHref:
    def test_basic_href(self):
        assert _extract_player_id_from_href("/Player.aspx?playerId=12345") == 12345

    def test_href_with_extra_params(self):
        assert _extract_player_id_from_href("/Player.aspx?playerId=67890&season=2023") == 67890

    def test_none_href(self):
        assert _extract_player_id_from_href(None) is None

    def test_empty_href(self):
        assert _extract_player_id_from_href("") is None

    def test_no_player_id(self):
        assert _extract_player_id_from_href("/Player.aspx?other=value") is None

    def test_large_id(self):
        assert _extract_player_id_from_href("?playerId=999999") == 999999


class TestIsBasic2Headers:
    def test_basic2_with_BB(self):
        assert _is_basic2_headers(["이름", "BB", "SO"]) is True

    def test_basic2_with_slg(self):
        assert _is_basic2_headers(["이름", "SLG"]) is True

    def test_basic2_with_obp(self):
        assert _is_basic2_headers(["OBP"]) is True

    def test_basic2_with_ops(self):
        assert _is_basic2_headers(["OPS"]) is True

    def test_basic2_with_hbp(self):
        assert _is_basic2_headers(["HBP"]) is True

    def test_not_basic2(self):
        assert _is_basic2_headers(["이름", "AVG", "G", "AB"]) is False

    def test_empty_headers(self):
        assert _is_basic2_headers([]) is False

    def test_korean_headers(self):
        assert _is_basic2_headers(["볼넷"]) is True

    def test_combined_headers(self):
        assert _is_basic2_headers(["BB", "SO"]) is True


class TestBuildBattingData:
    def _make_ctx(self, cells, series_key="regular", is_basic2=False, year=2023):
        return BattingRowData(
            cells=cells,
            player_id=12345,
            player_name="홍길동",
            team_code="LG",
            series_key=series_key,
            is_basic2=is_basic2,
            year=year,
        )

    def test_regular_basic1(self):
        cells = ["", "홍길동", "LG", "0.300", "120", "450", "100", "80", "20", "5", "15", "50", "10", "3", "2"]
        ctx = self._make_ctx(cells, series_key="regular", is_basic2=False)
        result = _build_batting_data(ctx)
        assert result["player_id"] == 12345
        assert result["player_name"] == "홍길동"
        assert result["team_code"] == "LG"
        assert result["season"] == 2023
        assert result["league"] == "REGULAR"
        assert result["avg"] == 0.3
        assert result["games"] == 120
        assert result["plate_appearances"] == 450
        assert result["at_bats"] == 100
        assert result["runs"] == 80
        assert result["hits"] == 20
        assert result["doubles"] == 5
        assert result["triples"] == 15
        assert result["home_runs"] == 50
        assert result["total_bases"] == 10
        assert result["rbi"] == 3
        assert result["sacrifice_hits"] == 2

    def test_regular_basic2(self):
        cells = [
            "",
            "홍길동",
            "LG",
            "0.300",
            "30",
            "5",
            "10",
            "15",
            "3",
            "0.500",
            "0.400",
            "0.900",
            "5",
            "0.250",
            "0.200",
        ]
        ctx = self._make_ctx(cells, series_key="regular", is_basic2=True)
        result = _build_batting_data(ctx)
        assert result["avg"] == 0.3
        assert result["walks"] == 30
        assert result["intentional_walks"] == 5
        assert result["hbp"] == 10
        assert result["strikeouts"] == 15
        assert result["gdp"] == 3
        assert result["slg"] == 0.5
        assert result["obp"] == 0.4
        assert result["ops"] == 0.9
        assert result["extra_stats"]["multi_hits"] == 5
        assert result["extra_stats"]["risp_avg"] == 0.25
        assert result["extra_stats"]["pinch_hit_avg"] == 0.2

    def test_exhibition_series(self):
        cells = ["", "홍길동", "LG", "0.250"]
        ctx = self._make_ctx(cells, series_key="exhibition")
        result = _build_batting_data(ctx)
        assert result["league"] == "EXHIBITION"

    def test_playoff_series(self):
        cells = ["", "홍길동", "LG", "0.350"]
        ctx = self._make_ctx(cells, series_key="playoff")
        result = _build_batting_data(ctx)
        assert result["league"] == "PLAYOFF"

    def test_year_fallback(self):
        cells = ["", "홍길동", "LG", "0.300"]
        ctx = self._make_ctx(cells, year=None)
        result = _build_batting_data(ctx)
        assert result["season"] >= 2024

    def test_short_cells(self):
        cells = ["", "홍길동", "LG"]
        ctx = self._make_ctx(cells)
        result = _build_batting_data(ctx)
        assert result["avg"] is None
        assert result["games"] is None

    def test_non_regular_series(self):
        cells = [
            "",
            "홍길동",
            "LG",
            "0.280",
            "50",
            "200",
            "30",
            "15",
            "3",
            "2",
            "5",
            "10",
            "2",
            "1",
            "3",
            "5",
            "2",
            "1",
        ]
        ctx = self._make_ctx(cells, series_key="korean_series")
        result = _build_batting_data(ctx)
        assert result["league"] == "KOREAN_SERIES"
        assert result["hits"] == 15
        assert result["doubles"] == 3
        assert result["triples"] == 2
        assert result["home_runs"] == 5
        assert result["stolen_bases"] == 2
        assert result["caught_stealing"] == 1
        assert result["walks"] == 3
        assert result["hbp"] == 5
        assert result["strikeouts"] == 2
        assert result["gdp"] == 1


class TestExtractBasic2StatByHeader:
    def test_walks(self):
        batting_data = {}
        _extract_basic2_stat_by_header("BB", ["", "", "", "", "30"], batting_data)
        assert batting_data["walks"] == 30

    def test_strikeouts(self):
        batting_data = {}
        _extract_basic2_stat_by_header("SO", ["", "", "", "", "", "", "", "15"], batting_data)
        assert batting_data["strikeouts"] == 15

    def test_slg(self):
        batting_data = {}
        _extract_basic2_stat_by_header("SLG", ["", "", "", "", "", "", "", "", "", "0.500"], batting_data)
        assert batting_data["slg"] == 0.5

    def test_extra_stat(self):
        batting_data = {}
        _extract_basic2_stat_by_header("MH", ["", "", "", "", "", "", "", "", "", "", "", "", "5"], batting_data)
        assert batting_data["extra_stats"]["multi_hits"] == 5

    def test_unknown_header(self):
        batting_data = {}
        _extract_basic2_stat_by_header("UNKNOWN", ["", "", "", "10"], batting_data)
        assert "UNKNOWN" not in batting_data

    def test_cells_too_short(self):
        batting_data = {}
        _extract_basic2_stat_by_header("BB", ["", "", ""], batting_data)
        assert "walks" not in batting_data


class TestParseFastRow:
    def test_basic_row(self):
        row = {
            "cells": ["", "홍길동", "LG", "0.300", "120"],
            "linkHref": "/Player.aspx?playerId=12345",
            "linkText": "홍길동",
        }
        result = _parse_fast_row(row, "BB", 2023, {})
        assert result is not None
        player_id, data = result
        assert player_id == 12345
        assert data["player_name"] == "홍길동"

    def test_short_cells(self):
        row = {"cells": ["", "홍길동"], "linkHref": "/Player.aspx?playerId=12345"}
        result = _parse_fast_row(row, "BB", 2023, {})
        assert result is None

    def test_no_player_id(self):
        row = {"cells": ["", "홍길동", "LG"], "linkHref": "/Player.aspx?other=value"}
        result = _parse_fast_row(row, "BB", 2023, {})
        assert result is None

    @patch("src.crawlers.player_batting_all_series_crawler.get_team_code")
    def test_team_code_lookup(self, mock_get_team_code):
        mock_get_team_code.return_value = "LG"
        row = {
            "cells": ["", "홍길동", "LG", "0.300", "120"],
            "linkHref": "/Player.aspx?playerId=12345",
            "linkText": "홍길동",
        }
        result = _parse_fast_row(row, "BB", 2023, {})
        assert result is not None
        _, data = result
        assert data["team_code"] == "LG"

    @patch("src.crawlers.player_batting_all_series_crawler.get_team_code")
    def test_team_mapping_fallback(self, mock_get_team_code):
        mock_get_team_code.return_value = None
        row = {
            "cells": ["", "홍길동", "LG", "0.300", "120"],
            "linkHref": "/Player.aspx?playerId=12345",
            "linkText": "홍길동",
        }
        team_mapping = {"LG": "LG"}
        result = _parse_fast_row(row, "BB", 2023, team_mapping)
        assert result is not None
        _, data = result
        assert data["team_code"] == "LG"


class TestBuildBattingCrawlSummary:
    def test_all_valid(self):
        rows = [
            {
                "player_id": 1,
                "player_name": "A",
                "season": 2023,
                "team_code": "LG",
                "avg": 0.300,
                "games": 120,
                "at_bats": 400,
            },
            {
                "player_id": 2,
                "player_name": "B",
                "season": 2023,
                "team_code": "SS",
                "avg": 0.280,
                "games": 110,
                "at_bats": 380,
            },
        ]
        summary, valid = build_batting_crawl_summary(rows)
        assert summary["processed_rows"] == 2
        assert summary["valid_rows"] == 2
        assert summary["filtered_rows"] == 0
        assert len(valid) == 2

    def test_empty_list(self):
        summary, valid = build_batting_crawl_summary([])
        assert summary["processed_rows"] == 0
        assert summary["valid_rows"] == 0
        assert len(valid) == 0


class TestFinalizeBattingSummary:
    def test_returns_valid_data(self):
        all_players = [
            {
                "player_id": 1,
                "player_name": "A",
                "season": 2023,
                "team_code": "LG",
                "avg": 0.300,
                "games": 120,
                "at_bats": 400,
            },
            {
                "player_id": 2,
                "player_name": "B",
                "season": 2023,
                "team_code": "SS",
                "avg": 0.280,
                "games": 110,
                "at_bats": 380,
            },
        ]
        series_info = {"name": "정규시즌"}
        result = _finalize_batting_summary(all_players, series_info)
        assert len(result) == 2

    def test_empty_input(self):
        result = _finalize_batting_summary([], {"name": "테스트"})
        assert result == []


class TestBattingPageParsers:
    def test_fast_basic1_table_parser_builds_normalized_payload(self):
        page = MagicMock()
        page.evaluate.return_value = {
            "is_basic2": False,
            "results": [
                {
                    "player_id": 123,
                    "player_name": "홍길동",
                    "team_name": "LG 트윈스",
                    "raw_cells": [
                        "1",
                        "홍길동",
                        "LG 트윈스",
                        "0.333",
                        "10",
                        "40",
                        "30",
                        "12",
                        "3",
                        "1",
                        "2",
                        "8",
                        "1",
                        "0",
                        "4",
                        "1",
                        "5",
                        "0",
                        "0",
                    ],
                },
            ],
        }

        with (
            patch("src.crawlers.player_batting_all_series_crawler.get_team_mapping_for_year"),
            patch("src.crawlers.player_batting_all_series_crawler.resolve_team_code", return_value="LG"),
        ):
            records = parse_batting_stats_table(page, "regular", 2025, use_fast=True)

        assert records[0]["player_id"] == 123
        assert records[0]["team_code"] == "LG"
        assert records[0]["season"] == 2025
        assert records[0]["avg"] == 0.333
        assert records[0]["home_runs"] == 8

    def test_fast_table_parser_returns_empty_for_invalid_extraction(self):
        page = MagicMock()
        page.evaluate.return_value = {"is_basic2": False, "results": []}

        records = parse_batting_stats_table(page, "regular", 2025, use_fast=True)

        assert records == []

    def test_fast_basic2_header_parser_extracts_requested_stat(self):
        page = MagicMock()
        page.query_selector.return_value = None
        rows = [
            {
                "cells": ["1", "홍길동", "LG", "0.333", "12"],
                "linkHref": "/Player/Detail.aspx?playerId=123",
                "linkText": "홍길동",
            },
        ]

        with (
            patch("src.crawlers.player_batting_all_series_crawler.extract_rows_fast", return_value=rows),
            patch(
                "src.crawlers.player_batting_all_series_crawler.get_team_mapping_for_year", return_value={"LG": "LG"}
            ),
            patch("src.crawlers.player_batting_all_series_crawler.get_team_code", return_value="LG"),
        ):
            records = _parse_basic2_header_data_fast(page, "BB", "볼넷", 2025)

        assert records == {
            123: {
                "player_id": 123,
                "player_name": "홍길동",
                "team_code": "LG",
                "walks": 12,
            },
        }

    def test_merge_basic2_data_updates_only_non_identity_values(self):
        basic1 = [
            {
                "player_id": 123,
                "player_name": "홍길동",
                "team_code": "LG",
                "season": 2025,
                "league": "REGULAR",
                "avg": 0.3,
            },
        ]

        with patch(
            "src.crawlers.player_batting_all_series_crawler.crawl_basic2_with_headers",
            return_value={
                123: {
                    "player_id": 123,
                    "player_name": "다른 이름",
                    "team_code": "SS",
                    "walks": 12,
                    "ops": None,
                },
            },
        ):
            merged = _merge_basic2_data(basic1, MagicMock(), 2025, {}, MagicMock())

        assert merged == [
            {
                "player_id": 123,
                "player_name": "홍길동",
                "team_code": "LG",
                "season": 2025,
                "league": "REGULAR",
                "avg": 0.3,
                "walks": 12,
            },
        ]

    def test_legacy_table_parser_reads_dom_rows(self):
        class Node:
            def __init__(self, text="", *, href=None, children=None):
                self.text = text
                self.href = href
                self.children = children or {}

            def get_attribute(self, name):
                return self.href if name == "href" else None

            def query_selector(self, selector):
                return self.children.get(selector)

            def query_selector_all(self, selector):
                return self.children.get(selector, [])

            def text_content(self):
                return self.text

        headers = [
            Node(value) for value in ["순위", "선수명", "팀명", "AVG", "G", "PA", "AB", "R", "H", "2B", "3B", "HR"]
        ]
        cells = [Node(value) for value in ["1", "홍길동", "LG", "0.333", "10", "40", "30", "8", "12", "3", "1", "2"]]
        cells[1].children["a"] = Node("홍길동", href="/Player/Detail.aspx?playerId=123")
        row = Node(children={"td": cells})
        table = Node(children={"thead": Node(children={"th": headers}), "tbody": Node(children={"tr": [row]})})
        page = MagicMock()
        page.query_selector.side_effect = lambda selector: table if selector == "table" else None

        with (
            patch("src.crawlers.player_batting_all_series_crawler.get_team_mapping_for_year"),
            patch("src.crawlers.player_batting_all_series_crawler.resolve_team_code", return_value="LG"),
        ):
            records = parse_batting_stats_table(page, "regular", 2025, use_fast=False)

        assert records[0]["player_id"] == 123
        assert records[0]["hits"] == 12
        assert records[0]["home_runs"] == 2

    def test_legacy_basic2_parser_reads_header_specific_stat(self):
        class Node:
            def __init__(self, text="", *, href=None, children=None):
                self.text = text
                self.href = href
                self.children = children or {}

            def get_attribute(self, name):
                return self.href if name == "href" else None

            def query_selector(self, selector):
                return self.children.get(selector)

            def query_selector_all(self, selector):
                return self.children.get(selector, [])

            def text_content(self):
                return self.text

        cells = [Node(value) for value in ["1", "홍길동", "LG", "0.333", "12"]]
        cells[1].children["a"] = Node("홍길동", href="/Player/Detail.aspx?playerId=123")
        row = Node(children={"td": cells})
        table = Node(children={"tbody": Node(children={"tr": [row]})})
        page = MagicMock()
        page.query_selector.side_effect = lambda selector: table if selector == "table" else None

        with (
            patch(
                "src.crawlers.player_batting_all_series_crawler.get_team_mapping_for_year", return_value={"LG": "LG"}
            ),
            patch("src.crawlers.player_batting_all_series_crawler.get_team_code", return_value="LG"),
        ):
            records = _parse_basic2_header_data_legacy(page, "BB", "볼넷", 2025)

        assert records[123]["walks"] == 12

    def test_collect_basic2_pages_merges_duplicate_player_rows(self):
        page = MagicMock()
        players = {}

        with (
            patch("src.crawlers.player_batting_all_series_crawler.retry_wait_for_selector", side_effect=[True, True]),
            patch(
                "src.crawlers.player_batting_all_series_crawler.parse_batting_stats_table",
                side_effect=[
                    [{"player_id": 1, "avg": 0.3}],
                    [{"player_id": 1, "walks": 10}, {"player_id": 2, "avg": 0.2}],
                ],
            ),
            patch("src.crawlers.player_batting_all_series_crawler.go_to_next_page", side_effect=[True, False]),
        ):
            _collect_basic2_pages(page, 2025, players)

        assert players == {
            1: {"player_id": 1, "avg": 0.3, "walks": 10},
            2: {"player_id": 2, "avg": 0.2},
        }

    def test_go_to_next_page_clicks_enabled_number_button(self):
        page = MagicMock()
        button = MagicMock()
        button.get_attribute.return_value = None
        page.query_selector.side_effect = [MagicMock(), button]
        policy = MagicMock()

        moved = go_to_next_page(page, 1, policy)

        assert moved is True
        policy.delay.assert_called_once()
        page.click.assert_called_once_with('a[href*="btnNo2"]', timeout=15000)

    def test_navigate_to_basic2_returns_false_when_link_is_unavailable(self):
        with patch("src.crawlers.player_batting_all_series_crawler.retry_wait_for_selector", return_value=False):
            moved = _navigate_to_basic2(MagicMock(), None)

        assert moved is False

    def test_crawl_series_orchestrates_basic1_basic2_and_finalization(self):
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

        def _collect(ctx):
            ctx.all_players_data.append({"player_id": 123, "player_name": "홍길동"})

        crawled = [{"player_id": 123, "player_name": "홍길동", "walks": 12}]

        with (
            patch("src.crawlers.player_batting_all_series_crawler.sync_playwright", return_value=manager),
            patch("src.crawlers.player_batting_all_series_crawler.install_sync_resource_blocking"),
            patch("src.crawlers.player_batting_all_series_crawler.compliance.is_allowed_sync", return_value=True),
            patch("src.crawlers.player_batting_all_series_crawler._select_season_and_series"),
            patch(
                "src.crawlers.player_batting_all_series_crawler._get_team_options",
                return_value=[{"value": "", "text": "전체"}],
            ),
            patch("src.crawlers.player_batting_all_series_crawler._collect_batting_stats_loop", side_effect=_collect),
            patch("src.crawlers.player_batting_all_series_crawler._merge_basic2_data", return_value=crawled),
            patch("src.crawlers.player_batting_all_series_crawler._finalize_batting_summary", return_value=crawled),
            patch("src.crawlers.player_batting_all_series_crawler._save_batting_if_needed") as save,
        ):
            result = crawl_series_batting_stats(2025, "regular", save_to_db=True, headless=True)

        assert result == crawled
        playwright.chromium.launch.assert_called_once_with(headless=True)
        browser.close.assert_called_once()
        save.assert_called_once_with(crawled, save_to_db=True)

    def test_crawl_series_rejects_unknown_series_before_browser_start(self):
        with patch("src.crawlers.player_batting_all_series_crawler.sync_playwright") as browser:
            result = crawl_series_batting_stats(2025, "unknown")

        assert result == []
        browser.assert_not_called()

    def test_season_and_series_selectors_wait_and_apply_values(self):
        page = MagicMock()
        policy = MagicMock()

        with patch("src.crawlers.player_batting_all_series_crawler.retry_wait_for_selector", return_value=True):
            _select_year_option(page, 2025, policy)
            _select_series_option(page, "0", policy)

        page.select_option.assert_any_call(
            'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]',
            "2025",
        )
        page.select_option.assert_any_call(
            'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]',
            value="0",
        )
        assert policy.delay.call_count == 2

    def test_team_helpers_extract_select_and_sort(self):
        page = MagicMock()
        page.eval_on_selector_all.return_value = [{"value": "LG", "text": "LG"}, {"value": "", "text": "전체"}]
        page.query_selector.return_value = MagicMock()
        policy = MagicMock()

        options = _get_team_options(page, by_team=True)
        selected = _select_team_if_needed(page, options[0], by_team=True, policy=policy)
        _apply_pa_sorting(page, policy)

        assert options == [{"value": "LG", "text": "LG"}]
        assert selected is True
        assert page.select_option.called
        assert page.click.called

    def test_collect_batting_loop_updates_duplicate_player_until_last_page(self):
        page = MagicMock()
        policy = MagicMock()
        data = []
        ctx = BattingCrawlContext(
            page=page,
            year=2025,
            series_key="regular",
            iteration_targets=[{"value": "", "text": "전체"}],
            by_team=False,
            limit=None,
            policy=policy,
            unique_players=set(),
            all_players_data=data,
        )

        with (
            patch("src.crawlers.player_batting_all_series_crawler._apply_pa_sorting"),
            patch(
                "src.crawlers.player_batting_all_series_crawler.parse_batting_stats_table",
                side_effect=[
                    [{"player_id": 1, "avg": 0.3}],
                    [{"player_id": 1, "walks": 12}, {"player_id": 2, "avg": 0.2}],
                ],
            ),
            patch("src.crawlers.player_batting_all_series_crawler.go_to_next_page", side_effect=[True, False]),
        ):
            _collect_batting_stats_loop(ctx)

        assert data == [{"player_id": 1, "avg": 0.3, "walks": 12}, {"player_id": 2, "avg": 0.2}]

    def test_batting_fallback_marks_source_and_saves_payloads(self):
        rows = [{"player_id": 123, "source": "FALLBACK"}]

        with (
            patch("src.crawlers.player_batting_all_series_crawler.fallback_batting_from_db", return_value=rows),
            patch("src.crawlers.player_batting_all_series_crawler.FallbackMonitor.log_fallback") as monitor,
            patch("src.crawlers.player_batting_all_series_crawler.save_batting_stats_safe", return_value=1) as save,
        ):
            result = _handle_batting_fallback(2025, "regular", "test failure", save_to_db=True)

        assert result == [{"player_id": 123, "source": "FALLBACK_AUTO"}]
        monitor.assert_called_once()
        save.assert_called_once_with(result)

    def test_save_batting_if_needed_ignores_empty_payloads_and_saves_nonempty(self):
        with patch("src.crawlers.player_batting_all_series_crawler.save_batting_stats_safe", return_value=1) as save:
            _save_batting_if_needed([], save_to_db=True)
            _save_batting_if_needed([{"player_id": 123}], save_to_db=True)

        save.assert_called_once_with([{"player_id": 123}])

    def test_crawl_all_series_delegates_each_mapping_with_shared_options(self):
        policy = MagicMock()
        mapping = {
            "regular": {"name": "정규시즌"},
            "exhibition": {"name": "시범경기"},
        }

        with (
            patch("src.crawlers.player_batting_all_series_crawler.RequestPolicy", return_value=policy),
            patch("src.crawlers.player_batting_all_series_crawler.get_series_mapping", return_value=mapping),
            patch(
                "src.crawlers.player_batting_all_series_crawler.crawl_series_batting_stats",
                side_effect=[[{"player_id": 1}], [{"player_id": 2}]],
            ) as crawl_series,
        ):
            result = crawl_all_series(2025, limit=10, save_to_db=True, headless=True, by_team=True)

        assert result == {"regular": [{"player_id": 1}], "exhibition": [{"player_id": 2}]}
        assert crawl_series.call_count == 2
        policy.delay.assert_called()
