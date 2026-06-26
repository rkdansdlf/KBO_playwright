from __future__ import annotations

from unittest.mock import patch

import pytest

from src.crawlers.player_batting_all_series_crawler import (
    BattingRowData,
    _build_batting_data,
    _extract_basic2_stat_by_header,
    _extract_player_id_from_href,
    _finalize_batting_summary,
    _is_basic2_headers,
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
