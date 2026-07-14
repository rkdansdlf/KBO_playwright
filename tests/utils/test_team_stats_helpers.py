"""Tests for team_stats_helpers — cell value and team ID utilities."""

from __future__ import annotations

import pytest

from src.utils.team_stats_helpers import (
    build_team_column_map,
    get_cell_value,
    parse_numeric,
    parse_team_stats_html,
    resolve_team_id,
    TeamStatsParseContext,
)


class MockLocator:
    def __init__(self, text):
        self._text = text

    def get_attribute(self, name):
        return None

    def get_text(self, strip=True):
        return self._text.strip() if strip else self._text


class TestGetCellValue:
    def test_valid_index(self):
        cells = [MockLocator("LG"), MockLocator("5")]
        assert get_cell_value(cells, 0) == "LG"
        assert get_cell_value(cells, 1) == "5"

    def test_out_of_range(self):
        cells = [MockLocator("LG")]
        assert get_cell_value(cells, 5) is None

    def test_empty_cells(self):
        assert get_cell_value([], 0) is None


class TestResolveTeamId:
    def test_exact_match(self):
        mapping = {"LG": "LG", "NC": "NC"}
        assert resolve_team_id("LG", mapping) == "LG"

    def test_space_normalized(self):
        mapping = {"LG트윈스": "LG"}
        assert resolve_team_id("LG 트윈스", mapping) == "LG"

    def test_no_match(self):
        assert resolve_team_id("없음", {}) is None

    def test_none_name_raises(self):
        with pytest.raises(AttributeError):
            resolve_team_id(None, {"LG": "LG"})


class TestParseNumeric:
    def test_int_value(self):
        assert parse_numeric("5", as_float=False) == 5

    def test_float_value(self):
        assert parse_numeric("3.14", as_float=True) == pytest.approx(3.14)

    def test_comma_separated(self):
        assert parse_numeric("1,234", as_float=False) == 1234

    def test_percentage(self):
        assert parse_numeric("85.5%", as_float=True) == pytest.approx(85.5)

    def test_empty_string(self):
        assert parse_numeric("", as_float=False) is None

    def test_dash_value(self):
        assert parse_numeric("-", as_float=False) is None

    def test_na_value(self):
        assert parse_numeric("N/A", as_float=False) is None

    def test_float_string_when_int_requested_is_truncated(self):
        assert parse_numeric(".321", as_float=False) == 0

    def test_invalid_value_returns_none(self):
        assert parse_numeric("abc", as_float=True) is None


class TestBuildTeamColumnMap:
    def test_maps_known_headers_and_defaults_team_name(self):
        result = build_team_column_map(["순위", "팀", "AVG"], {"avg": "avg"})

        assert result == {"avg": 2, "team_name": 1}

    def test_defaults_team_name_to_zero_for_single_column(self):
        assert build_team_column_map(["팀"], {}) == {"team_name": 0}


class TestParseTeamStatsHtml:
    def test_returns_empty_when_no_table_exists(self):
        assert (
            parse_team_stats_html(
                "<html><body>No stats</body></html>",
                TeamStatsParseContext(2025, "KBO", {}, {}, set(), set()),
            )
            == []
        )

    def test_parses_thead_tbody_stats_and_extra_fields(self):
        html = """
        <table class="tData01">
          <thead><tr><th>순위</th><th>팀</th><th>AVG</th><th>HR</th><th>OPS</th></tr></thead>
          <tbody>
            <tr><td>1</td><td>LG 트윈스</td><td>.321</td><td>25</td><td>.900</td></tr>
          </tbody>
        </table>
        """

        result = parse_team_stats_html(
            html,
            TeamStatsParseContext(
                2025,
                "KBO",
                {"LG트윈스": "LG"},
                {"팀": "team_name", "avg": "avg", "hr": "home_runs", "ops": "ops"},
                {"avg", "home_runs"},
                {"avg", "ops"},
            ),
        )

        assert result == [
            {
                "team_id": "LG",
                "team_name": "LG 트윈스",
                "season": 2025,
                "league": "KBO",
                "avg": pytest.approx(0.321),
                "home_runs": 25,
                "extra_stats": {"ops": pytest.approx(0.9)},
            },
        ]

    def test_parses_rows_without_tbody_and_skips_short_rows(self):
        html = """
        <table>
          <tr><th>팀</th><th>승</th></tr>
          <tr><td>SSG</td><td>80</td></tr>
          <tr><td>짧은행</td></tr>
        </table>
        """

        result = parse_team_stats_html(
            html,
            TeamStatsParseContext(
                2025,
                "KBO",
                {"SSG": "SSG"},
                {"팀": "team_name", "승": "wins"},
                {"wins"},
                set(),
            ),
        )

        assert result == [
            {
                "team_id": "SSG",
                "team_name": "SSG",
                "season": 2025,
                "league": "KBO",
                "wins": 80,
            },
        ]

    def test_uses_custom_value_parser_and_skips_none_values(self):
        html = """
        <table>
          <tr><th>팀</th><th>폼</th><th>메모</th></tr>
          <tr><td>NC</td><td>W3</td><td>-</td></tr>
        </table>
        """

        def parser(header_key, value):
            if value == "-":
                return None
            return f"{header_key}:{value}"

        result = parse_team_stats_html(
            html,
            TeamStatsParseContext(
                2025,
                "KBO",
                {},
                {"팀": "team_name", "폼": "form", "메모": "memo"},
                {"form"},
                set(),
                value_parser=parser,
            ),
        )

        assert result == [
            {
                "team_id": "NC",
                "team_name": "NC",
                "season": 2025,
                "league": "KBO",
                "form": "form:W3",
            },
        ]
