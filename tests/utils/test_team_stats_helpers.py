"""Tests for team_stats_helpers — cell value and team ID utilities."""

import pytest

from src.utils.team_stats_helpers import get_cell_value, resolve_team_id, parse_numeric


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
        assert parse_numeric("5", False) == 5

    def test_float_value(self):
        assert parse_numeric("3.14", True) == pytest.approx(3.14)

    def test_comma_separated(self):
        assert parse_numeric("1,234", False) == 1234

    def test_percentage(self):
        assert parse_numeric("85.5%", True) == pytest.approx(85.5)

    def test_empty_string(self):
        assert parse_numeric("", False) is None

    def test_dash_value(self):
        assert parse_numeric("-", False) is None

    def test_na_value(self):
        assert parse_numeric("N/A", False) is None
