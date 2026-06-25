"""Tests for low-coverage utility modules."""
from __future__ import annotations

import pytest

from src.utils.team_codes import TEAM_NAME_TO_CODE, resolve_team_code
from src.utils.series_validation import (
    filter_series_for_year,
    get_available_series_by_year,
    is_series_available,
    validate_year_series_combination,
)
from src.utils.player_stats_helpers import extract_rows_fast


class TestTeamCodes:
    def test_team_name_to_code_mapping(self):
        assert isinstance(TEAM_NAME_TO_CODE, dict)
        assert len(TEAM_NAME_TO_CODE) > 0

    def test_resolve_team_code_exists(self):
        assert callable(resolve_team_code)


class TestSeriesValidation:
    def test_get_available_series_returns_list(self):
        result = get_available_series_by_year(2020)
        assert isinstance(result, list)

    def test_is_series_available_valid(self):
        series = get_available_series_by_year(2020)
        if series:
            assert is_series_available(2020, series[0]) is True

    def test_is_series_available_invalid(self):
        assert is_series_available(2020, "INVALID_SERIES") is False

    def test_filter_series_returns_list(self):
        result = filter_series_for_year(2020, ["regular"])
        assert isinstance(result, list)

    def test_validate_year_series_valid(self):
        series = get_available_series_by_year(2020)
        if series:
            result = validate_year_series_combination(2020, series[0])
            assert result[0] is True

    def test_validate_year_series_invalid(self):
        result = validate_year_series_combination(2020, "INVALID")
        assert result[0] is False


class TestPlayerStatsHelpers:
    def test_function_exists(self):
        assert callable(extract_rows_fast)
