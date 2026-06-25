from __future__ import annotations

import pytest

from src.utils.series_validation import (
    filter_series_for_year,
    get_available_series_by_year,
    is_series_available,
    validate_year_series_combination,
)


class TestGetAvailableSeriesByYear:
    def test_returns_list(self):
        result = get_available_series_by_year(2025)
        assert isinstance(result, list)

    def test_2026_season(self):
        result = get_available_series_by_year(2026)
        assert len(result) > 0


class TestIsSeriesAvailable:
    def test_valid_series(self):
        series = get_available_series_by_year(2025)
        if series:
            assert is_series_available(2025, series[0]) is True

    def test_invalid_series(self):
        assert is_series_available(2025, "INVALID") is False


class TestFilterSeriesForYear:
    def test_returns_list(self):
        all_series = get_available_series_by_year(2025)
        result = filter_series_for_year(2025, all_series)
        assert isinstance(result, list)

    def test_non_empty(self):
        all_series = get_available_series_by_year(2025)
        result = filter_series_for_year(2025, all_series)
        assert len(result) > 0

    def test_empty_input(self):
        result = filter_series_for_year(2025, [])
        assert result == []


class TestValidateYearSeriesCombination:
    def test_valid_combination(self):
        valid_series = get_available_series_by_year(2025)
        if valid_series:
            is_valid, msg = validate_year_series_combination(2025, valid_series[0])
            assert is_valid is True

    def test_unknown_series(self):
        is_valid, msg = validate_year_series_combination(2025, "UNKNOWN_SERIES")
        assert is_valid is False
