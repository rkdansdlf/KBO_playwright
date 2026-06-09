"""Tests for series_validation — KBO season series by year."""


from src.utils.series_validation import (
    filter_series_for_year,
    get_available_series_by_year,
    get_recommended_series_for_period,
    get_series_info,
    is_series_available,
    validate_year_series_combination,
)


class TestGetAvailableSeriesByYear:
    def test_before_1982(self):
        assert get_available_series_by_year(1981) == []

    def test_1982_1985(self):
        assert get_available_series_by_year(1983) == ["regular", "korean_series"]

    def test_1986_1988(self):
        result = get_available_series_by_year(1987)
        assert "exhibition" in result
        assert "korean_series" in result

    def test_2000_2001(self):
        assert "playoff" not in get_available_series_by_year(2001)
        assert "korean_series" in get_available_series_by_year(2000)

    def test_2002_2006(self):
        assert "playoff" in get_available_series_by_year(2003)

    def test_2007_2014(self):
        result = get_available_series_by_year(2010)
        assert "semi_playoff" in result
        assert "playoff" in result
        assert "wildcard" not in result

    def test_2015_present(self):
        result = get_available_series_by_year(2025)
        assert "wildcard" in result
        assert "semi_playoff" in result
        assert "playoff" in result
        assert "korean_series" in result
        assert "exhibition" in result
        assert "regular" in result


class TestIsSeriesAvailable:
    def test_regular_available_all_years(self):
        assert is_series_available(1982, "regular")
        assert is_series_available(2025, "regular")

    def test_wildcard_not_available_before_2015(self):
        assert not is_series_available(2014, "wildcard")
        assert is_series_available(2015, "wildcard")

    def test_before_kbo(self):
        assert not is_series_available(1981, "regular")


class TestFilterSeriesForYear:
    def test_filters_unavailable(self):
        result = filter_series_for_year(2001, ["regular", "playoff", "korean_series"])
        assert "regular" in result
        assert "korean_series" in result
        assert "playoff" not in result

    def test_all_available(self):
        result = filter_series_for_year(2025, ["regular", "exhibition"])
        assert result == ["regular", "exhibition"]


class TestGetSeriesInfo:
    def test_regular_key(self):
        info = get_series_info()
        assert info["regular"]["name"] == "KBO 정규시즌"
        assert info["regular"]["since"] == 1982

    def test_wildcard_since(self):
        info = get_series_info()
        assert info["wildcard"]["since"] == 2015


class TestValidateYearSeriesCombination:
    def test_valid_combination(self):
        valid, msg = validate_year_series_combination(2025, "regular")
        assert valid
        assert "유효" in msg

    def test_year_too_early(self):
        valid, msg = validate_year_series_combination(1981, "regular")
        assert not valid
        assert "1982" in msg

    def test_series_too_early(self):
        valid, msg = validate_year_series_combination(2000, "wildcard")
        assert not valid
        assert "2015년" in msg

    def test_unknown_series(self):
        valid, msg = validate_year_series_combination(2025, "unknown_series")
        assert not valid
        assert "알 수 없는" in msg


class TestGetRecommendedSeriesForPeriod:
    def test_early_period(self):
        result = get_recommended_series_for_period(1982, 1985)
        assert "regular" in result
        assert "korean_series" in result

    def test_modern_period(self):
        result = get_recommended_series_for_period(2015, 2025)
        assert "wildcard" in result
        assert "exhibition" in result
