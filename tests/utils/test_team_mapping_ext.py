from __future__ import annotations

import pytest

from src.utils.team_mapping import TeamMapper


class TestEarlyKboFuzzyMatch:
    def test_mbc_chungryong(self):
        assert TeamMapper._early_kbo_fuzzy_match("MBC") == "LG"
        assert TeamMapper._early_kbo_fuzzy_match("청룡") == "LG"

    def test_haetae_tigers(self):
        assert TeamMapper._early_kbo_fuzzy_match("해태") == "HT"
        assert TeamMapper._early_kbo_fuzzy_match("타이거즈") == "HT"

    def test_sammi(self):
        assert TeamMapper._early_kbo_fuzzy_match("삼미") == "SM"

    def test_chungbo(self):
        assert TeamMapper._early_kbo_fuzzy_match("청보") == "CB"

    def test_no_match(self):
        assert TeamMapper._early_kbo_fuzzy_match("삼성") is None


class TestNinetiesFuzzyMatch:
    def test_binggre(self):
        assert TeamMapper._nineties_fuzzy_match("빙그레") == "BE"

    def test_taepyeongyang(self):
        assert TeamMapper._nineties_fuzzy_match("태평양") == "TP"

    def test_no_match(self):
        assert TeamMapper._nineties_fuzzy_match("LG") is None


class TestLateNinetiesFuzzyMatch:
    def test_hyundai(self):
        assert TeamMapper._late_nineties_fuzzy_match("현대") == "HU"

    def test_ssangbangwool(self):
        assert TeamMapper._late_nineties_fuzzy_match("쌍방울") == "SL"

    def test_no_match(self):
        assert TeamMapper._late_nineties_fuzzy_match("LG") is None


class TestYearSpecificFuzzyMatch:
    def test_early_year(self):
        assert TeamMapper._year_specific_fuzzy_match("MBC", 1983) == "LG"

    def test_nineties_year(self):
        assert TeamMapper._year_specific_fuzzy_match("빙그레", 1990) == "BE"

    def test_late_nineties_year(self):
        assert TeamMapper._year_specific_fuzzy_match("현대", 1998) == "HU"

    def test_no_year(self):
        assert TeamMapper._year_specific_fuzzy_match("MBC", None) is None

    def test_year_after_2000(self):
        assert TeamMapper._year_specific_fuzzy_match("MBC", 2020) is None


class TestPartialFuzzyMatch:
    def test_pattern_in_name(self):
        result = TeamMapper._partial_fuzzy_match("해태 타이거즈")
        assert result == "HT"

    def test_name_in_pattern(self):
        result = TeamMapper._partial_fuzzy_match("해태")
        assert result is not None

    def test_no_partial_match(self):
        result = TeamMapper._partial_fuzzy_match("완전히 다른 이름")
        assert result is None


class TestValidateTeamCode:
    def test_valid_current_codes(self):
        mapper = TeamMapper()
        for code in ("LG", "NC", "KT", "SS", "LT", "DB", "KIA", "HH", "KH", "SSG"):
            assert mapper.validate_team_code(code) is True

    def test_valid_historical_codes(self):
        mapper = TeamMapper()
        for code in ("CB", "SM", "TP", "SL", "OB", "HT", "WO", "SK", "NX", "HU", "MBC", "BE"):
            assert mapper.validate_team_code(code) is True

    def test_invalid_code(self):
        mapper = TeamMapper()
        assert mapper.validate_team_code("ZZ") is False

    def test_empty_code(self):
        mapper = TeamMapper()
        assert mapper.validate_team_code("") is False


class TestGetTeamCode:
    def test_none_name(self):
        mapper = TeamMapper()
        assert mapper.get_team_code(None) is None

    def test_empty_name(self):
        mapper = TeamMapper()
        assert mapper.get_team_code("") is None

    def test_whitespace_name(self):
        mapper = TeamMapper()
        result = mapper.get_team_code("   ")
        assert result is None or isinstance(result, str)

    def test_static_mapping_fallback(self):
        mapper = TeamMapper()
        result = mapper.get_team_code("LG")
        assert result is not None


class TestAddMappingForYears:
    def test_adds_year_specific(self):
        mapper = TeamMapper()
        mapper._add_mapping_for_years("테스트팀", "TS", 2020, 2022)
        assert mapper.oci_mapping["테스트팀"] == "TS"
        assert mapper.year_specific_mapping[2020]["테스트팀"] == "TS"
        assert mapper.year_specific_mapping[2021]["테스트팀"] == "TS"
        assert mapper.year_specific_mapping[2022]["테스트팀"] == "TS"


class TestGetAllTeamsForYear:
    def test_returns_static_mapping(self):
        mapper = TeamMapper()
        result = mapper.get_all_teams_for_year(2025)
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_year_specific_overrides(self):
        mapper = TeamMapper()
        mapper.year_specific_mapping[2020] = {"테스트팀": "TS"}
        result = mapper.get_all_teams_for_year(2020)
        assert "테스트팀" in result
