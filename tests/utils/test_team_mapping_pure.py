"""Tests for team_mapping — pure team name mapping logic."""

from __future__ import annotations

from src.utils.team_mapping import TeamMapper


class TestTeamMapperInit:
    def test_static_mapping_populated(self) -> None:
        mapper = TeamMapper()
        assert "LG" in mapper.static_mapping
        assert "삼성" in mapper.static_mapping
        assert mapper.static_mapping["LG"] == "LG"

    def test_oci_not_loaded_initially(self) -> None:
        mapper = TeamMapper()
        assert mapper._oci_loaded is False


class TestGetTeamCode:
    def test_direct_static_match(self) -> None:
        mapper = TeamMapper()
        assert mapper.get_team_code("LG") == "LG"

    def test_korean_name_match(self) -> None:
        mapper = TeamMapper()
        assert mapper.get_team_code("삼성") == "SS"

    def test_full_team_name(self) -> None:
        mapper = TeamMapper()
        assert mapper.get_team_code("LG트윈스") == "LG"

    def test_empty_string_returns_none(self) -> None:
        mapper = TeamMapper()
        assert mapper.get_team_code("") is None

    def test_none_returns_none(self) -> None:
        mapper = TeamMapper()
        assert mapper.get_team_code(None) is None

    def test_whitespace_stripped(self) -> None:
        mapper = TeamMapper()
        assert mapper.get_team_code("  LG  ") == "LG"

    def test_unknown_team_returns_none_or_fuzzy(self) -> None:
        mapper = TeamMapper()
        result = mapper.get_team_code("UNKNOWN_TEAM_XYZ")
        assert result is None or isinstance(result, str)


class TestHistoricalPatterns:
    def test_ob_bears_mapping(self) -> None:
        mapper = TeamMapper()
        assert mapper.get_team_code("OB베어스") == "OB"

    def test_doosan_historical(self) -> None:
        mapper = TeamMapper()
        assert mapper.get_team_code("두산베어스") in ("DB", "OB")

    def test_nc_dinos(self) -> None:
        mapper = TeamMapper()
        assert mapper.get_team_code("NC다이노스") == "NC"


class TestPartialFuzzyMatch:
    def test_partial_match_substring(self) -> None:
        mapper = TeamMapper()
        result = mapper._partial_fuzzy_match("OB베어스")
        assert result == "OB"

    def test_partial_match_contains(self) -> None:
        mapper = TeamMapper()
        result = mapper._partial_fuzzy_match("삼성라이온즈")
        assert result == "SS"

    def test_no_partial_match(self) -> None:
        mapper = TeamMapper()
        assert mapper._partial_fuzzy_match("XYZUNKNOWN") is None


class TestYearSpecificFuzzyMatch:
    def test_early_kbo_mbc(self) -> None:
        mapper = TeamMapper()
        assert mapper._year_specific_fuzzy_match("MBC청룡", 1983) == "LG"

    def test_early_kbo_haetae(self) -> None:
        mapper = TeamMapper()
        assert mapper._year_specific_fuzzy_match("해태타이거즈", 1985) == "HT"

    def test_nineties_binggre(self) -> None:
        mapper = TeamMapper()
        assert mapper._year_specific_fuzzy_match("빙그레이글스", 1990) == "BE"

    def test_late_nineties_hyundai(self) -> None:
        mapper = TeamMapper()
        assert mapper._year_specific_fuzzy_match("현대유니콘스", 1998) == "HU"

    def test_no_match_returns_none(self) -> None:
        mapper = TeamMapper()
        assert mapper._year_specific_fuzzy_match("알수없음", 2020) is None

    def test_no_year_returns_none(self) -> None:
        mapper = TeamMapper()
        assert mapper._year_specific_fuzzy_match("MBC", None) is None


class TestEarlyKboFuzzyMatch:
    def test_mbc(self) -> None:
        assert TeamMapper._early_kbo_fuzzy_match("MBC청룡") == "LG"

    def test_chungryong(self) -> None:
        assert TeamMapper._early_kbo_fuzzy_match("청룡") == "LG"

    def test_haetae(self) -> None:
        assert TeamMapper._early_kbo_fuzzy_match("해태") == "HT"

    def test_sammi(self) -> None:
        assert TeamMapper._early_kbo_fuzzy_match("삼미") == "SM"

    def test_cheongbo(self) -> None:
        assert TeamMapper._early_kbo_fuzzy_match("청보") == "CB"

    def test_no_match(self) -> None:
        assert TeamMapper._early_kbo_fuzzy_match("없음") is None


class TestNinetiesFuzzyMatch:
    def test_binggre(self) -> None:
        assert TeamMapper._nineties_fuzzy_match("빙그레") == "BE"

    def test_taepyeongyang(self) -> None:
        assert TeamMapper._nineties_fuzzy_match("태평양") == "TP"

    def test_no_match(self) -> None:
        assert TeamMapper._nineties_fuzzy_match("없음") is None


class TestLateNinetiesFuzzyMatch:
    def test_hyundai(self) -> None:
        assert TeamMapper._late_nineties_fuzzy_match("현대") == "HU"

    def test_ssangbangwool(self) -> None:
        assert TeamMapper._late_nineties_fuzzy_match("쌍방울") == "SL"

    def test_no_match(self) -> None:
        assert TeamMapper._late_nineties_fuzzy_match("없음") is None


class TestGetAllTeamsForYear:
    def test_returns_static_mapping(self) -> None:
        mapper = TeamMapper()
        result = mapper.get_all_teams_for_year(2025)
        assert "LG" in result
        assert "NC" in result

    def test_returns_dict(self) -> None:
        mapper = TeamMapper()
        result = mapper.get_all_teams_for_year(2020)
        assert isinstance(result, dict)


class TestValidateTeamCode:
    def test_valid_current_code(self) -> None:
        mapper = TeamMapper()
        assert mapper.validate_team_code("LG") is True

    def test_valid_historical_code(self) -> None:
        mapper = TeamMapper()
        assert mapper.validate_team_code("OB") is True

    def test_invalid_code(self) -> None:
        mapper = TeamMapper()
        assert mapper.validate_team_code("ZZ") is False

    def test_empty_code(self) -> None:
        mapper = TeamMapper()
        assert mapper.validate_team_code("") is False

    def test_none_code(self) -> None:
        mapper = TeamMapper()
        assert mapper.validate_team_code(None) is False
