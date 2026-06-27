"""Tests for team_mapping pure static methods."""

from __future__ import annotations

import pytest

from src.utils.team_codes import resolve_team_code
from src.utils.team_mapping import TeamMapper


def _make_mapper() -> TeamMapper:
    mapper = TeamMapper.__new__(TeamMapper)
    mapper._oci_loaded = False
    mapper.oci_mapping = {}
    mapper.static_mapping = {"LG": "LG", "SSG": "SSG", "KIA": "KIA"}
    mapper.year_specific_mapping = {}
    return mapper


class TestEarlyKboFuzzyMatch:
    def test_mbc_to_lg(self) -> None:
        assert TeamMapper._early_kbo_fuzzy_match("MBC") == "LG"

    def test_haetae_to_ht(self) -> None:
        assert TeamMapper._early_kbo_fuzzy_match("해태") == "HT"

    def test_tigers_to_ht(self) -> None:
        assert TeamMapper._early_kbo_fuzzy_match("타이거즈") == "HT"

    def test_chungbo_to_cb(self) -> None:
        assert TeamMapper._early_kbo_fuzzy_match("청보") == "CB"

    def test_no_match(self) -> None:
        assert TeamMapper._early_kbo_fuzzy_match("UNKNOWN") is None


class TestNinetiesFuzzyMatch:
    def test_binggre_to_be(self) -> None:
        assert TeamMapper._nineties_fuzzy_match("빙그레") == "BE"

    def test_taepyeongyang_to_tp(self) -> None:
        assert TeamMapper._nineties_fuzzy_match("태평양") == "TP"

    def test_no_match(self) -> None:
        assert TeamMapper._nineties_fuzzy_match("UNKNOWN") is None


class TestLateNinetiesFuzzyMatch:
    def test_hyundai_to_hu(self) -> None:
        assert TeamMapper._late_nineties_fuzzy_match("현대") == "HU"

    def test_no_match(self) -> None:
        assert TeamMapper._late_nineties_fuzzy_match("UNKNOWN") is None


class TestYearSpecificFuzzyMatch:
    def test_early_year_branch(self) -> None:
        mapper = _make_mapper()
        result = mapper._year_specific_fuzzy_match("MBC", 1983)
        assert result == "LG"

    def test_nineties_branch(self) -> None:
        mapper = _make_mapper()
        result = mapper._year_specific_fuzzy_match("빙그레", 1995)
        assert result == "BE"

    def test_late_nineties_branch(self) -> None:
        mapper = _make_mapper()
        result = mapper._year_specific_fuzzy_match("현대", 1998)
        assert result == "HU"


class TestGetTeamCode:
    def test_mbc_resolves_via_fuzzy(self) -> None:
        mapper = _make_mapper()
        assert mapper.get_team_code("MBC", 1990) == "LG"

    def test_empty_returns_none(self) -> None:
        mapper = _make_mapper()
        assert mapper.get_team_code("", 2020) is None


class TestValidateTeamCode:
    def test_valid_code(self) -> None:
        mapper = _make_mapper()
        assert mapper.validate_team_code("LG") is True

    def test_historical_code(self) -> None:
        mapper = _make_mapper()
        assert mapper.validate_team_code("CB") is True

    def test_empty_returns_false(self) -> None:
        mapper = _make_mapper()
        assert mapper.validate_team_code("") is False

    def test_invalid_code(self) -> None:
        mapper = _make_mapper()
        assert mapper.validate_team_code("XX") is False


class TestGetAllTeamsForYear:
    def test_default_year_returns_dict(self) -> None:
        mapper = _make_mapper()
        teams = mapper.get_all_teams_for_year(2020)
        assert isinstance(teams, dict)

    def test_with_year_specific_override(self) -> None:
        mapper = _make_mapper()
        mapper.year_specific_mapping = {2020: {"TEST": "T1"}}
        teams = mapper.get_all_teams_for_year(2020)
        assert "TEST" in teams

    def test_different_year_no_override(self) -> None:
        mapper = _make_mapper()
        mapper.year_specific_mapping = {2020: {"TEST": "T1"}}
        teams = mapper.get_all_teams_for_year(2021)
        assert "TEST" not in teams
