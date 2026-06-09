"""Tests for team_mapping — KBO team name-to-code mapping."""

from unittest.mock import MagicMock, patch

import pytest

from src.utils.team_mapping import TeamMapper


@pytest.fixture
def mapper():
    return TeamMapper()


class TestStaticMapping:
    def test_current_teams(self, mapper):
        assert mapper.static_mapping["LG"] == "LG"
        assert mapper.static_mapping["NC"] == "NC"
        assert mapper.static_mapping["삼성"] == "SS"
        assert mapper.static_mapping["롯데"] == "LT"
        assert mapper.static_mapping["두산"] == "DB"
        assert mapper.static_mapping["KIA"] == "KIA"
        assert mapper.static_mapping["한화"] == "HH"
        assert mapper.static_mapping["키움"] == "KH"
        assert mapper.static_mapping["SSG"] == "SSG"

    def test_full_names(self, mapper):
        assert mapper.static_mapping["LG트윈스"] == "LG"
        assert mapper.static_mapping["삼성라이온즈"] == "SS"

    def test_size(self, mapper):
        assert len(mapper.static_mapping) >= 19


class TestGetTeamCode:
    def test_exact_static_match(self, mapper):
        assert mapper.get_team_code("LG", 2025) == "LG"

    def test_korean_name(self, mapper):
        assert mapper.get_team_code("삼성", 2025) == "SS"

    def test_empty_name(self, mapper):
        assert mapper.get_team_code("") is None

    def test_none_name(self, mapper):
        assert mapper.get_team_code(None) is None


class TestFuzzyMatch:
    def test_ob_bears(self, mapper):
        assert mapper._fuzzy_match("OB") == "OB"

    def test_mbc_cheongryong(self, mapper):
        assert mapper._fuzzy_match("MBC") == "MBC"

    def test_binggrae(self, mapper):
        assert mapper._fuzzy_match("빙그레") == "BE"

    def test_haeTae(self, mapper):
        assert mapper._fuzzy_match("해태") == "HT"

    def test_hyundai(self, mapper):
        assert mapper._fuzzy_match("현대") == "HU"

    def test_nexen(self, mapper):
        assert mapper._fuzzy_match("넥센") == "NX"

    def test_sk(self, mapper):
        assert mapper._fuzzy_match("SK") == "SK"

    def test_ssangbangwool(self, mapper):
        assert mapper._fuzzy_match("쌍방울") == "SL"

    def test_cheongbo(self, mapper):
        assert mapper._fuzzy_match("청보") == "CB"

    def test_sammi(self, mapper):
        assert mapper._fuzzy_match("삼미") == "SM"

    def test_taepyungyang(self, mapper):
        assert mapper._fuzzy_match("태평양") == "TP"

    def test_partial_substring(self, mapper):
        assert mapper._fuzzy_match("OB베어스") == "OB"

    def test_unknown_returns_none(self, mapper):
        assert mapper._fuzzy_match("없는팀") is None

    def test_year_specific_mbc_early(self, mapper):
        # MBC in early years maps to LG (franchise continuity)
        assert mapper._fuzzy_match("MBC", 1983) == "MBC"

    def test_year_specific_hyundai(self, mapper):
        assert mapper._fuzzy_match("현대", 1998) == "HU"

    def test_year_specific_old_teams(self, mapper):
        result = mapper._fuzzy_match("삼미", 1983)
        assert result == "SM"

    def test_year_specific_cheongbo(self, mapper):
        assert mapper._fuzzy_match("청보", 1983) == "CB"

    def test_year_specific_taepyungyang_1990(self, mapper):
        assert mapper._fuzzy_match("태평양", 1990) == "TP"

    def test_year_specific_ssangbangwool(self, mapper):
        assert mapper._fuzzy_match("쌍방울", 1998) == "SL"


class TestValidateTeamCode:
    def test_valid_current_code(self, mapper):
        assert mapper.validate_team_code("LG")
        assert mapper.validate_team_code("SS")
        assert mapper.validate_team_code("KIA")

    def test_valid_historical_code(self, mapper):
        assert mapper.validate_team_code("OB")
        assert mapper.validate_team_code("HT")
        assert mapper.validate_team_code("SK")

    def test_invalid_code(self, mapper):
        assert not mapper.validate_team_code("XYZ")

    def test_empty_code(self, mapper):
        assert not mapper.validate_team_code("")

    def test_none_code(self, mapper):
        assert not mapper.validate_team_code(None)


class TestGetAllTeamsForYear:
    def test_returns_static_for_unloaded_year(self, mapper):
        mapping = mapper.get_all_teams_for_year(1990)
        assert "LG" in mapping
        assert mapping["LG"] == "LG"

    def test_includes_all_current_teams(self, mapper):
        mapping = mapper.get_all_teams_for_year(2025)
        assert len(mapping) >= 10


class TestLoadOciMapping:
    @patch("src.db.engine.get_oci_url")
    def test_oci_url_not_set(self, mock_get_oci_url, mapper):
        mock_get_oci_url.return_value = None
        result = mapper.load_oci_mapping()
        assert not result
        assert not mapper._oci_loaded

    @patch("src.db.engine.get_oci_url")
    def test_oci_url_set_but_no_table(self, mock_get_oci_url, mapper):
        mock_get_oci_url.return_value = "sqlite:///:memory:"
        result = mapper.load_oci_mapping()
        # Will fail at information_schema query but not crash
        assert isinstance(result, bool)
