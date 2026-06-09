from src.utils.team_history import (
    FRANCHISE_CANONICAL_CODE,
    _TEAM_HISTORY,
    canonical_code_for_team_code,
    find_team_history_entry,
    franchise_id_for_team_code,
    iter_team_history,
    resolve_team_code_for_season,
)


class TestIterTeamHistory:
    def test_returns_all_entries(self):
        entries = list(iter_team_history())
        assert len(entries) > 10


class TestFindTeamHistoryEntry:
    def test_find_ss(self):
        entry = find_team_history_entry("SS")
        assert entry is not None
        assert entry.team_code == "SS"
        assert entry.franchise_id == 1

    def test_find_lg_in_1990(self):
        entry = find_team_history_entry("LG", 1990)
        assert entry is not None
        assert entry.team_code == "LG"

    def test_find_mbc_in_1985(self):
        entry = find_team_history_entry("MBC", 1985)
        assert entry is not None
        assert entry.team_code == "MBC"

    def test_find_mbc_in_1990_returns_next_brand(self):
        entry = find_team_history_entry("MBC", 1990)
        assert entry is not None
        assert entry.team_code == "LG"

    def test_find_kia_in_2000_returns_ht(self):
        entry = find_team_history_entry("KIA", 2000)
        assert entry is not None
        assert entry.team_code == "HT"

    def test_find_kia_in_2001(self):
        entry = find_team_history_entry("KIA", 2001)
        assert entry is not None
        assert entry.team_code == "KIA"

    def test_nonexistent_code(self):
        assert find_team_history_entry("ZZ") is None

    def test_hd_maps_to_hu(self):
        entry = find_team_history_entry("HD")
        assert entry is not None
        assert entry.team_code == "HU"


class TestFranchiseIdForTeamCode:
    def test_known_franchise(self):
        assert franchise_id_for_team_code("SS") == 1
        assert franchise_id_for_team_code("LG") == 3

    def test_unknown_returns_none(self):
        assert franchise_id_for_team_code("ZZ") is None


class TestCanonicalCodeForTeamCode:
    def test_known_code(self):
        assert canonical_code_for_team_code("SS") == "SS"
        assert canonical_code_for_team_code("OB") == "DB"
        assert canonical_code_for_team_code("SK") == "SSG"

    def test_unknown_returns_none(self):
        assert canonical_code_for_team_code("ZZ") is None


class TestResolveTeamCodeForSeason:
    def test_ss_all_seasons(self):
        assert resolve_team_code_for_season("SS", 1990) == "SS"
        assert resolve_team_code_for_season("SS", 2025) == "SS"

    def test_resolves_historical_brand(self):
        assert resolve_team_code_for_season("KIA", 2000) == "HT"
        assert resolve_team_code_for_season("KIA", 2025) == "KIA"

    def test_resolves_ssg_to_sk(self):
        assert resolve_team_code_for_season("SSG", 2010) == "SK"
        assert resolve_team_code_for_season("SSG", 2025) == "SSG"

    def test_unknown_returns_none(self):
        assert resolve_team_code_for_season("ZZ", 2025) is None


class TestFranchiseCanonicalCode:
    def test_all_franchises_have_canonical(self):
        assert FRANCHISE_CANONICAL_CODE[1] == "SS"
        assert FRANCHISE_CANONICAL_CODE[3] == "LG"
        assert FRANCHISE_CANONICAL_CODE[8] == "SSG"
        assert FRANCHISE_CANONICAL_CODE[11] == "KH"


class TestTeamHistoryDataIntegrity:
    def test_history_has_all_active_teams(self):
        codes = {e.team_code for e in _TEAM_HISTORY}
        for team in ("SS", "LT", "LG", "DB", "KIA", "HH", "SSG", "NC", "KT", "KH"):
            assert team in codes or any(canonical_code_for_team_code(team) or "")
