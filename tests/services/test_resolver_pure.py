"""Unit tests for player_id_resolver pure functions."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.services.player_id_resolver import CANONICAL_TEAM_CODES, PlayerIdResolver


@pytest.fixture
def resolver() -> PlayerIdResolver:
    session = MagicMock()
    return PlayerIdResolver(session=session, allow_unknown_registration=False)


class TestCandidateModels:
    def test_pitcher_returns_pitching(self, resolver: PlayerIdResolver) -> None:
        assert resolver._candidate_models(is_pitcher=True) == [PlayerSeasonPitching]

    def test_batter_returns_batting(self, resolver: PlayerIdResolver) -> None:
        assert resolver._candidate_models(is_pitcher=False) == [PlayerSeasonBatting]

    def test_none_returns_both(self, resolver: PlayerIdResolver) -> None:
        assert resolver._candidate_models(is_pitcher=None) == [PlayerSeasonBatting, PlayerSeasonPitching]


class TestUnknownProfileTeam:
    def test_active_teams(self, resolver: PlayerIdResolver) -> None:
        assert resolver._unknown_profile_team("LG") == "LG"
        assert resolver._unknown_profile_team("KT") == "KT"
        assert resolver._unknown_profile_team("SSG") == "SSG"
        assert resolver._unknown_profile_team("NC") == "NC"
        assert resolver._unknown_profile_team("KIA") == "KIA"
        assert resolver._unknown_profile_team("LT") == "롯데"
        assert resolver._unknown_profile_team("HH") == "한화"
        assert resolver._unknown_profile_team("DB") == "두산"

    def test_alternative_team_names(self, resolver: PlayerIdResolver) -> None:
        assert resolver._unknown_profile_team("SS") == "삼성"
        assert resolver._unknown_profile_team("LOT") == "롯데"
        assert resolver._unknown_profile_team("LOTTE") == "롯데"
        assert resolver._unknown_profile_team("OB") == "두산"
        assert resolver._unknown_profile_team("DOOSAN") == "두산"
        assert resolver._unknown_profile_team("BE") == "빙그레"
        assert resolver._unknown_profile_team("HU") == "현대"

    def test_all_star_teams(self, resolver: PlayerIdResolver) -> None:
        assert resolver._unknown_profile_team("EA") == "East"
        assert resolver._unknown_profile_team("WE") == "West"
        assert resolver._unknown_profile_team("KR") == "Korea"
        assert resolver._unknown_profile_team("JP") == "Japan"
        assert resolver._unknown_profile_team("TW") == "Taiwan"

    def test_unknown_falls_back_to_code(self, resolver: PlayerIdResolver) -> None:
        assert resolver._unknown_profile_team("ZZ") == "ZZ"


class TestCanonicalTeamCodesMap:
    @pytest.mark.parametrize(
        ("historical", "expected"),
        [
            ("OB", "DB"),
            ("SK", "SSG"),
            ("WO", "KH"),
            ("NX", "KH"),
            ("HT", "KIA"),
        ],
    )
    def test_mappings(self, historical: str, expected: str) -> None:
        assert CANONICAL_TEAM_CODES[historical] == expected

    @pytest.mark.parametrize("code", ["LG", "KT", "SSG", "NC", "KIA", "HH", "LT", "DB"])
    def test_active_codes_passthrough(self, code: str) -> None:
        assert CANONICAL_TEAM_CODES.get(code, code) == code


class TestCacheKeyCompleteMatrix:
    @pytest.mark.parametrize(
        ("name", "team", "season", "uniform", "is_pitcher", "expected_suffix"),
        [
            ("김철수", "LG", 2024, None, True, "_P"),
            ("김철수", "LG", 2024, None, False, "_B"),
            ("김철수", "LG", 2024, None, None, "_A"),
            ("김철수", None, 2024, None, True, "_P"),
            ("김철수", None, 2024, None, False, "_B"),
            ("김철수", "KT", 2026, "30", True, "_P"),
            ("최재영", "KH", 2026, None, True, "_P"),
        ],
    )
    def test_cache_key_format(
        self,
        resolver: PlayerIdResolver,
        name: str,
        team: str | None,
        season: int,
        uniform: str | None,
        is_pitcher: bool | None,
        expected_suffix: str,
    ) -> None:
        key = resolver._cache_key(name, team, season, uniform, is_pitcher=is_pitcher)
        assert key.startswith(f"{name}_{team}_{season}_")
        assert key.endswith(expected_suffix)

    def test_uniform_no_empty_string_treated_as_none(self, resolver: PlayerIdResolver) -> None:
        key_with_none = resolver._cache_key("Name", "LG", 2024, None, is_pitcher=True)
        key_with_empty = resolver._cache_key("Name", "LG", 2024, "", is_pitcher=True)
        assert key_with_none == key_with_empty


class TestResolveStaticOverride2026Cluster:
    """2026 same-name collisions — the highest-risk override group."""

    @pytest.mark.parametrize(
        ("name", "team", "season", "is_pitcher", "expected_id"),
        [
            ("김민수", "KT", 2026, True, 65048),
            ("김민수", "KT", 2026, False, 52303),
            ("최재영", "KH", 2026, True, 56338),
            ("최재영", "KH", 2026, False, 56338),
            ("최원준", "KT", 2026, True, 66606),
            ("최원준", "KT", 2026, False, 66606),
            ("김민혁", "KT", 2026, False, 64004),
            ("박지훈", "DB", 2026, True, 50204),
            ("박지훈", "DB", 2026, False, 50204),
            ("김민석", "DB", 2026, True, 53554),
            ("김민석", "DB", 2026, False, 53554),
            ("임기영", "SS", 2026, True, 62754),
            ("임기영", "SS", 2026, False, 62754),
            ("임기영", "SS", 2026, None, 62754),
            ("오재원", "HH", 2026, True, 56754),
            ("오재원", "HH", 2026, False, 56754),
            ("박시원", "NC", 2026, True, 50996),
            ("박시원", "NC", 2026, False, 50996),
            ("박시원", "NC", 2026, None, 50996),
            ("신재인", "NC", 2026, True, 56909),
            ("신재인", "NC", 2026, False, 56909),
            ("신재인", "NC", 2026, None, 56909),
            ("안우진", "KH", 2026, True, 68341),
            ("안우진", "KH", 2026, False, 68341),
            ("보쉴리", "KT", 2026, True, 56036),
            ("이형범", "KIA", 2026, True, 62951),
            ("박세진", "LT", 2026, True, 66047),
            ("김민", "SSG", 2026, True, 68043),
            ("최용준", "SSG", 2026, True, 50650),
            ("왕옌청", "HH", 2026, True, 56719),
            ("왕옌청", "HH", 2026, False, 56719),
            ("왕옌청", "HH", 2026, None, 56719),
            ("박채울", "KH", 2026, True, 54303),
            ("박채울", "KH", 2026, False, 54303),
            ("박채울", "KH", 2026, None, 54303),
            ("히우라", "KH", 2026, False, 56305),
            ("히우라", "KH", 2026, None, 56305),
            ("유민", "HH", 2026, False, 52765),
            ("유민", "HH", 2026, None, 52765),
            ("류지혁", "KIA", 2022, True, 62234),
            ("류지혁", "KIA", 2022, False, 62234),
            ("김선빈", "KIA", 2022, True, 78603),
            ("김선빈", "KIA", 2022, False, 78603),
            ("최형우", "KIA", 2022, True, 72443),
            ("최형우", "KIA", 2022, False, 72443),
            ("장현식", "KIA", 2022, True, 63950),
            ("장현식", "KIA", 2022, False, 63950),
            ("한승혁", "KIA", 2022, True, 61666),
            ("한승혁", "KIA", 2022, False, 61666),
            ("정해영", "KIA", 2022, True, 50662),
            ("정해영", "KIA", 2022, False, 50662),
            ("김태혁", "NX", 2018, True, 76430),
            ("김태혁", "NX", 2018, False, 76430),
            ("이주형", "KH", 2026, False, 50167),
            ("이주형", "KH", 2026, True, 50167),
            ("이주형", "KH", 2026, None, 50167),
            ("양현종", "KH", 2026, False, 55370),
            ("양현종", "KH", 2026, True, 55370),
            ("양현종", "KH", 2026, None, 55370),
            ("브룩스", "KH", 2026, False, 56322),
            ("브룩스", "KH", 2026, True, 56322),
            ("브룩스", "KH", 2026, None, 56322),
            ("정다훈", "KH", 2026, True, 56345),
            ("정다훈", "KH", 2026, False, 56345),
            ("타케다", "SSG", 2026, True, 56823),
            ("타케다", "SSG", 2026, False, 56823),
            ("타케다", "SSG", 2026, None, 56823),
        ],
    )
    def test_positive_match(
        self,
        resolver: PlayerIdResolver,
        name: str,
        team: str,
        season: int,
        is_pitcher: bool | None,
        expected_id: int,
    ) -> None:
        result = resolver._resolve_static_override(name, team, season, is_pitcher=is_pitcher)
        assert result == expected_id, f"Expected {expected_id} for {name}/{team}/{season}/{is_pitcher}, got {result}"

    @pytest.mark.parametrize(
        ("name", "team", "season", "is_pitcher"),
        [
            ("김민수", "KT", 2025, True),
            ("김민수", "SSG", 2026, True),
            ("최재영", "KT", 2026, True),
            ("최재영", "KH", 2025, True),
            ("박시원", "LG", 2026, False),
            ("신재인", "KT", 2026, True),
            ("브룩스", "SSG", 2026, True),
            ("이주형", "SSG", 2026, True),
        ],
    )
    def test_negative_no_match_returns_none(
        self,
        resolver: PlayerIdResolver,
        name: str,
        team: str,
        season: int,
        is_pitcher: bool | None,
    ) -> None:
        result = resolver._resolve_static_override(name, team, season, is_pitcher=is_pitcher)
        assert result is None, f"Expected no match for {name}/{team}/{season}/{is_pitcher}, got {result}"

    def test_relaxed_role_fallback(self, resolver: PlayerIdResolver) -> None:
        result = resolver._resolve_static_override("임기영", "SS", 2026, is_pitcher=None)
        assert result == 62754


class TestResolveStaticOverrideHistorical:
    """Pre-2026 overrides (전준호, 마일영, etc.)."""

    @pytest.mark.parametrize(
        ("name", "team", "season", "is_pitcher", "expected_id"),
        [
            ("전준호", "HU", 2001, False, 91511),
            ("전준호", "HU", 2001, True, 94364),
            ("전준호", "HU", 2007, True, 94364),
            ("마일영", "HU", 2001, True, 70329),
            ("마일영", "HU", 2001, False, 70329),
            ("김민재", "LT", 2001, False, 91523),
            ("양현석", "KIA", 2001, False, 70608),
            ("임선동", "HU", 2001, True, 97133),
            ("김수경", "HU", 2001, True, 98330),
            ("위재영", "HU", 2001, True, 95318),
            ("테일러", "HU", 2001, True, 2943),
        ],
    )
    def test_historical_override(
        self,
        resolver: PlayerIdResolver,
        name: str,
        team: str,
        season: int,
        is_pitcher: bool | None,
        expected_id: int,
    ) -> None:
        result = resolver._resolve_static_override(name, team, season, is_pitcher=is_pitcher)
        assert result == expected_id


class TestResolveSamsungLeeSeunghyun:
    def test_uniform_57(self, resolver: PlayerIdResolver) -> None:
        result = resolver._resolve_samsung_lee_seunghyun("이승현", "SS", 2026, "57", is_pitcher=True)
        assert result == 51454

    def test_uniform_20(self, resolver: PlayerIdResolver) -> None:
        result = resolver._resolve_samsung_lee_seunghyun("이승현", "SS", 2026, "20", is_pitcher=True)
        assert result == 60146

    def test_uniform_26(self, resolver: PlayerIdResolver) -> None:
        result = resolver._resolve_samsung_lee_seunghyun("이승현", "SS", 2026, "26", is_pitcher=True)
        assert result == 60146

    def test_no_uniform_defaults_to_57(self, resolver: PlayerIdResolver) -> None:
        result = resolver._resolve_samsung_lee_seunghyun("이승현", "SS", 2026, None, is_pitcher=True)
        assert result == 51454

    @pytest.mark.parametrize(
        ("name", "team", "season", "is_pitcher"),
        [
            ("이승현", "SS", 2026, False),
            ("이승현", "SS", 2025, True),
            ("이승현", "LG", 2026, True),
            ("김철수", "SS", 2026, True),
            ("이승현", "SS", 2026, None),
        ],
    )
    def test_negative_returns_none(
        self,
        resolver: PlayerIdResolver,
        name: str,
        team: str,
        season: int,
        is_pitcher: bool | None,
    ) -> None:
        result = resolver._resolve_samsung_lee_seunghyun(name, team, season, None, is_pitcher=is_pitcher)
        assert result is None

    def test_unknown_uniform_returns_none(self, resolver: PlayerIdResolver) -> None:
        result = resolver._resolve_samsung_lee_seunghyun("이승현", "SS", 2026, "99", is_pitcher=True)
        assert result is None


class TestResolveHanwhaParkJunyoung:
    def test_uniform_68(self, resolver: PlayerIdResolver) -> None:
        result = resolver._resolve_hanwha_park_junyoung("박준영", "HH", 2026, "68", is_pitcher=True)
        assert result == 56709

    def test_uniform_96(self, resolver: PlayerIdResolver) -> None:
        result = resolver._resolve_hanwha_park_junyoung("박준영", "HH", 2026, "96", is_pitcher=True)
        assert result == 52731

    @pytest.mark.parametrize(
        ("name", "team", "season", "is_pitcher"),
        [
            ("박준영", "HH", 2026, False),
            ("박준영", "HH", 2025, True),
            ("박준영", "LG", 2026, True),
            ("김철수", "HH", 2026, True),
            ("박준영", "HH", 2026, None),
        ],
    )
    def test_negative_returns_none(
        self,
        resolver: PlayerIdResolver,
        name: str,
        team: str,
        season: int,
        is_pitcher: bool | None,
    ) -> None:
        result = resolver._resolve_hanwha_park_junyoung(name, team, season, None, is_pitcher=is_pitcher)
        assert result is None

    def test_no_uniform_returns_none(self, resolver: PlayerIdResolver) -> None:
        result = resolver._resolve_hanwha_park_junyoung("박준영", "HH", 2026, None, is_pitcher=True)
        assert result is None

    def test_unknown_uniform_returns_none(self, resolver: PlayerIdResolver) -> None:
        result = resolver._resolve_hanwha_park_junyoung("박준영", "HH", 2026, "50", is_pitcher=True)
        assert result is None


class TestFilterSurrogateIdsEdgeCases:
    def test_single_candidate_returns_unchanged(self, resolver: PlayerIdResolver) -> None:
        result = resolver._filter_surrogate_ids({123}, "AnyName")
        assert result == {123}

    def test_empty_set_returns_empty(self, resolver: PlayerIdResolver) -> None:
        result = resolver._filter_surrogate_ids(set(), "AnyName")
        assert result == set()
