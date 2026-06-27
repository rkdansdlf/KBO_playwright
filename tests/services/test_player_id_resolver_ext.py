from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.constants import SURROGATE_PLAYER_ID_BOUNDARY
from src.services.player_id_resolver import PlayerIdResolver, PlayerIdentity


@pytest.fixture
def resolver():
    session = MagicMock()
    return PlayerIdResolver(session=session, allow_unknown_registration=False)


@pytest.fixture
def resolver_with_auto():
    session = MagicMock()
    return PlayerIdResolver(session=session, allow_unknown_registration=True)


class TestReturnAmbiguous:
    def test_returns_none_and_caches(self, resolver):
        result = resolver._return_ambiguous("key", "Name", "LG", 2024, [1, 2, 3])
        assert result is None
        assert resolver._cache["key"] is None

    def test_logs_warning(self, resolver, caplog):
        with caplog.at_level("WARNING"):
            resolver._return_ambiguous("key", "Name", "LG", 2024, [1, 2])
        assert "AMBIGUOUS PLAYER" in caplog.text


class TestFilterSurrogateIds:
    def test_multiple_surrogates_with_name_match(self, resolver):
        resolver.session.execute.return_value.fetchall.side_effect = [
            [(1, "100"), (2, "101")],
            [(100, "대상"), (101, "다른")],
        ]
        result = resolver._filter_surrogate_ids({1, 2}, "대상")
        assert 100 in result
        assert 2 in result
        assert 1 not in result

    def test_surrogate_target_missing_profile(self, resolver):
        resolver.session.execute.return_value.fetchall.side_effect = [
            [(1, "100")],
            [],
        ]
        result = resolver._filter_surrogate_ids({1, 2}, "Name")
        assert 2 in result
        assert 1 in result

    def test_no_surrogates_returns_original(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = []
        result = resolver._filter_surrogate_ids({1, 2})
        assert result == {1, 2}

    def test_name_mismatch_excludes_target(self, resolver):
        resolver.session.execute.return_value.fetchall.side_effect = [
            [(1, "100")],
            [(100, "다른이름")],
        ]
        result = resolver._filter_surrogate_ids({1}, "찾는이름")
        assert 1 in result


class TestReturnExistingUnknownOrAmbiguous:
    def test_strict_mode_returns_ambiguous(self, resolver):
        resolver.strict_game_resolution = True
        result = resolver._return_existing_unknown_or_ambiguous(
            "key",
            PlayerIdentity("Name", "LG", 2024, None, None),
            [SURROGATE_PLAYER_ID_BOUNDARY + 1, SURROGATE_PLAYER_ID_BOUNDARY + 2],
        )
        assert result is None

    def test_non_strict_finds_existing_unknown(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = [(SURROGATE_PLAYER_ID_BOUNDARY + 1,)]
        result = resolver._return_existing_unknown_or_ambiguous(
            "key",
            PlayerIdentity("Name", "LG", 2024, None, None),
            [SURROGATE_PLAYER_ID_BOUNDARY + 1],
        )
        assert result == SURROGATE_PLAYER_ID_BOUNDARY + 1

    def test_falls_to_ambiguous_when_no_existing(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = []
        result = resolver._return_existing_unknown_or_ambiguous(
            "key",
            PlayerIdentity("Name", "LG", 2024, None, None),
            [SURROGATE_PLAYER_ID_BOUNDARY + 1, SURROGATE_PLAYER_ID_BOUNDARY + 2],
        )
        assert result is None


class TestPreloadSeasonIndex:
    def test_preloads_batters_and_pitchers(self, resolver):
        resolver.session.execute.return_value.fetchall.side_effect = [
            [("김선빈", "KIA", 78603), ("김선빈", "KIA", 78603)],
            [("김선빈", "KIA", 78603)],
            [],
            [],
        ]
        resolver.preload_season_index(2022)
        assert len(resolver._cache) > 0

    def test_empty_results(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = []
        resolver.preload_season_index(2024)


class TestCacheSingleOrAmbiguous:
    def test_single_candidate_returns(self, resolver):
        result = resolver._cache_single_or_ambiguous(
            "key",
            PlayerIdentity("Name", "LG", 2024, None, None),
            {123},
        )
        assert result == 123

    def test_multiple_candidates_returns_none(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = []
        result = resolver._cache_single_or_ambiguous(
            "key",
            PlayerIdentity("Name", "LG", 2024, None, None),
            {123, 456},
        )
        assert result is None

    def test_empty_candidates_returns_none(self, resolver):
        result = resolver._cache_single_or_ambiguous(
            "key",
            PlayerIdentity("Name", "LG", 2024, None, None),
            set(),
        )
        assert result is None


class TestResolveFromSeasonStats:
    def test_allstar_skips_team_filter(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = [(123,)]
        identity = PlayerIdentity("Name", "EA", 2024, None, None)
        result = resolver._resolve_from_season_stats(identity, cache_key="key")
        assert result == 123

    def test_filters_by_team_code(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = [(456,)]
        identity = PlayerIdentity("Name", "LG", 2024, None, None)
        result = resolver._resolve_from_season_stats(identity, cache_key="key")
        assert result == 456


class TestResolveFromPlayerBasicContext:
    def test_no_kor_team_name_returns_none(self, resolver):
        result = resolver._resolve_from_player_basic_context("Name", "XX", 2024, None, "key")
        assert result is None

    def test_finds_by_team_name(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = [(789,)]
        result = resolver._resolve_from_player_basic_context("Name", "OB", 2024, None, "key")
        assert result == 789


class TestResolveByUniformNo:
    def test_no_uniform_no_returns_none(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = []
        result = resolver._resolve_by_uniform_no("Name", "LG", 2024, None, "key")
        assert result is None

    def test_finds_by_uniform(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = [(999,)]
        result = resolver._resolve_by_uniform_no("Name", "LG", 2024, "10", "key")
        assert result == 999


class TestResolveStrictGameFactsOrNone:
    def test_returns_fact_id(self, resolver):
        resolver._resolve_from_same_season_game_facts = MagicMock(return_value=123)
        identity = PlayerIdentity("Name", "LG", 2024, None, None)
        result = resolver._resolve_strict_game_facts_or_none(identity, cache_key="key")
        assert result == 123

    def test_returns_none_when_no_facts(self, resolver):
        resolver._resolve_from_same_season_game_facts = MagicMock(return_value=None)
        identity = PlayerIdentity("Name", "LG", 2024, None, None)
        result = resolver._resolve_strict_game_facts_or_none(identity, cache_key="key")
        assert result is None


class TestResolveUniqueHistoricalName:
    def test_finds_unique(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = [(111,)]
        result = resolver._resolve_unique_historical_name("Unique", "LG", 2024, None, "key")
        assert result == 111

    def test_multiple_returns_none(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = [(111,), (222,)]
        resolver._filter_surrogate_ids = MagicMock(return_value={111, 222})
        result = resolver._resolve_unique_historical_name("Dup", "LG", 2024, None, "key")
        assert result is None


class TestResolveUnknownRegistration:
    def test_disabled_returns_none(self, resolver):
        result = resolver._resolve_unknown_registration("Name", "LG", 2024, "10", "key")
        assert result is None

    def test_no_team_returns_none(self, resolver_with_auto):
        result = resolver_with_auto._resolve_unknown_registration("Name", "", 2024, "10", "key")
        assert result is None

    def test_registers_new_player(self, resolver_with_auto):
        resolver_with_auto.session.execute.return_value.fetchall.side_effect = [
            [],
            [(SURROGATE_PLAYER_ID_BOUNDARY,)],
        ]
        resolver_with_auto.session.execute.return_value.scalar.return_value = SURROGATE_PLAYER_ID_BOUNDARY
        result = resolver_with_auto._resolve_unknown_registration("NewPlayer", "LG", 2024, "10", "key")
        assert result == SURROGATE_PLAYER_ID_BOUNDARY + 1

    def test_reuses_existing_unknown(self, resolver_with_auto):
        resolver_with_auto._find_existing_unknown_player = MagicMock(return_value=SURROGATE_PLAYER_ID_BOUNDARY + 5)
        resolver_with_auto.session.execute.return_value.fetchall.return_value = []
        result = resolver_with_auto._resolve_unknown_registration("Existing", "LG", 2024, "10", "key")
        assert result == SURROGATE_PLAYER_ID_BOUNDARY + 5


class TestResolveNonStrictFallbacks:
    def test_historical_found(self, resolver):
        resolver._resolve_unique_historical_name = MagicMock(return_value=111)
        result = resolver._resolve_non_strict_fallbacks("Name", "LG", 2024, None, "key")
        assert result == 111

    def test_relaxed_found(self, resolver):
        resolver._resolve_unique_historical_name = MagicMock(return_value=None)
        resolver._resolve_relaxed_and_cache = MagicMock(return_value=222)
        result = resolver._resolve_non_strict_fallbacks("Name", "LG", 2024, None, "key")
        assert result == 222

    def test_unknown_registration(self, resolver):
        resolver._resolve_unique_historical_name = MagicMock(return_value=None)
        resolver._resolve_relaxed_and_cache = MagicMock(return_value=None)
        resolver._resolve_unknown_registration = MagicMock(return_value=333)
        result = resolver._resolve_non_strict_fallbacks("Name", "LG", 2024, None, "key")
        assert result == 333


class TestResolveId:
    def test_empty_name_returns_none(self, resolver):
        assert resolver.resolve_id("", "LG", 2024) is None

    def test_uses_override(self, resolver):
        with patch.object(resolver, "_cache", {}):
            result = resolver.resolve_id("전준호", "HU", 2001, is_pitcher=False)
            assert result == 91511

    def test_samsung_override(self, resolver):
        with patch.object(resolver, "_cache", {}):
            result = resolver.resolve_id("이승현", "SS", 2026, "57", is_pitcher=True)
            assert result == 51454

    def test_samsung_override_20_26(self, resolver):
        with patch.object(resolver, "_cache", {}):
            result = resolver.resolve_id("이승현", "SS", 2026, "20", is_pitcher=True)
            assert result == 60146

    def test_samsung_override_no_uniform(self, resolver):
        with patch.object(resolver, "_cache", {}):
            result = resolver.resolve_id("이승현", "SS", 2026, is_pitcher=True)
            assert result == 51454

    def test_hanwha_park_junyoung_68(self, resolver):
        with patch.object(resolver, "_cache", {}):
            result = resolver.resolve_id("박준영", "HH", 2026, "68", is_pitcher=True)
            assert result == 56709

    def test_hanwha_park_junyoung_96(self, resolver):
        with patch.object(resolver, "_cache", {}):
            result = resolver.resolve_id("박준영", "HH", 2026, "96", is_pitcher=True)
            assert result == 52731

    def test_strict_mode(self, resolver):
        resolver.strict_game_resolution = True
        resolver.session.execute.return_value.fetchall.return_value = []
        with patch.object(resolver, "_cache", {}):
            result = resolver.resolve_id("Unknown", "LG", 2024)
            assert result is None

    def test_non_strict_mode(self, resolver):
        resolver.strict_game_resolution = False
        resolver.session.execute.return_value.fetchall.return_value = []
        with patch.object(resolver, "_cache", {}):
            result = resolver.resolve_id("Unknown", "LG", 2024)
            assert result is None

    def test_cache_hit(self, resolver):
        resolver._cache["Test_LG_2024__A"] = 99999
        assert resolver.resolve_id("Test", "LG", 2024) == 99999

    def test_name_alias_applied(self, resolver):
        resolver.NAME_ALIASES["AliasName"] = "RealName"
        resolver.session.execute.return_value.fetchall.return_value = [(123,)]
        with patch.object(resolver, "_cache", {}):
            result = resolver.resolve_id("AliasName", "LG", 2024)
            assert result == 123


class TestRegisterUnknownPlayer:
    def test_creates_new_player(self, resolver_with_auto):
        resolver_with_auto.session.execute.return_value.fetchall.return_value = []
        resolver_with_auto.session.execute.return_value.scalar.return_value = SURROGATE_PLAYER_ID_BOUNDARY
        result = resolver_with_auto.register_unknown_player("New", "LG", "10")
        assert result == SURROGATE_PLAYER_ID_BOUNDARY + 1
        resolver_with_auto.session.add.assert_called_once()
        resolver_with_auto.session.commit.assert_called_once()

    def test_returns_none_on_error(self, resolver_with_auto):
        mock_fetchall = MagicMock()
        mock_fetchall.fetchall.return_value = []
        mock_scalar = MagicMock()
        mock_scalar.scalar.return_value = SURROGATE_PLAYER_ID_BOUNDARY
        resolver_with_auto.session.execute.side_effect = [mock_fetchall, mock_scalar]
        resolver_with_auto.session.commit.side_effect = RuntimeError("DB error")
        result = resolver_with_auto.register_unknown_player("New", "LG", "10")
        assert result is None
        resolver_with_auto.session.rollback.assert_called_once()


class TestResolveRelaxed:
    def test_single_candidate(self, resolver):
        resolver.session.execute.return_value.fetchall.side_effect = [
            [(123,)],
            [(123,)],
        ]
        result = resolver._resolve_relaxed("Name", "LG", 2024)
        assert result == 123

    def test_multiple_candidates_returns_none(self, resolver):
        resolver.session.execute.return_value.fetchall.side_effect = [
            [(123,), (456,)],
            [(123,), (456,)],
            [(123,), (456,)],
        ]
        result = resolver._resolve_relaxed("Name", "LG", 2024)
        assert result is None

    def test_fallback_to_player_basic(self, resolver):
        resolver.session.execute.return_value.fetchall.side_effect = [
            [(123,), (456,)],
            [(123,), (456,)],
            [(789,)],
        ]
        result = resolver._resolve_relaxed("Name", "LG", 2024)
        assert result == 789

    def test_allstar_skips_team_filter(self, resolver):
        resolver.session.execute.return_value.fetchall.side_effect = [
            [(123,)],
            [(123,)],
        ]
        result = resolver._resolve_relaxed("Name", "EA", 2024)
        assert result == 123


class TestFindExistingUnknownPlayer:
    def test_with_team_name(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = [(SURROGATE_PLAYER_ID_BOUNDARY + 1,)]
        result = resolver._find_existing_unknown_player("Name", "LG", "10")
        assert result == SURROGATE_PLAYER_ID_BOUNDARY + 1

    def test_without_uniform_no(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = [
            (SURROGATE_PLAYER_ID_BOUNDARY + 1,),
            (SURROGATE_PLAYER_ID_BOUNDARY + 2,),
        ]
        result = resolver._find_existing_unknown_player("Name", "LG", None)
        assert result == SURROGATE_PLAYER_ID_BOUNDARY + 1

    def test_unknown_team_no_team_filter(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = [(SURROGATE_PLAYER_ID_BOUNDARY + 1,)]
        result = resolver._find_existing_unknown_player("Name", "XX", None)
        assert result == SURROGATE_PLAYER_ID_BOUNDARY + 1


class TestResolveFromSameSeasonGameFacts:
    def test_empty_inputs(self, resolver):
        assert resolver._resolve_from_same_season_game_facts("", "LG", 2024, uniform_no=None, is_pitcher=None) is None
        assert resolver._resolve_from_same_season_game_facts("Name", "", 2024, uniform_no=None, is_pitcher=None) is None
        assert resolver._resolve_from_same_season_game_facts("Name", "LG", 0, uniform_no=None, is_pitcher=None) is None

    def test_finds_in_batting_stats(self, resolver):
        mock_connection = MagicMock()
        mock_inspect = MagicMock()
        mock_inspect.has_table.return_value = True
        mock_connection.inspect.return_value = mock_inspect
        resolver.session.connection.return_value = mock_connection
        resolver.session.execute.return_value.fetchall.return_value = [(123,)]
        result = resolver._resolve_from_same_season_game_facts("Name", "LG", 2024, uniform_no=None, is_pitcher=False)
        assert result == 123

    def test_filters_surrogate_ids(self, resolver):
        mock_connection = MagicMock()
        mock_inspect = MagicMock()
        mock_inspect.has_table.return_value = True
        mock_connection.inspect.return_value = mock_inspect
        resolver.session.connection.return_value = mock_connection
        fetchall_returns = [
            [(123,)],  # Batting
            [(123,)],  # Lineup
            [(123,)],  # Pitching
            [(123, "Name")],  # Surrogate filter query 1
            [(123, "Name")],  # Surrogate filter query 2
        ]
        resolver.session.execute.return_value.fetchall.side_effect = fetchall_returns
        result = resolver._resolve_from_same_season_game_facts("Name", "LG", 2024, uniform_no=None, is_pitcher=None)
        assert result == 123

    def test_ambiguous_returns_none(self, resolver):
        mock_connection = MagicMock()
        mock_inspect = MagicMock()
        mock_inspect.has_table.return_value = True
        mock_connection.inspect.return_value = mock_inspect
        resolver.session.connection.return_value = mock_connection
        fetchall_returns = [
            [(123,)],  # Batting
            [(456,)],  # Lineup
            [(789,)],  # Pitching
            [(123, "Name"), (456, "Other"), (789, "Another")],  # Surrogate filter
            [(123, "Name"), (456, "Other"), (789, "Another")],  # Surrogate filter
        ]
        resolver.session.execute.return_value.fetchall.side_effect = fetchall_returns
        result = resolver._resolve_from_same_season_game_facts("Name", "LG", 2024, uniform_no=None, is_pitcher=None)
        assert result is None


class TestCanonicalTeamCode:
    def test_ob_to_db(self, resolver):
        assert resolver._canonical_team_code("OB") == "DB"

    def test_sk_to_ssg(self, resolver):
        assert resolver._canonical_team_code("SK") == "SSG"

    def test_wo_to_kh(self, resolver):
        assert resolver._canonical_team_code("WO") == "KH"

    def test_unknown_unchanged(self, resolver):
        assert resolver._canonical_team_code("XX") == "XX"


class TestCacheRole:
    def test_pitcher_role(self, resolver):
        key = resolver._cache_key("Name", "LG", 2024, None, is_pitcher=True)
        assert key.endswith("_P")

    def test_batter_role(self, resolver):
        key = resolver._cache_key("Name", "LG", 2024, None, is_pitcher=False)
        assert key.endswith("_B")

    def test_any_role(self, resolver):
        key = resolver._cache_key("Name", "LG", 2024, None, is_pitcher=None)
        assert key.endswith("_A")
