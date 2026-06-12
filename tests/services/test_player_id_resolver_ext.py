from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.services.player_id_resolver import PlayerIdResolver


@pytest.fixture
def resolver():
    session = MagicMock()
    return PlayerIdResolver(session=session, allow_unknown_registration=False)


class TestLoadAliasesFromCsv:
    def test_missing_file_returns_empty(self):
        aliases = PlayerIdResolver._load_aliases_from_csv()
        assert isinstance(aliases, dict)

    def test_cache_set_after_resolve(self, resolver):
        resolver._cache["test_key"] = 42
        assert resolver._cache["test_key"] == 42


class TestFilterSurrogateIds:
    def test_single_id_returns_as_is(self, resolver):
        assert resolver._filter_surrogate_ids({1}) == {1}

    def test_empty_returns_empty(self, resolver):
        assert resolver._filter_surrogate_ids(set()) == set()

    def test_replaces_with_kbo_person_id(self, resolver):
        resolver.session.execute.return_value.fetchall.side_effect = [
            [(1, "100")],
            [(100, "대상선수")],
        ]
        result = resolver._filter_surrogate_ids({1, 2})
        assert 100 in result


class TestResolveId:
    def test_empty_name_returns_none(self, resolver):
        assert resolver.resolve_id("", "LG", 2024) is None

    def test_uses_static_overrides(self, resolver):
        with patch.object(resolver, "_cache", {}):
            result = resolver.resolve_id("전준호", "HU", 2001, is_pitcher=False)
            assert result == 91511

    def test_returns_cached_result(self, resolver):
        resolver._cache["Kim_LG_2024__A"] = 12345
        assert resolver.resolve_id("Kim", "LG", 2024) == 12345

    def test_standardizes_team_code(self, resolver):
        resolver.resolve_id("Kim", "SK", 2024)
        call_args = resolver.session.execute.call_args_list
        for call in call_args:
            _, kwargs = call
            params = kwargs.get("params", {})
            if "team_code" in params:
                assert params["team_code"] != "SK"

    def test_relaxed_fallback_returns_none_when_no_match(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = []
        result = resolver.resolve_id("UnknownPlayer", "LG", 2024)
        assert result is None


class TestRegisterUnknownPlayer:
    def test_returns_existing_unknown(self, resolver):
        resolver.allow_unknown_registration = True
        resolver.session.execute.return_value.fetchall.side_effect = [
            [MagicMock(_mocks=(900001,))],
            [MagicMock(_mocks=(900001,))],
        ]
        resolver._find_existing_unknown_player = MagicMock(return_value=900001)
        result = resolver.register_unknown_player("Test", "LG", "10")
        assert result == 900001


class TestResolveRelaxed:
    def test_returns_none_when_no_models_match(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = []
        result = resolver._resolve_relaxed("Nobody", "LG", 2024)
        assert result is None


class TestFindExistingUnknownPlayer:
    def test_no_match_returns_none(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = []
        result = resolver._find_existing_unknown_player("Test", "LG", "10")
        assert result is None

    def test_match_returns_id(self, resolver):
        resolver.session.execute.return_value.fetchall.return_value = [(900001,)]
        result = resolver._find_existing_unknown_player("Test", "LG", "10")
        assert result == 900001
