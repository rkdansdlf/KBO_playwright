from __future__ import annotations

from unittest.mock import MagicMock

import pytest


class TestPlayerSyncMixin:
    @pytest.fixture
    def mixin(self):
        from src.sync.sync_players import PlayerSyncMixin

        instance = PlayerSyncMixin()
        instance.sqlite_session = MagicMock()
        instance.target_session = MagicMock()
        instance.oci_engine = MagicMock()
        instance._season_map_cache = {}
        instance._franchise_id_mapping_cache = {}
        instance._player_id_mapping_cache = None
        instance._temp_table_counter = __import__("itertools").count(1)
        instance.sync_simple_table = MagicMock()
        instance._bulk_copy_upsert = MagicMock()
        return instance

    def test_sync_players(self, mixin):
        mixin.sync_simple_table.return_value = 10
        assert mixin.sync_players() == 10

    def test_sync_player_identities_no_mapping(self, mixin):
        mixin._get_player_id_mapping = MagicMock(return_value={})
        assert mixin.sync_player_identities() == 0

    def test_sync_player_identities_with_mapping(self, mixin):
        mixin._get_player_id_mapping = MagicMock(return_value={1: 100, 2: 200})
        mixin.sync_simple_table.return_value = 2
        assert mixin.sync_player_identities() == 2

    def test_sync_all_batting_data(self, mixin):
        mixin.sync_pitcher_data = MagicMock(return_value=5)
        mixin.sync_batting_data = MagicMock(return_value=8)
        result = mixin.sync_all_batting_data()
        assert result == {"pitcher_data": 5, "batting_data": 8}

    def test_sync_player_basic(self, mixin):
        mixin.sync_simple_table.return_value = 15
        assert mixin.sync_player_basic() == 15

    def test_sync_player_basic_by_ids_empty(self, mixin):
        assert mixin.sync_player_basic_by_ids([]) == 0

    def test_sync_player_basic_by_ids_no_players(self, mixin):
        mixin.sqlite_session.query.return_value.filter.return_value.all.return_value = []
        assert mixin.sync_player_basic_by_ids([1, 2]) == 0

    def test_sync_player_basic_by_ids_with_data(self, mixin):
        from src.models.player import PlayerBasic

        p = MagicMock(spec=PlayerBasic)
        p.player_id = 1
        p.name = "Kim"
        p.team = "SSG"
        p.position = "OF"
        p.status = "Active"
        mixin.sqlite_session.query.return_value.filter.return_value.all.return_value = [p]
        result = mixin.sync_player_basic_by_ids([1])
        assert result == 1

    def test_sync_player_movements(self, mixin):
        mixin.sync_simple_table.return_value = 6
        assert mixin.sync_player_movements() == 6

    def test_sync_fa_contracts(self, mixin):
        mixin.sync_simple_table.return_value = 4
        assert mixin.sync_fa_contracts() == 4

    def test_sync_crawl_runs(self, mixin):
        mixin.sync_simple_table.return_value = 3
        assert mixin.sync_crawl_runs() == 3

    def test_get_player_id_mapping_no_kbo_ids(self, mixin):
        mixin.sqlite_session.query.return_value.all.return_value = []
        assert mixin._get_player_id_mapping() == {}

    def test_get_player_id_mapping_cached(self, mixin):
        mixin._player_id_mapping_cache = {1: 100}
        assert mixin._get_player_id_mapping() == {1: 100}

    def test_sync_referenced_player_basic_for_games_no_game_ids(self, mixin):
        assert mixin._sync_referenced_player_basic_for_games([]) == 0

    def test_sync_referenced_player_basic_for_games_no_refs(self, mixin):
        mixin.sqlite_session.query.return_value.filter.return_value.distinct.return_value.all.return_value = []
        result = mixin._sync_referenced_player_basic_for_games(["g1"])
        assert result == 0
