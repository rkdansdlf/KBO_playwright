from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.sync.sync_games import (
    _compact_metadata_source_payload_for_limit,
    _serialized_payload_length,
)


class TestSerializedPayloadLength:
    def test_dict(self):
        assert _serialized_payload_length({"a": 1}) > 0

    def test_str(self):
        assert _serialized_payload_length("hello") == 5

    def test_none(self):
        assert _serialized_payload_length(None) == 4


class TestCompactMetadataSourcePayload:
    def test_none_payload(self):
        assert _compact_metadata_source_payload_for_limit(None, 100) is None

    def test_under_limit(self):
        payload = {"pbp_validation_status": "ok", "key": "value"}
        result = _compact_metadata_source_payload_for_limit(payload, 1000)
        assert result == payload

    def test_over_limit_truncates_to_compact(self):
        payload = {
            "pbp_validation_status": "ok",
            "parser_version": "1.0",
            "source_schema_version": "2.0",
            "payload_hash": "abc123",
            "large_field": "x" * 1000,
        }
        result = _compact_metadata_source_payload_for_limit(payload, 50)
        assert isinstance(result, dict)
        assert "pbp_validation_status" in result

    def test_non_dict_over_limit_truncated(self):
        long_str = "x" * 100
        result = _compact_metadata_source_payload_for_limit(long_str, 10)
        assert len(result) == 10

    def test_fallback_truncated(self):
        payload = {"pbp_validation_status": "error", "pbp_validation_error": "x" * 500}
        result = _compact_metadata_source_payload_for_limit(payload, 20)
        assert result == {"truncated": True}


class TestGameSyncMixin:
    @pytest.fixture
    def mixin(self):
        from src.sync.sync_games import GameSyncMixin

        instance = GameSyncMixin()
        instance.sqlite_session = MagicMock()
        instance.target_session = MagicMock()
        instance.oci_engine = MagicMock()
        instance._season_map_cache = {}
        instance._franchise_id_mapping_cache = {}
        instance._temp_table_counter = __import__("itertools").count(1)
        instance.sync_simple_table = MagicMock()
        instance._bulk_copy_upsert = MagicMock()
        instance._reset_target_sequence_for_table = MagicMock()
        instance._target_table_exists = MagicMock(return_value=True)
        instance._get_season_map = MagicMock(return_value={})
        instance.test_connection = MagicMock(return_value=True)
        return instance

    def test_sync_games_calls_sync_simple_table(self, mixin):
        mixin.sync_simple_table.return_value = 5
        result = mixin.sync_games(limit=100)
        assert result == 5
        mixin.sync_simple_table.assert_called_once()

    def test_sync_player_game_batting(self, mixin):
        mixin.sync_simple_table.return_value = 3
        result = mixin.sync_player_game_batting()
        assert result == 3
        mixin.sync_simple_table.assert_called_once()

    def test_sync_player_game_pitching(self, mixin):
        mixin.sync_simple_table.return_value = 2
        result = mixin.sync_player_game_pitching()
        assert result == 2
        mixin.sync_simple_table.assert_called_once()

    def test_sync_all_game_data(self, mixin):
        mixin.sync_game_schedules = MagicMock(return_value=1)
        mixin.sync_games = MagicMock(return_value=2)
        mixin.sync_player_game_batting = MagicMock(return_value=3)
        mixin.sync_player_game_pitching = MagicMock(return_value=4)
        result = mixin.sync_all_game_data()
        assert result == {"game_schedules": 1, "games": 2, "player_game_batting": 3, "player_game_pitching": 4}

    def test_sync_specific_game_connection_fails(self, mixin):
        mixin.test_connection = MagicMock(return_value=False)
        result = mixin.sync_specific_game("g1")
        assert result == {}

    def test_sync_pregame_game_no_game_id(self, mixin):
        result = mixin.sync_pregame_game("")
        assert result == {}

    def test_get_unsynced_or_modified_game_ids(self, mixin):
        with patch("src.sync.sync_games.detect_dirty_game_ids", return_value=["g1"]):
            result = mixin.get_unsynced_or_modified_game_ids()
            assert result == ["g1"]

    def test_purge_game_detail_children_for_year(self, mixin):
        mixin._purge_game_detail_children_for_year(2025)
        calls = mixin.target_session.execute.call_args_list
        assert len(calls) > 0

    def test_sync_game_details_connection_fails(self, mixin):
        mixin.test_connection = MagicMock(return_value=False)
        result = mixin.sync_game_details()
        assert result == {}

    def test_sync_game_details_unsynced_only(self, mixin):
        mixin.sqlite_session.query.return_value.filter.return_value.all.return_value = []
        mixin.get_unsynced_or_modified_game_ids = MagicMock(return_value=[])
        result = mixin.sync_game_details(unsynced_only=True)
        assert "games" not in result

    def test_sync_game_details_with_year(self, mixin):
        mixin.sqlite_session.query.return_value.filter.return_value.all.return_value = []
        result = mixin.sync_game_details(year=2025)
        assert isinstance(result, dict)

    def test_transform_game_lineup_keeps_starter_batting_order(self, mixin):
        data = {"is_starter": True, "batting_order": 2, "appearance_seq": 2}
        result = mixin._transform_game_lineup_for_target(data)
        assert result["batting_order"] == 2

    def test_transform_game_lineup_nulls_substitute_batting_order(self, mixin):
        data = {"is_starter": False, "batting_order": 2, "appearance_seq": 3}
        result = mixin._transform_game_lineup_for_target(data)
        assert result["batting_order"] is None

    def test_transform_game_lineup_uses_appearance_seq_over_starter_flag(self, mixin):
        data = {"is_starter": True, "batting_order": 2, "appearance_seq": 3}
        result = mixin._transform_game_lineup_for_target(data)
        assert result["batting_order"] is None


class TestGameSyncMixinMetadataPayloadLimit:
    @pytest.fixture
    def mixin(self):
        from src.sync.sync_games import GameSyncMixin

        instance = GameSyncMixin()
        instance.oci_engine = MagicMock()
        return instance

    def test_cached_limit(self, mixin):
        mixin._cached_game_metadata_source_payload_limit = 100
        assert mixin._game_metadata_source_payload_limit() == 100
