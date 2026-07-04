from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameHighlight,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GamePlayByPlay,
    GameSummary,
    GameValidationMetrics,
    PlayerGameBatting,
    PlayerGamePitching,
)
from src.sync.sync_base import GameSyncEligibility
from src.sync.sync_games import (
    GameSyncMixin,
    _compact_metadata_source_payload_for_limit,
    _serialized_payload_length,
)


@pytest.fixture
def mixin():
    instance = GameSyncMixin()
    instance.sqlite_session = MagicMock()
    instance.target_session = MagicMock()
    instance.oci_engine = MagicMock()
    instance._season_map_cache = {}
    instance._franchise_id_mapping_cache = {}
    instance._temp_table_counter = __import__("itertools").count(1)
    instance.sync_simple_table = MagicMock(return_value=0)
    instance._bulk_copy_upsert = MagicMock()
    instance._reset_target_sequence_for_table = MagicMock()
    instance._target_table_exists = MagicMock(return_value=True)
    instance._get_season_map = MagicMock(return_value={})
    instance.test_connection = MagicMock(return_value=True)
    instance._run_target_session_with_retries = MagicMock(side_effect=lambda operation, **_kwargs: operation())
    instance._sync_referenced_player_basic_for_games = MagicMock(return_value=0)
    return instance


class TestCompactPayloadEdgeCases:
    def test_empty_dict_over_limit(self):
        payload = {"large": "x" * 100}
        result = _compact_metadata_source_payload_for_limit(payload, 5)
        assert result == {"truncated": True}

    def test_status_fallback_with_truncated(self):
        payload = {"pbp_validation_status": "ok" * 100, "other": "x" * 200}
        result = _compact_metadata_source_payload_for_limit(payload, 10)
        assert result == {"truncated": True}

    def test_status_fallback_under_limit(self):
        payload = {"pbp_validation_status": "ok", "large": "x" * 500}
        result = _compact_metadata_source_payload_for_limit(payload, 50)
        assert "pbp_validation_status" in result

    def test_zero_limit_returns_payload(self):
        payload = {"key": "value"}
        result = _compact_metadata_source_payload_for_limit(payload, 0)
        assert result == payload

    def test_list_payload(self):
        payload = [1, 2, 3, 4, 5]
        result = _compact_metadata_source_payload_for_limit(payload, 100)
        assert result == payload


class TestGameMetadataSourcePayloadLimit:
    def test_no_oci_engine_returns_none(self):
        instance = GameSyncMixin()
        instance.oci_engine = None
        assert instance._game_metadata_source_payload_limit() is None

    def test_inspect_finds_column_length(self):
        instance = GameSyncMixin()
        mock_inspect = MagicMock()
        mock_column = {"name": "source_payload", "type": MagicMock(length=2000)}
        mock_inspect.get_columns.return_value = [mock_column]
        with patch("src.sync.sync_games.inspect", return_value=mock_inspect):
            instance.oci_engine = MagicMock()
            result = instance._game_metadata_source_payload_limit()
        assert result == 2000

    def test_inspect_no_matching_column(self):
        instance = GameSyncMixin()
        mock_inspect = MagicMock()
        mock_inspect.get_columns.return_value = [{"name": "other", "type": MagicMock(length=100)}]
        with patch("src.sync.sync_games.inspect", return_value=mock_inspect):
            instance.oci_engine = MagicMock()
            result = instance._game_metadata_source_payload_limit()
        assert result is None

    def test_sqlalchemy_error_returns_none(self):
        from sqlalchemy.exc import SQLAlchemyError

        instance = GameSyncMixin()
        instance.oci_engine = MagicMock()
        with patch("src.sync.sync_games.inspect", side_effect=SQLAlchemyError("fail")):
            result = instance._game_metadata_source_payload_limit()
        assert result is None


class TestSyncGamesTransform:
    def test_sync_games_unmapped_season_id(self, mixin):
        mixin._get_season_map = MagicMock(return_value={})
        mixin.sync_simple_table = MagicMock(return_value=0)
        result = mixin.sync_games()
        assert result == 0

    def test_sync_games_with_mapped_season(self, mixin):
        mixin._get_season_map = MagicMock(return_value={(2025, 1): 10})
        mixin.sync_simple_table = MagicMock(return_value=5)
        result = mixin.sync_games()
        assert result == 5

    def test_sync_games_large_season_id(self, mixin):
        mixin._get_season_map = MagicMock(return_value={(2025, 1): 10})
        mixin.sync_simple_table = MagicMock(return_value=3)
        result = mixin.sync_games()
        assert result == 3


class TestSyncPlayerGameBatting:
    def test_with_year_filter(self, mixin):
        mixin.sync_simple_table = MagicMock(return_value=10)
        result = mixin.sync_player_game_batting(year=2025)
        assert result == 10
        call_args = mixin.sync_simple_table.call_args
        assert call_args[1]["filters"] is not None

    def test_without_year_filter(self, mixin):
        mixin.sync_simple_table = MagicMock(return_value=5)
        result = mixin.sync_player_game_batting()
        assert result == 5
        call_args = mixin.sync_simple_table.call_args
        assert call_args[1]["filters"] is None


class TestSyncPlayerGamePitching:
    def test_with_year_filter(self, mixin):
        mixin.sync_simple_table = MagicMock(return_value=8)
        result = mixin.sync_player_game_pitching(year=2025)
        assert result == 8

    def test_without_year_filter(self, mixin):
        mixin.sync_simple_table = MagicMock(return_value=4)
        result = mixin.sync_player_game_pitching()
        assert result == 4


class TestSyncGameSchedules:
    def test_stub_returns_zero(self, mixin):
        result = mixin.sync_game_schedules()
        assert result == 0


class TestGameDetailParentScope:
    def test_unsynced_only_with_no_targets(self, mixin):
        mixin.get_unsynced_or_modified_game_ids = MagicMock(return_value=[])
        with patch("src.sync.sync_games.filter_game_ids_by_year", return_value=[]):
            filters, target_ids = mixin._game_detail_parent_scope(None, None, unsynced_only=True)
        assert target_ids == []

    def test_unsynced_only_with_targets(self, mixin):
        mixin.get_unsynced_or_modified_game_ids = MagicMock(return_value=["g1", "g2"])
        with patch("src.sync.sync_games.filter_game_ids_by_year", return_value=["g1"]):
            filters, target_ids = mixin._game_detail_parent_scope(None, 2025, unsynced_only=True)
        assert target_ids == ["g1"]
        assert len(filters) == 1

    def test_days_filter(self, mixin):
        filters, target_ids = mixin._game_detail_parent_scope(days=7, year=None, unsynced_only=False)
        assert len(filters) == 1
        assert target_ids is None

    def test_year_filter(self, mixin):
        filters, target_ids = mixin._game_detail_parent_scope(None, year=2025, unsynced_only=False)
        assert len(filters) == 1
        assert target_ids is None

    def test_days_and_year_filters(self, mixin):
        filters, target_ids = mixin._game_detail_parent_scope(days=7, year=2025, unsynced_only=False)
        assert len(filters) == 2


class TestScopedGameIds:
    def test_with_target_game_ids(self, mixin):
        result = mixin._scoped_game_ids([], ["g1", "g2"])
        assert result == ["g1", "g2"]

    def test_with_filters(self, mixin):
        mock_query = MagicMock()
        mock_query.filter.return_value.all.return_value = [("g1",), ("g2",)]
        mixin.sqlite_session.query.return_value = mock_query
        result = mixin._scoped_game_ids(["some_filter"], None)
        assert result == ["g1", "g2"]

    def test_without_filters(self, mixin):
        mock_query = MagicMock()
        mock_query.all.return_value = [("g1",)]
        mixin.sqlite_session.query.return_value = mock_query
        result = mixin._scoped_game_ids([], None)
        assert result == ["g1"]


class TestSyncParentGamesForDetails:
    def test_unsynced_only_with_publishable_ids(self, mixin):
        results = {}
        mixin.sync_games = MagicMock(return_value=5)
        mixin._sync_parent_games_for_details(
            results,
            [],
            ["g1", "g2"],
            unsynced_only=True,
            batch_size=1000,
        )
        assert results["games"] == 5

    def test_unsynced_only_with_empty_publishable_ids(self, mixin):
        results = {}
        mixin._sync_parent_games_for_details(
            results,
            [],
            [],
            unsynced_only=True,
            batch_size=1000,
        )
        assert results["games"] == 0

    def test_not_unsynced_only(self, mixin):
        results = {}
        mixin.sync_games = MagicMock(return_value=10)
        mixin._sync_parent_games_for_details(
            results,
            ["filter1"],
            None,
            unsynced_only=False,
            batch_size=1000,
        )
        assert results["games"] == 10


class TestSyncGameIdAliases:
    def test_with_target_game_ids(self, mixin):
        from src.sync.sync_games import GameDetailSyncScope

        results = {}
        scope = GameDetailSyncScope(
            scoped_game_ids=["g1"],
            filters=[],
            target_game_ids=["g1"],
            year=None,
            days=None,
            unsynced_only=False,
            batch_size=1000,
        )
        mixin.sync_simple_table = MagicMock(return_value=2)
        mixin._sync_game_id_aliases(results, scope)
        assert results["game_id_aliases"] == 2

    def test_with_year(self, mixin):
        from src.sync.sync_games import GameDetailSyncScope

        results = {}
        scope = GameDetailSyncScope(
            scoped_game_ids=[],
            filters=[],
            target_game_ids=None,
            year=2025,
            days=None,
            unsynced_only=False,
            batch_size=1000,
        )
        mixin.sync_simple_table = MagicMock(return_value=3)
        mixin._sync_game_id_aliases(results, scope)
        assert results["game_id_aliases"] == 3

    def test_with_days_and_filters(self, mixin):
        from src.sync.sync_games import GameDetailSyncScope

        results = {}
        scope = GameDetailSyncScope(
            scoped_game_ids=[],
            filters=["f1"],
            target_game_ids=None,
            year=None,
            days=7,
            unsynced_only=False,
            batch_size=1000,
        )
        mock_game = MagicMock()
        mock_game.game_id = "g1"
        mock_query = MagicMock()
        mock_query.filter.return_value.all.return_value = [mock_game]
        mixin.sqlite_session.query.return_value = mock_query
        mixin.sync_simple_table = MagicMock(return_value=4)
        mixin._sync_game_id_aliases(results, scope)
        assert results["game_id_aliases"] == 4

    def test_with_empty_game_ids_from_days(self, mixin):
        from src.sync.sync_games import GameDetailSyncScope

        results = {}
        scope = GameDetailSyncScope(
            scoped_game_ids=[],
            filters=["f1"],
            target_game_ids=None,
            year=None,
            days=7,
            unsynced_only=False,
            batch_size=1000,
        )
        mock_query = MagicMock()
        mock_query.filter.return_value.all.return_value = []
        mixin.sqlite_session.query.return_value = mock_query
        mixin._sync_game_id_aliases(results, scope)
        assert "game_id_aliases" not in results


class TestGameDetailChildFilters:
    def test_with_year(self, mixin):
        result = mixin._game_detail_child_filters([], year=2025, days=None)
        assert result is not None
        assert len(result) == 1

    def test_with_days_and_games(self, mixin):
        mock_query = MagicMock()
        mock_query.filter.return_value.all.return_value = [MagicMock(game_id="g1")]
        mixin.sqlite_session.query.return_value = mock_query
        result = mixin._game_detail_child_filters(["f1"], year=None, days=7)
        assert result is not None

    def test_with_days_and_no_games(self, mixin):
        mock_query = MagicMock()
        mock_query.filter.return_value.all.return_value = []
        mixin.sqlite_session.query.return_value = mock_query
        result = mixin._game_detail_child_filters(["f1"], year=None, days=7)
        assert result == []

    def test_no_year_no_days(self, mixin):
        result = mixin._game_detail_child_filters([], year=None, days=None)
        assert result is None


class TestPrepareTargetGameDetailChildren:
    def test_year_not_unsynced_purges(self, mixin):
        mixin._purge_game_detail_children_for_year = MagicMock()
        mixin._prepare_target_game_detail_children(2025, unsynced_only=False, eligibility=MagicMock())
        mixin._purge_game_detail_children_for_year.assert_called_once_with(2025)

    def test_unsynced_only_replaces(self, mixin):
        eligibility = MagicMock()
        eligibility.detail_game_ids = ["g1"]
        eligibility.relay_game_ids = ["g2"]
        mixin._replace_target_child_rows_for_games = MagicMock()
        mixin._prepare_target_game_detail_children(None, unsynced_only=True, eligibility=eligibility)
        assert mixin._replace_target_child_rows_for_games.call_count == 2


class TestReplaceTargetChildRowsForGames:
    def test_empty_game_ids_returns_early(self, mixin):
        mixin._replace_target_child_rows_for_games(GameMetadata, [], label="test")
        mixin.target_session.query.assert_not_called()

    def test_no_target_session_returns_early(self, mixin):
        mixin.target_session = None
        mixin._replace_target_child_rows_for_games(GameMetadata, ["g1"], label="test")

    def test_with_tuple_of_models(self, mixin):
        mixin._target_table_exists = MagicMock(return_value=True)
        mixin._replace_target_child_rows_for_games(
            (GameMetadata, GameInningScore),
            ["g1"],
            label="test",
        )
        assert mixin.target_session.query.call_count == 2

    def test_deduplicates_game_ids(self, mixin):
        mixin._target_table_exists = MagicMock(return_value=True)
        mixin._replace_target_child_rows_for_games(
            (GameMetadata,),
            ["g1", "g1", "g2"],
            label="test",
        )
        mixin.target_session.query.assert_called_once_with(GameMetadata)


class TestSyncGameDetailsForIds:
    def test_empty_game_ids_returns_empty(self, mixin):
        result = mixin.sync_game_details_for_ids([])
        assert result == {}

    def test_connection_fails_returns_empty(self, mixin):
        mixin.test_connection = MagicMock(return_value=False)
        result = mixin.sync_game_details_for_ids(["g1"])
        assert result == {}

    def test_deduplicates_ids(self, mixin):
        mixin._aggregate_game_detail_chunks = MagicMock(return_value={})
        mixin.sync_game_details_for_ids(["g1", "g1", "g2"])
        call_args = mixin._aggregate_game_detail_chunks.call_args
        scope = call_args[0][0]
        assert len(scope.scoped_game_ids) == 2


class TestAggregateGameDetailChunks:
    def test_empty_scoped_ids(self, mixin):
        from src.sync.sync_games import GameDetailSyncScope

        scope = GameDetailSyncScope(
            scoped_game_ids=[],
            filters=[],
            target_game_ids=None,
            year=None,
            days=None,
            unsynced_only=False,
            batch_size=1000,
        )
        result = mixin._aggregate_game_detail_chunks(scope)
        assert result == {}

    def test_multiple_chunks(self, mixin):
        from src.sync.sync_games import GameDetailSyncScope

        mixin._sync_game_detail_chunk = MagicMock(return_value={"games": 5, "metadata": 3})
        scope = GameDetailSyncScope(
            scoped_game_ids=[f"g{i}" for i in range(25)],
            filters=[],
            target_game_ids=None,
            year=None,
            days=None,
            unsynced_only=False,
            batch_size=1000,
        )
        result = mixin._aggregate_game_detail_chunks(scope)
        assert result["games"] == 10
        assert result["metadata"] == 6

    def test_dict_result_aggregation(self, mixin):
        from src.sync.sync_games import GameDetailSyncScope

        mixin._sync_game_detail_chunk = MagicMock(return_value={"nested": {"a": 1, "b": 2}})
        scope = GameDetailSyncScope(
            scoped_game_ids=["g1"],
            filters=[],
            target_game_ids=None,
            year=None,
            days=None,
            unsynced_only=False,
            batch_size=1000,
        )
        result = mixin._aggregate_game_detail_chunks(scope)
        assert result["nested"]["a"] == 1
        assert result["nested"]["b"] == 2


class TestSyncGameDetailChunk:
    def test_child_filters_empty_returns_early(self, mixin):
        from src.sync.sync_games import GameDetailSyncScope

        scope = GameDetailSyncScope(
            scoped_game_ids=["g1"],
            filters=[],
            target_game_ids=None,
            year=None,
            days=None,
            unsynced_only=False,
            batch_size=1000,
        )
        eligibility = GameSyncEligibility(
            parent_game_ids=["g1"],
            detail_game_ids=["g1"],
            relay_game_ids=[],
        )
        with patch("src.sync.sync_games.build_game_sync_eligibility", return_value=eligibility):
            mixin._game_detail_child_filters = MagicMock(return_value=[])
            result = mixin._sync_game_detail_chunk(["g1"], scope)
        assert "metadata" not in result

    def test_skip_year_purge_replaces_children(self, mixin):
        from src.sync.sync_games import GameDetailSyncScope

        scope = GameDetailSyncScope(
            scoped_game_ids=["g1"],
            filters=[],
            target_game_ids=None,
            year=2025,
            days=None,
            unsynced_only=False,
            batch_size=1000,
        )
        eligibility = GameSyncEligibility(
            parent_game_ids=["g1"],
            detail_game_ids=["g1"],
            relay_game_ids=["g1"],
        )
        mixin._replace_target_child_rows_for_games = MagicMock()
        with patch("src.sync.sync_games.build_game_sync_eligibility", return_value=eligibility):
            mixin._sync_game_detail_chunk(["g1"], scope, skip_year_purge=True)
        assert mixin._replace_target_child_rows_for_games.call_count == 2


class TestSyncSpecificGame:
    def test_detail_game_ids_replaces_children(self, mixin):
        eligibility = GameSyncEligibility(
            parent_game_ids=["g1"],
            detail_game_ids=["g1"],
            relay_game_ids=[],
        )
        mixin._replace_target_child_rows_for_games = MagicMock()
        mixin._chunked = MagicMock(return_value=[])
        with patch("src.sync.sync_games.build_game_sync_eligibility", return_value=eligibility):
            mixin.sync_specific_game("g1")
        assert mixin._replace_target_child_rows_for_games.call_count == 1

    def test_relay_game_ids_replaces_children(self, mixin):
        eligibility = GameSyncEligibility(
            parent_game_ids=["g1"],
            detail_game_ids=[],
            relay_game_ids=["g1"],
        )
        mixin._replace_target_child_rows_for_games = MagicMock()
        mixin._chunked = MagicMock(return_value=[])
        with patch("src.sync.sync_games.build_game_sync_eligibility", return_value=eligibility):
            mixin.sync_specific_game("g1")
        assert mixin._replace_target_child_rows_for_games.call_count == 1

    def test_validation_metrics_table_missing(self, mixin):
        eligibility = GameSyncEligibility(
            parent_game_ids=["g1"],
            detail_game_ids=["g1"],
            relay_game_ids=[],
        )
        mixin._target_table_exists = MagicMock(return_value=False)
        mixin._chunked = MagicMock(return_value=[])
        with patch("src.sync.sync_games.build_game_sync_eligibility", return_value=eligibility):
            result = mixin.sync_specific_game("g1")
        assert result["validation_metrics"] == 0


class TestSyncPregameGame:
    def test_valid_game_id(self, mixin):
        mixin._chunked = MagicMock(return_value=[])
        result = mixin.sync_pregame_game("g1")
        assert "game" in result
        assert "metadata" in result
        assert "lineups" in result
        assert "summary" in result

    def test_deletes_existing_lineups(self, mixin):
        mixin._chunked = MagicMock(return_value=[])
        mixin.sync_pregame_game("g1")
        mixin.target_session.query.assert_any_call(GameLineup)


class TestSyncReviewSummariesForGames:
    def test_empty_game_ids_returns_zero(self, mixin):
        result = mixin.sync_review_summaries_for_games([])
        assert result == {"summary": 0, "games": 0}

    def test_deduplicates_and_syncs(self, mixin):
        mixin._sync_game_summary_rows = MagicMock(return_value=5)
        mixin._chunked = MagicMock(return_value=[["g1", "g2"]])
        result = mixin.sync_review_summaries_for_games(["g1", "g1", "g2"])
        assert result["summary"] == 5
        assert result["games"] == 2


class TestSyncGameSummaryRows:
    def test_no_rows_returns_zero(self, mixin):
        mock_query = MagicMock()
        mock_query.filter.return_value.filter.return_value.all.return_value = []
        mock_query.filter.return_value.all.return_value = []
        mixin.sqlite_session.query.return_value = mock_query
        result = mixin._sync_game_summary_rows()
        assert result == 0


class TestSyncGamePlayByPlay:
    def test_no_game_ids_returns_zero(self, mixin):
        mock_query = MagicMock()
        mock_query.all.return_value = []
        mixin.sqlite_session.query.return_value.distinct.return_value = mock_query
        result = mixin._sync_game_play_by_play()
        assert result == 0

    def test_with_game_ids_syncs(self, mixin):
        mock_query = MagicMock()
        mock_query.all.return_value = [("g1",), ("g2",)]
        mixin.sqlite_session.query.return_value.distinct.return_value = mock_query
        mixin.sync_simple_table = MagicMock(return_value=10)
        mixin._chunked = MagicMock(return_value=[["g1", "g2"]])
        result = mixin._sync_game_play_by_play()
        assert result == 10
