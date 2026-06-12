from __future__ import annotations

from unittest.mock import MagicMock

import pytest


class TestStatsSyncMixin:
    @pytest.fixture
    def mixin(self):
        from src.sync.sync_stats import StatsSyncMixin

        instance = StatsSyncMixin()
        instance.sqlite_session = MagicMock()
        instance.target_session = MagicMock()
        instance.oci_engine = MagicMock()
        instance._season_map_cache = {}
        instance._franchise_id_mapping_cache = {}
        instance._temp_table_counter = __import__("itertools").count(1)
        instance.sync_simple_table = MagicMock()
        return instance

    def test_sync_pitcher_data(self, mixin):
        mixin.sync_simple_table.return_value = 10
        assert mixin.sync_pitcher_data() == 10

    def test_sync_batting_data(self, mixin):
        mixin.sync_simple_table.return_value = 15
        assert mixin.sync_batting_data() == 15

    def test_verify_pitcher_sync(self, mixin):
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (10,)
        mixin.target_session.execute.return_value = mock_result
        mixin.verify_pitcher_sync(10)

    def test_verify_batting_sync(self, mixin):
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (15,)
        mixin.target_session.execute.return_value = mock_result
        mixin.verify_batting_sync(15)

    def test_sync_player_season_batting(self, mixin):
        mixin.sync_simple_table.return_value = 20
        result = mixin.sync_player_season_batting(year=2025)
        assert result == 20

    def test_sync_player_season_batting_skip_unchanged(self, mixin):
        mixin._get_table_signature = MagicMock(
            return_value={
                "local": {"count": 5, "max_updated_at": "2025-01-01"},
                "remote": {"count": 5, "max_updated_at": "2025-01-01"},
                "match": True,
            }
        )
        result = mixin.sync_player_season_batting(year=2025)
        assert result == 0

    def test_sync_player_season_pitching(self, mixin):
        mixin.sync_simple_table.return_value = 12
        result = mixin.sync_player_season_pitching(year=2025)
        assert result == 12

    def test_sync_player_season_pitching_force(self, mixin):
        mixin.sync_simple_table.return_value = 12
        result = mixin.sync_player_season_pitching(year=2025, force=True)
        assert result == 12

    def test_sync_all_player_data(self, mixin):
        mixin.sync_players = MagicMock(return_value=5)
        mixin.sync_player_identities = MagicMock(return_value=6)
        mixin.sync_player_season_batting = MagicMock(return_value=20)
        mixin.sync_player_season_pitching = MagicMock(return_value=15)
        mixin.sync_team_season_batting = MagicMock(return_value=8)
        mixin.sync_team_season_pitching = MagicMock(return_value=7)
        result = mixin.sync_all_player_data()
        assert result == {
            "players": 5,
            "player_identities": 6,
            "player_season_batting": 20,
            "player_season_pitching": 15,
            "team_season_batting": 8,
            "team_season_pitching": 7,
        }

    def test_sync_team_season_batting(self, mixin):
        mixin.sync_simple_table.return_value = 8
        assert mixin.sync_team_season_batting(year=2025) == 8

    def test_sync_team_season_pitching(self, mixin):
        mixin.sync_simple_table.return_value = 7
        assert mixin.sync_team_season_pitching(year=2025) == 7

    def test_sync_standings(self, mixin):
        mixin.sync_simple_table.return_value = 30
        assert mixin.sync_standings(year=2025) == 30

    def test_sync_stat_rankings(self, mixin):
        mixin.sync_simple_table.return_value = 25
        assert mixin.sync_stat_rankings(year=2025) == 25

    def test_sync_fielding_stats(self, mixin):
        mixin.sync_simple_table.return_value = 14
        assert mixin.sync_fielding_stats(year=2025) == 14

    def test_sync_baserunning_stats(self, mixin):
        mixin.sync_simple_table.return_value = 9
        assert mixin.sync_baserunning_stats(year=2025) == 9

    def test_get_table_signature(self, mixin):
        mock_model = MagicMock()
        mock_model.__tablename__ = "test_table"
        mixin.sqlite_session.execute.return_value.fetchone.return_value = (5, "2025-01-01 12:00:00")
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (5, "2025-01-01 12:00:00")
        mixin.target_session.execute.return_value = mock_result

        sig = mixin._get_table_signature(mock_model, 2025)
        assert "local" in sig
        assert "remote" in sig

    def test_purge_season_stats_all(self, mixin):
        mixin.purge_season_stats(2025, type="all")
        assert mixin.target_session.execute.call_count >= 1
        mixin.target_session.commit.assert_called_once()

    def test_purge_season_stats_batting(self, mixin):
        mixin.purge_season_stats(2025, type="batting")
        mixin.target_session.execute.assert_called()
        mixin.target_session.commit.assert_called_once()

    def test_show_oci_data_sample(self, mixin):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(1, 2025, 10, 5, 3, 3.50, 80.0)]
        mixin.target_session.execute.return_value = mock_result
        mixin.show_oci_data_sample()

    def test_add_existing_player_basic_filter(self, mixin):
        class FakeModel:
            __tablename__ = "player_season_batting"
            player_id = MagicMock()

        mixin.sqlite_session.query.return_value.count.return_value = 0
        result = mixin._add_existing_player_basic_filter(FakeModel(), [])
        assert len(result) == 1
