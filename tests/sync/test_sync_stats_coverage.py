from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError


class TestStatsSyncMixinCoverage:
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

    def test_add_existing_player_basic_filter_with_missing_count(self, mixin, caplog):
        class FakeModel:
            __tablename__ = "player_season_batting"
            player_id = MagicMock()

        filter_mock = MagicMock()
        filter_mock.count.return_value = 3
        query_mock = MagicMock()
        query_mock.filter.return_value = filter_mock
        mixin.sqlite_session.query.side_effect = [MagicMock(), query_mock]
        with caplog.at_level("WARNING"):
            result = mixin._add_existing_player_basic_filter(FakeModel(), [])
        assert len(result) == 1
        assert "Skipping 3" in caplog.text

    def test_verify_pitcher_sync_below_expected(self, mixin):
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (5,)
        mixin.target_session.execute.return_value = mock_result
        mixin.verify_pitcher_sync(10)

    def test_verify_pitcher_sync_sql_error(self, mixin):
        mixin.target_session.execute.side_effect = SQLAlchemyError("db error")
        mixin.verify_pitcher_sync(10)

    def test_verify_batting_sync_below_expected(self, mixin):
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (5,)
        mixin.target_session.execute.return_value = mock_result
        mixin.verify_batting_sync(10)

    def test_verify_batting_sync_sql_error(self, mixin):
        mixin.target_session.execute.side_effect = SQLAlchemyError("db error")
        mixin.verify_batting_sync(10)

    def test_show_oci_data_sample_empty(self, mixin):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mixin.target_session.execute.return_value = mock_result
        mixin.show_oci_data_sample()

    def test_show_oci_data_sample_sql_error(self, mixin):
        mixin.target_session.execute.side_effect = SQLAlchemyError("db error")
        mixin.show_oci_data_sample()

    def test_get_table_signature_row_none(self, mixin):
        mock_model = MagicMock()
        mock_model.__tablename__ = "test_table"
        mixin.sqlite_session.execute.return_value.fetchone.return_value = None
        mixin.target_session.execute.return_value.fetchone.return_value = None

        sig = mixin._get_table_signature(mock_model, 2025)
        assert sig["match"] is False

    def test_get_table_signature_sql_error_first(self, mixin):
        mock_model = MagicMock()
        mock_model.__tablename__ = "test_table"
        mixin.sqlite_session.execute.side_effect = SQLAlchemyError("local error")
        mixin.target_session.execute.return_value.fetchone.return_value = (5, "2025-01-01 12:00:00")

        sig = mixin._get_table_signature(mock_model, 2025)
        assert sig["match"] is False
        assert sig["local"]["count"] is None

    def test_get_table_signature_sql_error_second(self, mixin):
        mock_model = MagicMock()
        mock_model.__tablename__ = "test_table"
        mixin.sqlite_session.execute.return_value.fetchone.return_value = (5, "2025-01-01 12:00:00")
        mixin.target_session.execute.side_effect = SQLAlchemyError("remote error")

        sig = mixin._get_table_signature(mock_model, 2025)
        assert sig["match"] is False
        assert sig["remote"]["count"] is None

    def test_sync_player_season_batting_no_year(self, mixin):
        mixin.sync_simple_table.return_value = 20
        result = mixin.sync_player_season_batting(year=None)
        assert result == 20

    def test_sync_player_season_batting_with_year(self, mixin):
        mixin.sync_simple_table.return_value = 20
        result = mixin.sync_player_season_batting(year=2025)
        assert result == 20

    def test_sync_player_season_pitching_no_year(self, mixin):
        mixin.sync_simple_table.return_value = 12
        result = mixin.sync_player_season_pitching(year=None)
        assert result == 12

    def test_sync_player_season_pitching_with_year(self, mixin):
        mixin.sync_simple_table.return_value = 12
        result = mixin.sync_player_season_pitching(year=2025)
        assert result == 12

    def test_sync_team_season_batting_skip_unchanged(self, mixin):
        mixin._get_table_signature = MagicMock(
            return_value={
                "local": {"count": 5, "max_updated_at": "2025-01-01"},
                "remote": {"count": 5, "max_updated_at": "2025-01-01"},
                "match": True,
            },
        )
        result = mixin.sync_team_season_batting(year=2025)
        assert result == 0

    def test_sync_team_season_batting_with_year(self, mixin):
        mixin.sync_simple_table.return_value = 8
        result = mixin.sync_team_season_batting(year=2025)
        assert result == 8

    def test_sync_team_season_pitching_skip_unchanged(self, mixin):
        mixin._get_table_signature = MagicMock(
            return_value={
                "local": {"count": 5, "max_updated_at": "2025-01-01"},
                "remote": {"count": 5, "max_updated_at": "2025-01-01"},
                "match": True,
            },
        )
        result = mixin.sync_team_season_pitching(year=2025)
        assert result == 0

    def test_sync_team_season_pitching_with_year(self, mixin):
        mixin.sync_simple_table.return_value = 7
        result = mixin.sync_team_season_pitching(year=2025)
        assert result == 7

    def test_sync_standings_with_year(self, mixin):
        mixin.sync_simple_table.return_value = 30
        assert mixin.sync_standings(year=2025) == 30

    def test_sync_standings_with_days(self, mixin):
        mixin.sync_simple_table.return_value = 30
        assert mixin.sync_standings(days=7) == 30

    def test_sync_stat_rankings_with_filters(self, mixin):
        mixin.sync_simple_table.return_value = 25
        result = mixin.sync_stat_rankings(year=2025, filters=[MagicMock()])
        assert result == 25

    def test_sync_fielding_stats_skip_unchanged(self, mixin):
        mixin._get_table_signature = MagicMock(
            return_value={
                "local": {"count": 5, "max_updated_at": "2025-01-01"},
                "remote": {"count": 5, "max_updated_at": "2025-01-01"},
                "match": True,
            },
        )
        result = mixin.sync_fielding_stats(year=2025)
        assert result == 0

    def test_sync_fielding_stats_with_year(self, mixin):
        mixin.sync_simple_table.return_value = 14
        result = mixin.sync_fielding_stats(year=2025)
        assert result == 14

    def test_sync_baserunning_stats_skip_unchanged(self, mixin):
        mixin._get_table_signature = MagicMock(
            return_value={
                "local": {"count": 5, "max_updated_at": "2025-01-01"},
                "remote": {"count": 5, "max_updated_at": "2025-01-01"},
                "match": True,
            },
        )
        result = mixin.sync_baserunning_stats(year=2025)
        assert result == 0

    def test_sync_baserunning_stats_with_year(self, mixin):
        mixin.sync_simple_table.return_value = 9
        result = mixin.sync_baserunning_stats(year=2025)
        assert result == 9

    def test_sync_team_season_fielding_skip_unchanged(self, mixin):
        mixin._get_table_signature = MagicMock(
            return_value={
                "local": {"count": 5, "max_updated_at": "2025-01-01"},
                "remote": {"count": 5, "max_updated_at": "2025-01-01"},
                "match": True,
            },
        )
        result = mixin.sync_team_season_fielding(year=2025)
        assert result == 0

    def test_sync_team_season_fielding_with_year(self, mixin):
        mixin.sync_simple_table.return_value = 5
        result = mixin.sync_team_season_fielding(year=2025)
        assert result == 5

    def test_sync_team_season_baserunning_skip_unchanged(self, mixin):
        mixin._get_table_signature = MagicMock(
            return_value={
                "local": {"count": 5, "max_updated_at": "2025-01-01"},
                "remote": {"count": 5, "max_updated_at": "2025-01-01"},
                "match": True,
            },
        )
        result = mixin.sync_team_season_baserunning(year=2025)
        assert result == 0

    def test_sync_team_season_baserunning_with_year(self, mixin):
        mixin.sync_simple_table.return_value = 4
        result = mixin.sync_team_season_baserunning(year=2025)
        assert result == 4

    def test_purge_season_stats_pitching(self, mixin):
        mixin.purge_season_stats(2025, type="pitching")
        assert mixin.target_session.execute.call_count == 2
        mixin.target_session.commit.assert_called_once()

    def test_purge_season_stats_fielding(self, mixin):
        mixin.purge_season_stats(2025, type="fielding")
        assert mixin.target_session.execute.call_count == 2
        mixin.target_session.commit.assert_called_once()

    def test_purge_season_stats_baserunning(self, mixin):
        mixin.purge_season_stats(2025, type="baserunning")
        assert mixin.target_session.execute.call_count == 2
        mixin.target_session.commit.assert_called_once()

    def test_get_table_signature_no_year(self, mixin):
        mock_model = MagicMock()
        mock_model.__tablename__ = "test_table"
        mixin.sqlite_session.execute.return_value.fetchone.return_value = (10, "2025-06-01 00:00:00")
        mixin.target_session.execute.return_value.fetchone.return_value = (10, "2025-06-01 00:00:00")

        sig = mixin._get_table_signature(mock_model, None)
        assert sig["match"] is True

    def test_show_oci_data_sample_multiple_rows(self, mixin):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (1, 2025, 10, 5, 3, 3.50, 80.0),
            (2, 2025, 12, 6, 2, 2.80, 95.0),
            (3, 2025, 8, 3, 1, 4.20, 60.0),
        ]
        mixin.target_session.execute.return_value = mock_result
        mixin.show_oci_data_sample()
