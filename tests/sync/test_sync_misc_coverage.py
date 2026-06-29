from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from src.sync.sync_misc import (
    MiscSyncMixin,
    _format_daily_roster_scope,
    _normalize_daily_roster_date,
)


def _resolve_daily_roster_team_code(data):
    return MiscSyncMixin._resolve_daily_roster_team_code(data)


class TestNormalizeDailyRosterDateEdgeCases:
    def test_whitespace_string(self):
        assert _normalize_daily_roster_date("   ") is None

    def test_invalid_string_raises(self):
        with pytest.raises(ValueError):
            _normalize_daily_roster_date("not a date at all")

    def test_none_returns_none(self):
        assert _normalize_daily_roster_date(None) is None


class TestResolveDailyRosterTeamCode:
    def test_same_start_end(self):
        result = _format_daily_roster_scope(date(2025, 6, 1), date(2025, 6, 1))
        assert "2025-06-01" in result


class TestResolveDailyRosterTeamCode2:
    def test_empty_team_code(self):
        data = {"team_code": "", "roster_date": date(2025, 6, 1)}
        _resolve_daily_roster_team_code(data)
        assert data["team_code"] == ""

    def test_empty_roster_date(self):
        data = {"team_code": "LG", "roster_date": None}
        _resolve_daily_roster_team_code(data)
        assert data["team_code"] == "LG"

    def test_lot_alias(self):
        data = {"team_code": "LOT", "roster_date": date(2025, 6, 1)}
        with patch("src.utils.team_history.resolve_team_code_for_season", return_value="LT"):
            _resolve_daily_roster_team_code(data)
            assert data["team_code"] == "LT"

    def test_kw_alias(self):
        data = {"team_code": "KW", "roster_date": date(2025, 6, 1)}
        with patch("src.utils.team_history.resolve_team_code_for_season", return_value="KH"):
            _resolve_daily_roster_team_code(data)
            assert data["team_code"] == "KH"

    def test_roster_date_without_year(self):
        data = {"team_code": "LG", "roster_date": "2025-06-01"}
        _resolve_daily_roster_team_code(data)
        assert data["team_code"] == "LG"


class TestMiscSyncMixinExtended:
    @pytest.fixture
    def mixin(self):
        instance = MiscSyncMixin()
        instance.sqlite_session = MagicMock()
        instance.target_session = MagicMock()
        instance.oci_engine = MagicMock()
        instance._season_map_cache = {}
        instance._franchise_id_mapping_cache = {}
        instance._temp_table_counter = __import__("itertools").count(1)
        instance.sync_simple_table = MagicMock()
        instance._bulk_copy_upsert = MagicMock()
        instance._target_table_exists = MagicMock(return_value=True)
        instance._get_franchise_id_mapping = MagicMock(return_value={})
        instance._ensure_table = MagicMock()
        return instance

    def test_sync_teams_with_franchise_mapping(self, mixin):
        mixin._get_franchise_id_mapping.return_value = {1: 100}

        class FakeTeam:
            team_id = 1
            team_name = "Test Team"
            team_short_name = "TST"
            city = "Seoul"
            founded_year = 1982
            stadium_name = "Test Stadium"
            franchise_id = 1
            aliases = None
            is_active = True
            created_at = None
            updated_at = None

        mixin.sqlite_session.query.return_value.all.return_value = [FakeTeam()]
        result = mixin.sync_teams()
        assert result == 1
        mixin._bulk_copy_upsert.assert_called_once()

    def test_sync_teams_with_string_aliases(self, mixin):
        mixin._get_franchise_id_mapping.return_value = {}

        class FakeTeam:
            team_id = 1
            team_name = "Test Team"
            team_short_name = "TST"
            city = "Seoul"
            founded_year = 1982
            stadium_name = "Test Stadium"
            franchise_id = None
            aliases = '["alias1", "alias2"]'
            is_active = None
            created_at = None
            updated_at = None

        mixin.sqlite_session.query.return_value.all.return_value = [FakeTeam()]
        result = mixin.sync_teams()
        assert result == 1

    def test_sync_teams_with_list_aliases(self, mixin):
        mixin._get_franchise_id_mapping.return_value = {}

        class FakeTeam:
            team_id = 1
            team_name = "Test Team"
            team_short_name = "TST"
            city = "Seoul"
            founded_year = 1982
            stadium_name = "Test Stadium"
            franchise_id = None
            aliases = ["alias1", "alias2"]
            is_active = True
            created_at = None
            updated_at = None

        mixin.sqlite_session.query.return_value.all.return_value = [FakeTeam()]
        result = mixin.sync_teams()
        assert result == 1

    def test_sync_teams_with_invalid_json_aliases(self, mixin):
        mixin._get_franchise_id_mapping.return_value = {}

        class FakeTeam:
            team_id = 1
            team_name = "Test Team"
            team_short_name = "TST"
            city = "Seoul"
            founded_year = 1982
            stadium_name = "Test Stadium"
            franchise_id = None
            aliases = "not-json"
            is_active = True
            created_at = None
            updated_at = None

        mixin.sqlite_session.query.return_value.all.return_value = [FakeTeam()]
        result = mixin.sync_teams()
        assert result == 1

    def test_sync_teams_without_franchise_id(self, mixin):
        mixin._get_franchise_id_mapping.return_value = {}

        class FakeTeam:
            team_id = 1
            team_name = "Test Team"
            team_short_name = "TST"
            city = "Seoul"
            founded_year = 1982
            stadium_name = "Test Stadium"
            franchise_id = None
            aliases = None
            is_active = True
            created_at = None
            updated_at = None

        mixin.sqlite_session.query.return_value.all.return_value = [FakeTeam()]
        result = mixin.sync_teams()
        assert result == 1

    def test_sync_awards_with_migration(self, mixin, tmp_path):
        migration_file = tmp_path / "019_create_awards.sql"
        migration_file.write_text("CREATE TABLE IF NOT EXISTS awards (id SERIAL PRIMARY KEY);")

        with patch("src.sync.sync_misc.Path") as mock_path:
            mock_path.return_value.exists.return_value = True
            mock_path.return_value.read_text.return_value = "CREATE TABLE IF NOT EXISTS awards (id SERIAL PRIMARY KEY);"
            result = mixin.sync_awards()
            assert result is not None

    def test_sync_awards_migration_error(self, mixin):
        mixin.target_session.execute.side_effect = [
            __import__("sqlalchemy.exc", fromlist=["SQLAlchemyError"]).SQLAlchemyError("fail"),
            None,
        ]
        mixin.sync_simple_table.return_value = 0
        result = mixin.sync_awards()
        assert result == 0

    def test_sync_rag_chunks(self, mixin):
        mixin.sync_simple_table.return_value = 5
        result = mixin.sync_rag_chunks()
        assert result == 5

    def test_sync_rag_chunks_metadata_error(self, mixin):
        mixin.oci_engine = MagicMock()
        mixin.oci_engine.connect.return_value.__enter__ = MagicMock(
            side_effect=__import__("sqlalchemy.exc", fromlist=["SQLAlchemyError"]).SQLAlchemyError("fail")
        )
        mixin.oci_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mixin.sync_simple_table.return_value = 0
        result = mixin.sync_rag_chunks()
        assert result == 0

    def test_sync_transit_times_with_date(self, mixin):
        mixin.sync_simple_table.return_value = 3
        result = mixin.sync_transit_times(game_date="20250601")
        assert result == 3

    def test_sync_transit_times_no_date(self, mixin):
        mixin.sync_simple_table.return_value = 10
        result = mixin.sync_transit_times()
        assert result == 10

    def test_sync_congestion_with_date(self, mixin):
        mixin.sync_simple_table.return_value = 2
        result = mixin.sync_congestion(game_date="20250601")
        assert result == 2

    def test_sync_operation_notices_with_date(self, mixin):
        mixin.sync_simple_table.return_value = 1
        result = mixin.sync_operation_notices(game_date="20250601")
        assert result == 1

    def test_sync_daily_rosters_no_dates(self, mixin):
        mixin.sync_simple_table.return_value = 0
        result = mixin.sync_daily_rosters()
        assert result == 0

    def test_sync_daily_rosters_with_start_only(self, mixin):
        mixin.sync_simple_table.return_value = 0
        result = mixin.sync_daily_rosters(start_date="2025-06-01")
        assert result == 0

    def test_sync_daily_rosters_with_end_only(self, mixin):
        mixin.sync_simple_table.return_value = 0
        result = mixin.sync_daily_rosters(end_date="2025-06-30")
        assert result == 0

    def test_sync_daily_rosters_with_datetime(self, mixin):
        mixin.sync_simple_table.return_value = 0
        result = mixin.sync_daily_rosters(
            start_date=datetime(2025, 6, 1),
            end_date=datetime(2025, 6, 30),
        )
        assert result == 0

    def test_sync_team_history_with_data(self, mixin):
        class FakeHistory:
            id = 1
            franchise_id = 1
            season = 2025
            team_name = "Test"
            team_code = "TST"
            logo_url = None
            ranking = 1
            stadium = "Test Stadium"
            city = "Seoul"
            color = "#000000"
            created_at = None
            updated_at = None

        mixin.sqlite_session.query.return_value.all.return_value = [FakeHistory()]
        mixin._get_franchise_id_mapping.return_value = {1: 100}
        result = mixin.sync_team_history()
        assert result == 1

    def test_sync_team_history_no_records(self, mixin):
        mixin.sqlite_session.query.return_value.all.return_value = []
        result = mixin.sync_team_history()
        assert result == 0

    def test_sync_team_history_missing_franchise_mapping(self, mixin):
        class FakeHistory:
            id = 1
            franchise_id = 1
            season = 2025
            team_name = "Test"
            team_code = "TST"
            logo_url = None
            ranking = 1
            stadium = "Test Stadium"
            city = "Seoul"
            color = "#000000"
            created_at = None
            updated_at = None

        mixin.sqlite_session.query.return_value.all.return_value = [FakeHistory()]
        mixin._get_franchise_id_mapping.return_value = {}
        result = mixin.sync_team_history()
        assert result == 0

    def test_sync_team_code_map_with_franchise_mapping(self, mixin):
        mixin._get_franchise_id_mapping.return_value = {1: 100}
        mixin.sync_simple_table.return_value = 4
        result = mixin.sync_team_code_map()
        assert result == 4

    def test_sync_matchups_no_year(self, mixin):
        mixin.sync_simple_table.return_value = 0
        result = mixin.sync_matchups()
        assert isinstance(result, dict)
        assert len(result) == 7

    def test_sync_matchups_with_year(self, mixin):
        mixin.sync_simple_table.return_value = 0
        result = mixin.sync_matchups(year=2025)
        assert isinstance(result, dict)
        assert len(result) == 7

    def test_transform_daily_roster_row_success(self, mixin):
        data = {"team_code": "LG", "roster_date": date(2025, 6, 1)}
        with patch("src.utils.team_history.resolve_team_code_for_season", return_value="LG"):
            result = mixin._transform_daily_roster_row(data)
            assert result["team_code"] == "LG"

    def test_transform_daily_roster_row_error(self, mixin):
        data = {"team_code": "LG", "roster_date": date(2025, 6, 1)}
        with patch("src.utils.team_history.resolve_team_code_for_season", side_effect=RuntimeError("fail")):
            result = mixin._transform_daily_roster_row(data)
            assert result["team_code"] == "LG"
