from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock

import pytest

from src.sync.sync_misc import _format_daily_roster_scope, _normalize_daily_roster_date


class TestNormalizeDailyRosterDate:
    def test_none(self):
        assert _normalize_daily_roster_date(None) is None

    def test_date(self):
        d = date(2025, 6, 1)
        assert _normalize_daily_roster_date(d) == d

    def test_datetime(self):
        dt = datetime(2025, 6, 1, 12, 0)
        assert _normalize_daily_roster_date(dt) == date(2025, 6, 1)

    def test_iso_string(self):
        assert _normalize_daily_roster_date("2025-06-01") == date(2025, 6, 1)

    def test_compact_string(self):
        assert _normalize_daily_roster_date("20250601") == date(2025, 6, 1)

    def test_empty_string(self):
        assert _normalize_daily_roster_date("") is None

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            _normalize_daily_roster_date("not-a-date")


class TestFormatDailyRosterScope:
    def test_both_dates(self):
        result = _format_daily_roster_scope(date(2025, 1, 1), date(2025, 12, 31))
        assert "2025-01-01" in result
        assert "2025-12-31" in result

    def test_start_only(self):
        result = _format_daily_roster_scope(date(2025, 1, 1), None)
        assert "2025-01-01" in result

    def test_end_only(self):
        result = _format_daily_roster_scope(None, date(2025, 12, 31))
        assert "2025-12-31" in result

    def test_neither(self):
        assert _format_daily_roster_scope(None, None) == ""


class TestMiscSyncMixin:
    @pytest.fixture
    def mixin(self):
        from src.sync.sync_misc import MiscSyncMixin

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

    def test_sync_franchises(self, mixin):
        mixin.sync_simple_table.return_value = 10
        assert mixin.sync_franchises() == 10

    def test_sync_kbo_seasons(self, mixin):
        mixin.sync_simple_table.return_value = 5
        assert mixin.sync_kbo_seasons() == 5

    def test_sync_stadium_info(self, mixin):
        mixin.sync_simple_table.return_value = 3
        assert mixin.sync_stadium_info() == 3

    def test_sync_awards(self, mixin):
        mixin.sync_simple_table.return_value = 7
        result = mixin.sync_awards()
        assert result == 7

    def test_sync_teams_no_data(self, mixin):
        mixin.sqlite_session.query.return_value.all.return_value = []
        assert mixin.sync_teams() == 0

    def test_sync_daily_rosters_start_after_end_raises(self, mixin):
        with pytest.raises(ValueError, match="start_date must be earlier"):
            mixin.sync_daily_rosters(start_date="2025-12-31", end_date="2025-01-01")

    def test_sync_daily_rosters_with_dates(self, mixin):
        mixin.sync_simple_table.return_value = 0
        result = mixin.sync_daily_rosters(start_date="2025-06-01", end_date="2025-06-30")
        assert result == 0

    def test_sync_team_history_no_target_table(self, mixin):
        mixin._target_table_exists.return_value = False
        assert mixin.sync_team_history() == 0

    def test_sync_phase1_all(self, mixin):
        mixin.sync_game_broadcasts = MagicMock(return_value=1)
        mixin.sync_stadium_info = MagicMock(return_value=2)
        mixin.sync_stadium_regulations = MagicMock(return_value=3)
        mixin.sync_game_mvps = MagicMock(return_value=4)
        mixin.sync_injury_entries = MagicMock(return_value=5)
        mixin.sync_foreign_player_changes = MagicMock(return_value=6)
        mixin.sync_manager_changes = MagicMock(return_value=7)
        mixin.sync_team_rivalries = MagicMock(return_value=8)
        mixin.sync_cheer_songs = MagicMock(return_value=9)
        mixin.sync_cheer_chants = MagicMock(return_value=10)
        mixin.sync_team_events = MagicMock(return_value=11)
        result = mixin.sync_phase1_all()
        assert result["game_broadcasts"] == 1
        assert result["team_events"] == 11
        assert len(result) == 11

    def test_sync_stadium_realtime_all(self, mixin):
        mixin.sync_transit_times = MagicMock(return_value=5)
        mixin.sync_congestion = MagicMock(return_value=3)
        mixin.sync_operation_notices = MagicMock(return_value=2)
        result = mixin.sync_stadium_realtime_all(game_date="20250601")
        assert result == {"transit_times": 5, "congestion": 3, "operation_notices": 2}

    def test_sync_team_code_map(self, mixin):
        mixin.sync_simple_table.return_value = 4
        assert mixin.sync_team_code_map() == 4

    def test_sync_ticket_schedules(self, mixin):
        mixin.sync_simple_table.return_value = 12
        assert mixin.sync_ticket_schedules() == 12

    def test_sync_stadium_foods(self, mixin):
        mixin.sync_simple_table.return_value = 8
        assert mixin.sync_stadium_foods() == 8

    def test_sync_matchups(self, mixin):
        mixin.sync_simple_table.return_value = 3
        mixin.sync_matchups(year=2025)
        assert mixin.sync_simple_table.call_count >= 1
