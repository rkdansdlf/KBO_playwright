"""Test that game_repository re-exports all expected symbols."""

from src.repositories.game_repository import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DRAW,
    GAME_STATUS_LIVE,
    GAME_STATUS_POSTPONED,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_UNRESOLVED,
    LIVE_GAME_STATUSES,
    backfill_game_play_by_play_from_existing_events,
    backfill_missing_game_stubs_for_relays,
    derive_play_by_play_rows_from_events,
    get_games_by_date,
    mark_relay_source_unavailable,
    refresh_game_status_for_date,
    repair_game_parent_from_existing_children,
    resolve_canonical_game_id,
    save_game_detail,
    save_game_snapshot,
    save_pregame_lineups,
    save_relay_data,
    save_schedule_game,
    update_game_status,
)


class TestGameRepositoryExports:
    def test_constants(self):
        assert GAME_STATUS_SCHEDULED == "SCHEDULED"
        assert GAME_STATUS_COMPLETED == "COMPLETED"
        assert GAME_STATUS_CANCELLED == "CANCELLED"
        assert GAME_STATUS_POSTPONED == "POSTPONED"
        assert GAME_STATUS_LIVE == "LIVE"
        assert GAME_STATUS_DRAW == "DRAW"
        assert GAME_STATUS_UNRESOLVED == "UNRESOLVED_MISSING"

    def test_live_game_statuses(self):
        assert isinstance(LIVE_GAME_STATUSES, set)
        assert "LIVE" in LIVE_GAME_STATUSES

    def test_functions_importable(self):
        assert callable(backfill_game_play_by_play_from_existing_events)
        assert callable(backfill_missing_game_stubs_for_relays)
        assert callable(derive_play_by_play_rows_from_events)
        assert callable(get_games_by_date)
        assert callable(mark_relay_source_unavailable)
        assert callable(refresh_game_status_for_date)
        assert callable(repair_game_parent_from_existing_children)
        assert callable(resolve_canonical_game_id)
        assert callable(save_game_detail)
        assert callable(save_game_snapshot)
        assert callable(save_pregame_lineups)
        assert callable(save_relay_data)
        assert callable(save_schedule_game)
        assert callable(update_game_status)
