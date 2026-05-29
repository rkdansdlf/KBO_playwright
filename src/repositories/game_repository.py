"""
Repository for saving game details, box scores, and normalized relay data.
Thin facade re-exporting from domain-split modules.
"""

from src.repositories.game_helpers import (  # noqa: F401
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DRAW,
    GAME_STATUS_LIVE,
    GAME_STATUS_POSTPONED,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_UNRESOLVED,
    LIVE_GAME_STATUSES,
)
from src.repositories.game_relay import (
    backfill_game_play_by_play_from_existing_events,
    backfill_missing_game_stubs_for_relays,
    derive_play_by_play_rows_from_events,
    repair_game_parent_from_existing_children,
    save_relay_data,
)
from src.repositories.game_save import (
    get_games_by_date,
    resolve_canonical_game_id,
    save_game_detail,
    save_game_snapshot,
    save_pregame_lineups,
    save_schedule_game,
)
from src.repositories.game_status import (
    refresh_game_status_for_date,
    update_game_status,
)

__all__ = [
    "backfill_game_play_by_play_from_existing_events",
    "backfill_missing_game_stubs_for_relays",
    "derive_play_by_play_rows_from_events",
    "get_games_by_date",
    "refresh_game_status_for_date",
    "repair_game_parent_from_existing_children",
    "resolve_canonical_game_id",
    "save_game_detail",
    "save_game_snapshot",
    "save_pregame_lineups",
    "save_relay_data",
    "save_schedule_game",
    "update_game_status",
]
