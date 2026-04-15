"""Shared KBO game status constants and helpers."""
from __future__ import annotations

from typing import Iterable

GAME_STATUS_SCHEDULED = "SCHEDULED"
GAME_STATUS_LIVE = "LIVE"
GAME_STATUS_DELAYED = "DELAYED"
GAME_STATUS_SUSPENDED = "SUSPENDED"
GAME_STATUS_COMPLETED = "COMPLETED"
GAME_STATUS_DRAW = "DRAW"
GAME_STATUS_CANCELLED = "CANCELLED"
GAME_STATUS_POSTPONED = "POSTPONED"
GAME_STATUS_UNRESOLVED = "UNRESOLVED_MISSING"

LIVE_GAME_STATUSES = {
    GAME_STATUS_LIVE,
    GAME_STATUS_DELAYED,
    GAME_STATUS_SUSPENDED,
}

TERMINAL_GAME_STATUSES = {
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DRAW,
    GAME_STATUS_CANCELLED,
    GAME_STATUS_POSTPONED,
}

COMPLETED_LIKE_GAME_STATUSES = {
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DRAW,
}

ALL_GAME_STATUSES = {
    GAME_STATUS_SCHEDULED,
    *LIVE_GAME_STATUSES,
    *TERMINAL_GAME_STATUSES,
    GAME_STATUS_UNRESOLVED,
}


def is_terminal_status(status: str | None) -> bool:
    return str(status or "").upper() in TERMINAL_GAME_STATUSES


def is_completed_like_status(status: str | None) -> bool:
    return str(status or "").upper() in COMPLETED_LIKE_GAME_STATUSES


def is_live_status(status: str | None) -> bool:
    return str(status or "").upper() in LIVE_GAME_STATUSES


def normalize_game_status(status: str | None) -> str | None:
    if status is None:
        return None
    value = str(status).strip().upper()
    return value if value in ALL_GAME_STATUSES else value


def completed_like_statuses() -> Iterable[str]:
    return tuple(COMPLETED_LIKE_GAME_STATUSES)
