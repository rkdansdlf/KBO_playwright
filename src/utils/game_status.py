"""Shared KBO game status constants and helpers."""
from __future__ import annotations

from datetime import date
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
    aliases = {
        "CANCELED": GAME_STATUS_CANCELLED,
        "CANCEL": GAME_STATUS_CANCELLED,
        "CANCELLED_GAME": GAME_STATUS_CANCELLED,
    }
    value = aliases.get(value, value)
    return value if value in ALL_GAME_STATUSES else None


def completed_like_statuses() -> Iterable[str]:
    return tuple(COMPLETED_LIKE_GAME_STATUSES)


def derive_stable_game_status(
    *,
    game_date: date,
    current_status: str | None = None,
    new_status: str | None = None,
    home_score: int | None = None,
    away_score: int | None = None,
    has_progress_evidence: bool = False,
    today: date | None = None,
) -> str:
    """
    Central logic to resolve game status based on date and evidence.
    Ensures stability and prevents premature LIVE/COMPLETED transitions.
    """
    today = today or date.today()
    
    # 1. Future games are strictly SCHEDULED unless manually overridden (not handled here)
    if game_date > today:
        return GAME_STATUS_SCHEDULED
        
    # 2. Normalize inputs
    current_status = normalize_game_status(current_status)
    new_status = normalize_game_status(new_status)
    
    # 3. If we have scores and it's past or today, consider it terminal if context allows
    if home_score is not None and away_score is not None:
        # If it was already terminal, don't move it back unless the new status is also terminal
        if is_terminal_status(current_status) and not is_terminal_status(new_status) and new_status is not None:
            return current_status or GAME_STATUS_UNRESOLVED
        return GAME_STATUS_DRAW if home_score == away_score else GAME_STATUS_COMPLETED

    # 4. Handle today's games
    if game_date == today:
        # Only allow LIVE if there is actual evidence of progress
        if has_progress_evidence or is_live_status(new_status):
            # Even if new_status is LIVE, if we have NO evidence, be skeptical? 
            # For now, if a crawler explicitly says LIVE, we might trust it, 
            # but the goal is to be conservative.
            return new_status or GAME_STATUS_LIVE if has_progress_evidence else GAME_STATUS_SCHEDULED
        
        # If it's today and we had it as LIVE, but now have no evidence and no new status, 
        # keep it LIVE to avoid flickering, OR move back to SCHEDULED?
        # Decision: If no evidence and it's today, SCHEDULED is safer.
        return GAME_STATUS_SCHEDULED

    # 5. Handle past games
    if game_date < today:
        if is_terminal_status(new_status):
            return new_status or GAME_STATUS_UNRESOLVED
        if is_terminal_status(current_status):
            return current_status or GAME_STATUS_UNRESOLVED
            
        # Past games without scores should eventually be CANCELLED or UNRESOLVED
        return new_status or current_status or GAME_STATUS_UNRESOLVED

    return new_status or GAME_STATUS_SCHEDULED
