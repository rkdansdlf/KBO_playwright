"""Shared KBO game status constants and helpers."""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

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
    """Return whether the terminal status.

    Args:
        status: Status.
        status: Status.
        status: Status.

    Returns:
        True if the condition is met, False otherwise.

    """
    return str(status or "").upper() in TERMINAL_GAME_STATUSES


def is_completed_like_status(status: str | None) -> bool:
    """Return whether the completed like status.

    Args:
        status: Status.
        status: Status.
        status: Status.

    Returns:
        True if the condition is met, False otherwise.

    """
    return str(status or "").upper() in COMPLETED_LIKE_GAME_STATUSES


def is_live_status(status: str | None) -> bool:
    """Return whether the live status.

    Args:
        status: Status.
        status: Status.
        status: Status.

    Returns:
        True if the condition is met, False otherwise.

    """
    return str(status or "").upper() in LIVE_GAME_STATUSES


def normalize_game_status(status: str | None) -> str | None:
    """Normalize game status.

    Args:
        status: Status.
        status: Status.
        status: Status.

    Returns:
        The result of the operation.

    """
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
    """Handle the completed like statuses operation.

    Returns:
        The result of the operation.

    """
    return tuple(COMPLETED_LIKE_GAME_STATUSES)


@dataclass(frozen=True)
class GameStatusEvidence:
    """GameStatusEvidence class."""

    game_date: date
    current_status: str | None = None
    new_status: str | None = None
    home_score: int | None = None
    away_score: int | None = None
    has_progress_evidence: bool = False
    today: date | None = None


def _resolve_scored_status(current_status: str | None, new_status: str | None, home_score: int, away_score: int) -> str:
    """Resolve scored status.

    Args:
        current_status: Current Status.
        new_status: New Status.
        home_score: Home Score.
        away_score: Away Score.
        current_status: Current Status.
        new_status: New Status.
        home_score: Home Score.
        away_score: Away Score.
        current_status: Current Status.
        new_status: New Status.
        home_score: Home Score.
        away_score: Away Score.

    Returns:
        String result.

    """
    if is_terminal_status(current_status) and not is_terminal_status(new_status) and new_status is not None:
        return current_status or GAME_STATUS_UNRESOLVED
    return GAME_STATUS_DRAW if home_score == away_score else GAME_STATUS_COMPLETED


def _resolve_today_status(evidence: GameStatusEvidence, new_status: str | None) -> str:
    """Resolve today status.

    Args:
        evidence: Evidence.
        new_status: New Status.
        evidence: Evidence.
        new_status: New Status.
        evidence: Evidence.
        new_status: New Status.

    Returns:
        String result.

    """
    if evidence.has_progress_evidence or is_live_status(new_status):
        return new_status or GAME_STATUS_LIVE if evidence.has_progress_evidence else GAME_STATUS_SCHEDULED
    return GAME_STATUS_SCHEDULED


def _resolve_past_status(current_status: str | None, new_status: str | None) -> str:
    """Resolve past status.

    Args:
        current_status: Current Status.
        new_status: New Status.
        current_status: Current Status.
        new_status: New Status.
        current_status: Current Status.
        new_status: New Status.

    Returns:
        String result.

    """
    if is_terminal_status(new_status):
        return new_status or GAME_STATUS_UNRESOLVED
    if is_terminal_status(current_status):
        return current_status or GAME_STATUS_UNRESOLVED
    return GAME_STATUS_UNRESOLVED


def derive_stable_game_status(evidence: GameStatusEvidence | None = None, **kwargs: object) -> str:
    """Central logic to resolve game status based on date and evidence.

    Ensure stability and prevents premature LIVE/COMPLETED transitions.

    Args:
        evidence: Evidence.
        kwargs: Keyword arguments to pass through.
        evidence: Evidence.
        kwargs: Keyword arguments to pass through.

    """
    if evidence is None:
        evidence = GameStatusEvidence(**kwargs)  # type: ignore[arg-type]
    elif kwargs:
        msg = "Pass either GameStatusEvidence or keyword evidence, not both"
        raise TypeError(msg)

    if evidence.today is None:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        today = datetime.now(ZoneInfo("Asia/Seoul")).date()
    else:
        today = evidence.today

    if evidence.game_date > today:
        return GAME_STATUS_SCHEDULED

    current_status = normalize_game_status(evidence.current_status)
    new_status = normalize_game_status(evidence.new_status)

    if evidence.home_score is not None and evidence.away_score is not None:
        return _resolve_scored_status(current_status, new_status, evidence.home_score, evidence.away_score)

    if evidence.game_date == today:
        return _resolve_today_status(evidence, new_status)

    if evidence.game_date < today:
        return _resolve_past_status(current_status, new_status)

    return new_status or GAME_STATUS_SCHEDULED
