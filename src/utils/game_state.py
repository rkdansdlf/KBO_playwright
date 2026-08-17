"""Game lifecycle state machine for live relay crawling.

Tracks the progression of a game through its lifecycle stages,
enabling precise detection of game end, suspension, cancellation,
and stabilization windows.

"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Mapping

GameLifecycleState = Literal[
    "before",
    "running",
    "delayed",
    "suspended",
    "cancelled",
    "result_pending_stabilization",
    "final",
]

# All valid lifecycle states
LIFECYCLE_STATES: set[str] = {
    "before",
    "running",
    "delayed",
    "suspended",
    "cancelled",
    "result_pending_stabilization",
    "final",
}

# Terminal states (no further transitions expected)
TERMINAL_STATES: set[str] = {"cancelled", "final"}

# States where relay data is expected
RELAY_ACTIVE_STATES: set[str] = {"running", "delayed", "suspended", "result_pending_stabilization"}

# Allowed transitions: (from_state, to_state) pairs
ALLOWED_TRANSITIONS: set[tuple[str, str]] = {
    ("before", "running"),
    ("before", "cancelled"),
    ("running", "delayed"),
    ("running", "suspended"),
    ("running", "result_pending_stabilization"),
    ("running", "cancelled"),
    ("delayed", "running"),
    ("delayed", "suspended"),
    ("delayed", "result_pending_stabilization"),
    ("suspended", "running"),
    ("suspended", "delayed"),
    ("suspended", "cancelled"),
    ("suspended", "result_pending_stabilization"),
    ("result_pending_stabilization", "final"),
    ("result_pending_stabilization", "running"),
    ("result_pending_stabilization", "delayed"),
    ("result_pending_stabilization", "suspended"),
}


def is_terminal(state: str) -> bool:
    """Return whether the terminal.

    Args:
        state: State.
        state: State.
        state: State.

    Returns:
        True if the condition is met, False otherwise.

    """
    return state in TERMINAL_STATES


def validate_transition(current: str | None, next_state: str) -> tuple[bool, str | None]:
    """Check if a lifecycle state transition is valid.

    Return (is_valid, error_reason).

    Args:
        current: Current.
        next_state: Next State.
        current: Current.
        next_state: Next State.

    """
    if next_state not in LIFECYCLE_STATES:
        return False, f"unknown_lifecycle_state_{next_state}"

    if current is None:
        return True, None

    if current not in LIFECYCLE_STATES:
        return False, f"unknown_current_state_{current}"

    if current in TERMINAL_STATES:
        return False, f"terminal_state_{current}_cannot_transition"

    if (current, next_state) in ALLOWED_TRANSITIONS:
        return True, None

    return False, f"invalid_transition_{current}_to_{next_state}"


def derive_lifecycle_from_naver_status(nav_status: str | None) -> str | None:
    """Map a Naver schedule API status to a GameLifecycleState.

    Args:
        nav_status: Nav Status.
        nav_status: Nav Status.

    """
    if not nav_status:
        return None
    upper = nav_status.upper().strip()
    mapping = {
        "BEFORE": "before",
        "SCHEDULED": "before",
        "RUNNING": "running",
        "STARTED": "running",
        "LIVE": "running",
        "IN_PROGRESS": "running",
        "RESULT": "result_pending_stabilization",
        "CANCEL": "cancelled",
        "CANCELLED": "cancelled",
        "POSTPONED": "cancelled",
        "DELAYED": "delayed",
        "SUSPENDED": "suspended",
        "ENDED": "final",
        "END": "final",
        "FINISHED": "final",
        "FINAL": "final",
    }
    return mapping.get(upper)


def extract_naver_status(game: Mapping[str, object]) -> str | None:
    """Extract a lifecycle-relevant status from a Naver schedule payload."""
    if game.get("cancel") is True:
        return "CANCELLED"
    if game.get("suspended") is True:
        return "SUSPENDED"

    for field in ("statusInfo", "statusDescription", "statusName"):
        value = game.get(field)
        if isinstance(value, str) and any(marker in value.casefold() for marker in ("취소", "cancel", "postpon")):
            return "CANCELLED"

    for field in ("status", "statusCode", "gameStatus", "gameState", "progressState"):
        value = game.get(field)
        if value and isinstance(value, str):
            return value.strip().upper()
    return None


NAVER_STATUS_MAP = {s.lower(): s.upper() for s in ["before", "running", "result", "cancel", "delayed", "suspended"]}
