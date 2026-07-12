"""Validation helpers for player identity payloads."""

from __future__ import annotations

import re
from collections import Counter
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

INVALID_PLAYER_NAMES = {
    "",
    "-",
    "N/A",
    "NA",
    "UNKNOWN",
    "UNKNOWN PLAYER",
    "Unknown",
    "Unknown Player",
    "타자",
    "투수",
    "은퇴선수(타자)",
    "은퇴선수(투수)",
    "은퇴선수",
    "선수",
}
UNKNOWN_ID_NAME_RE = re.compile(r"^unknown\s+\d+$", re.IGNORECASE)


def normalize_player_name(name: object) -> str:
    """Normalize player name.

    Args:
        name: Name.
        name: Name.
        name: Name.

    Returns:
        String result.

    """
    if name is None:
        return ""
    return str(name).strip()


def is_invalid_player_name(name: object) -> bool:
    """Return whether the invalid player name.

    Args:
        name: Name.
        name: Name.
        name: Name.

    Returns:
        True if the condition is met, False otherwise.

    """
    normalized = normalize_player_name(name)

    return (
        not normalized
        or normalized.upper() in {value.upper() for value in INVALID_PLAYER_NAMES}
        or UNKNOWN_ID_NAME_RE.match(normalized) is not None
    )


def normalize_player_id(player_id: object) -> int | None:
    """Normalize player id.

    Args:
        player_id: Player ID.
        player_id: Player ID.
        player_id: Player ID.

    Returns:
        The result of the operation.

    """
    if player_id is None:
        return None
    try:
        normalized = int(str(player_id).strip())
    except (TypeError, ValueError):
        return None
    return normalized if normalized > 0 else None


def validate_player_payload(payload: Mapping[str, Any]) -> tuple[bool, str | None]:
    """Validate player payload.

    Args:
        payload: Payload.
        payload: Payload.
        payload: Data payload to process.

    Returns:
        Tuple result.

    """
    player_id = normalize_player_id(payload.get("player_id"))

    if player_id is None:
        return False, "invalid_player_id"

    if is_invalid_player_name(payload.get("name") or payload.get("player_name")):
        raw_name = normalize_player_name(payload.get("name") or payload.get("player_name"))
        if (
            raw_name.upper() in {value.upper() for value in INVALID_PLAYER_NAMES if value}
            or UNKNOWN_ID_NAME_RE.match(raw_name) is not None
        ):
            return False, "unknown_player_name"
        return False, "missing_player_name"

    return True, None


def filter_valid_player_payloads(
    payloads: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], Counter[str]]:
    """Filter valid player payloads.

    Args:
        payloads: Payloads.
        payloads: Payloads.
        payloads: Payloads.

    Returns:
        Tuple result.

    """
    filtered: list[dict[str, Any]] = []

    reasons: Counter[str] = Counter[str]()

    for payload in payloads:
        ok, reason = validate_player_payload(payload)
        if not ok:
            reasons[reason or "invalid_player_payload"] += 1
            continue
        row = dict(payload)
        row["player_id"] = normalize_player_id(row.get("player_id"))
        row["name"] = normalize_player_name(row.get("name") or row.get("player_name"))
        filtered.append(row)

    return filtered, reasons
