"""Validation helpers for KBO schedule payloads."""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any
from collections.abc import Mapping

from src.utils.game_status import (
    COMPLETED_LIKE_GAME_STATUSES,
    GAME_STATUS_CANCELLED,
    GAME_STATUS_POSTPONED,
    GAME_STATUS_SCHEDULED,
    LIVE_GAME_STATUSES,
    normalize_game_status,
)
from src.utils.team_codes import KBO_GAME_ID_TEAM_CODES, normalize_kbo_game_id

_GAME_ID_RE = re.compile(r"^(\d{8})([A-Z]+)(\d)$")


def parse_schedule_date(value: Any) -> date | None:
    text = str(value or "").replace("-", "").strip()
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None


def split_schedule_game_id(game_id: Any) -> tuple[str, str, str, str] | None:
    raw = str(game_id or "").strip().upper()
    if not raw:
        return None

    normalized = normalize_kbo_game_id(raw)
    for candidate in (raw, normalized):
        match = _GAME_ID_RE.match(candidate)
        if not match:
            continue
        game_date, team_part, doubleheader_no = match.groups()
        for away_code in KBO_GAME_ID_TEAM_CODES:
            if not team_part.startswith(away_code):
                continue
            home_code = team_part[len(away_code) :]
            if home_code in KBO_GAME_ID_TEAM_CODES:
                return game_date, away_code, home_code, doubleheader_no
    return None


def validate_schedule_game_payload(
    game: Mapping[str, Any],
    *,
    expected_year: int | None = None,
    expected_month: int | None = None,
) -> tuple[bool, str | None]:
    game_id = str(game.get("game_id") or "").strip()
    if not game_id:
        return False, "missing_game_id"

    game_date = parse_schedule_date(game.get("game_date"))
    if not game_date:
        return False, "invalid_game_date"
    if expected_year is not None and game_date.year != expected_year:
        return False, "schedule_date_mismatch"
    if expected_month is not None and game_date.month != expected_month:
        return False, "schedule_date_mismatch"

    id_parts = split_schedule_game_id(game_id)
    if not id_parts:
        return False, "invalid_game_id"
    id_date, _, _, _ = id_parts
    if id_date != game_date.strftime("%Y%m%d"):
        return False, "game_id_date_mismatch"

    for field in ("away_team_code", "home_team_code"):
        if not str(game.get(field) or "").strip():
            return False, f"missing_{field}"

    if normalize_game_status(game.get("game_status")) is None:
        return False, "invalid_game_status"

    if "stadium" not in game or game.get("stadium") is None:
        return False, "missing_stadium"

    return True, None


def is_detail_candidate_game(game: Mapping[str, Any], *, today: date | None = None) -> bool:
    game_date = parse_schedule_date(game.get("game_date"))
    if not game_date:
        return False
    if today is not None and game_date > today:
        return False

    status = normalize_game_status(game.get("game_status")) or GAME_STATUS_SCHEDULED
    if status in {GAME_STATUS_CANCELLED, GAME_STATUS_POSTPONED}:
        return False
    if status in COMPLETED_LIKE_GAME_STATUSES or status in LIVE_GAME_STATUSES:
        return True

    # Older schedule rows may still be marked SCHEDULED even after first pitch.
    # Keep current/past scheduled rows eligible so detail recovery is not skipped.
    if status == GAME_STATUS_SCHEDULED:
        return today is None or game_date <= today

    return False
