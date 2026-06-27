"""
Play-by-play (PBP) data validation rules.

Validates structural integrity (e.g. missing innings) and score correctness.
Supports two-phase validation: live (structural) and post-game (cross-check).
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from src.constants import MAX_OUTS
from src.models.game import Game
from src.repositories.game_helpers import (
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DRAW,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Validation status constants
VALIDATION_PENDING_LIVE = "pending_live"
VALIDATION_PROVISIONALLY_VALID = "provisionally_valid"
VALIDATION_UNVERIFIED = "unverified"
VALIDATION_SOURCE_INCOMPLETE = "source_incomplete"
VALIDATION_SOURCE_UNAVAILABLE = "source_unavailable"
VALIDATION_RECOVERED = "recovered"
VALIDATION_VERIFIED = "verified"

ALL_VALIDATION_STATES = frozenset(
    {
        VALIDATION_PENDING_LIVE,
        VALIDATION_PROVISIONALLY_VALID,
        VALIDATION_UNVERIFIED,
        VALIDATION_SOURCE_INCOMPLETE,
        VALIDATION_SOURCE_UNAVAILABLE,
        VALIDATION_RECOVERED,
        VALIDATION_VERIFIED,
    },
)

# Terminal validation states (no further re-validation expected)
TERMINAL_VALIDATION_STATES = frozenset({VALIDATION_VERIFIED, VALIDATION_RECOVERED, VALIDATION_SOURCE_UNAVAILABLE})


def validate_live_events(events: list[dict[str, Any]]) -> list[str]:
    """
    Validate event sequence structure during live play.

    Checks for structural errors only — no cross-referencing with box scores.
    Returns a list of warning/error messages (empty = clean).
    """
    warnings: list[str] = []
    if not events:
        return warnings

    prev_inning = None
    prev_half = None
    prev_outs = 0
    prev_home_score = 0
    prev_away_score = 0
    home_scores: list[int] = []
    away_scores: list[int] = []

    for i, event in enumerate(events):
        inning = event.get("inning")
        half = event.get("inning_half")
        outs = event.get("outs")
        home_score = event.get("home_score", 0)
        away_score = event.get("away_score", 0)

        warnings.extend(_score_regression_warnings(i, home_score, away_score, prev_home_score, prev_away_score))
        warnings.extend(_inning_regression_warnings(i, inning, half, prev_inning, prev_half))
        warnings.extend(_out_count_warnings(OutCountContext(i, outs, inning, half, prev_outs, prev_inning, prev_half)))
        warnings.extend(_event_sequence_warnings(i, event, events))

        home_scores.append(home_score)
        away_scores.append(away_score)
        prev_inning = inning
        prev_half = half
        prev_outs = outs  # type: ignore[assignment]
        prev_home_score = home_score
        prev_away_score = away_score

    return warnings


def _score_regression_warnings(
    index: int,
    home_score: int,
    away_score: int,
    prev_home_score: int,
    prev_away_score: int,
) -> list[str]:
    """
    Handles the score regression warnings operation.

    Args:
        index: Index.
        home_score: Home Score.
        away_score: Away Score.
        prev_home_score: Prev Home Score.
        prev_away_score: Prev Away Score.

    Returns:
        List of results.

    """
    warnings = []
    if home_score < prev_home_score:
        warnings.append(f"event_{index}: home_score decreased {prev_home_score}->{home_score}")
    if away_score < prev_away_score:
        warnings.append(f"event_{index}: away_score decreased {prev_away_score}->{away_score}")
    return warnings


def _inning_regression_warnings(
    index: int,
    inning: int | None,
    half: str | None,
    prev_inning: int | None,
    prev_half: str | None,
) -> list[str]:
    """
    Handles the inning regression warnings operation.

    Args:
        index: Index.
        inning: Inning number.
        half: Half.
        prev_inning: Prev Inning.
        prev_half: Prev Half.

    Returns:
        List of results.

    """
    if prev_inning is None:
        return []
    if inning < prev_inning:  # type: ignore[operator]
        return [f"event_{index}: inning regressed {prev_inning}->{inning}"]
    if inning == prev_inning and half is not None and prev_half is not None:
        half_order = {"top": 0, "bottom": 1}
        if half_order.get(half, 0) < half_order.get(prev_half, 1):
            return [f"event_{index}: half regressed {prev_half}->{half}"]
    return []


@dataclass
class OutCountContext:
    """OutCountContext class."""

    index: int
    outs: int | None
    inning: int | None
    half: str | None
    prev_outs: int | None
    prev_inning: int | None
    prev_half: str | None


def _out_count_warnings(ctx: OutCountContext) -> list[str]:
    """
    Handles the out count warnings operation.

    Args:
        ctx: Ctx.

    Returns:
        List of results.

    """
    if ctx.outs is None or ctx.prev_outs is None:
        return []
    if ctx.outs < 0 or ctx.outs > MAX_OUTS:
        return [f"event_{ctx.index}: out count out of range {ctx.outs}"]
    if ctx.index <= 0 or ctx.inning != ctx.prev_inning or ctx.half != ctx.prev_half:
        return []

    out_diff = ctx.outs - ctx.prev_outs
    if out_diff < 0:
        return [f"event_{ctx.index}: outs decreased {ctx.prev_outs}->{ctx.outs} without inning change"]
    if out_diff > MAX_OUTS:
        return [f"event_{ctx.index}: outs jumped by {out_diff} in one event"]
    return []


def _event_sequence_warnings(index: int, event: dict[str, Any], events: list[dict[str, Any]]) -> list[str]:
    """
    Handles the event sequence warnings operation.

    Args:
        index: Index.
        event: Event.
        events: List of events.

    Returns:
        List of results.

    """
    seq = event.get("event_seq")
    if seq is None or index <= 0:
        return []
    prev_seq = events[index - 1].get("event_seq")
    if prev_seq is not None and seq <= prev_seq:
        return [f"event_{index}: event_seq reversed {prev_seq}->{seq}"]
    return []


def cross_validate_with_box_score(
    session: Session,
    game_id: str,
    events: list[dict[str, Any]],
) -> tuple[bool, str | None]:
    """
    Cross-validate PBP event scores against game_inning_scores table.

    Returns (is_match, error_reason).
    Only applicable for completed games with inning score data.
    """
    from src.models.game import GameInningScore

    inning_rows = (
        session.query(GameInningScore)
        .filter(GameInningScore.game_id == game_id)
        .order_by(GameInningScore.team_side, GameInningScore.inning)
        .all()
    )
    if not inning_rows:
        logger.warning(
            "cross_validate_with_box_score: no inning_scores found for game %s — skipping validation",
            game_id,
        )
        return True, None  # No box score data to compare against

    # Compute inning-by-inning runs from PBP events
    pbp_innings: dict[str, dict[int, int]] = {"away": {}, "home": {}}
    prev_home = 0
    prev_away = 0

    for event in events:
        raw_home = event.get("home_score")
        raw_away = event.get("away_score")
        inning = event.get("inning")
        half = event.get("inning_half")

        if inning is None:
            continue
        if raw_home is None and raw_away is None:
            logger.debug("Skipping event with null scores: event_seq=%s", event.get("event_seq"))
            continue
        home_cur = raw_home if raw_home is not None else prev_home
        away_cur = raw_away if raw_away is not None else prev_away

        home_runs = home_cur - prev_home
        away_runs = away_cur - prev_away

        if half == "top":
            pbp_innings["away"][inning] = pbp_innings["away"].get(inning, 0) + max(0, away_runs)
        elif half == "bottom":
            pbp_innings["home"][inning] = pbp_innings["home"].get(inning, 0) + max(0, home_runs)

        prev_home = home_cur
        prev_away = away_cur

    # Compare with database inning scores
    for row in inning_rows:
        side = row.team_side  # "away" or "home"
        inn = row.inning
        db_runs = row.runs or 0
        pbp_runs = pbp_innings.get(side, {}).get(inn, 0)  # type: ignore[call-overload]
        if db_runs != pbp_runs:
            return False, (f"inning_score_mismatch_{side}_inning_{inn}: box_score={db_runs}_pbp={pbp_runs}")

    return True, None


def validate_pbp_payload(
    session: Session,
    game_id: str,
    events: list[dict[str, Any]],
    raw_pbp_rows: list[dict[str, Any]],
) -> tuple[bool, str | None]:
    """
    Validate final PBP payload for structural integrity and score correctness.

    Returns (is_valid, error_reason).
    """
    if not events and not raw_pbp_rows:
        return False, "empty_payload"

    inning_error = _validate_pbp_innings(events, raw_pbp_rows)
    if inning_error is not None:
        return False, inning_error

    # 2. Score Validation (Final Score Validation)
    game = session.query(Game).filter(Game.game_id == game_id).first()
    if game and game.game_status in (GAME_STATUS_COMPLETED, GAME_STATUS_DRAW, "COMPLETED", "DRAW"):
        score_error = _validate_pbp_final_score(game, events)
        if score_error is not None:
            return False, score_error

    return True, None


def _validate_pbp_innings(events: list[dict[str, Any]], raw_pbp_rows: list[dict[str, Any]]) -> str | None:
    """
    Validates pbp innings.

    Args:
        events: List of events.
        raw_pbp_rows: Raw Pbp Rows.

    Returns:
        The result of the operation.

    """
    innings_in_pbp = sorted({row.get("inning") for row in raw_pbp_rows if row.get("inning") is not None})  # type: ignore[type-var]
    if not innings_in_pbp:
        innings_in_pbp = sorted({event.get("inning") for event in events if event.get("inning") is not None})  # type: ignore[type-var]

    if not innings_in_pbp:
        return "no_innings_found"

    min_inn = innings_in_pbp[0]
    max_inn = innings_in_pbp[-1]
    if min_inn != 1:
        return f"starts_at_inning_{min_inn}_instead_of_1"

    missing_innings = set(range(1, int(max_inn) + 1)) - set(innings_in_pbp)  # type: ignore[arg-type]
    if missing_innings:
        return f"missing_innings_{sorted(missing_innings)}"
    return None


def _validate_pbp_final_score(game: Game, events: list[dict[str, Any]]) -> str | None:
    """
    Validates pbp final score.

    Args:
        game: Game.
        events: List of events.

    Returns:
        The result of the operation.

    """
    db_home_score = game.home_score
    db_away_score = game.away_score

    if db_home_score is None or db_away_score is None:
        return None

    pbp_score = _last_pbp_score(events)
    if pbp_score is None:
        return None

    pbp_home_score, pbp_away_score = pbp_score
    if pbp_home_score != db_home_score or pbp_away_score != db_away_score:
        return f"score_mismatch_pbp_{pbp_home_score}-{pbp_away_score}_vs_db_{db_home_score}-{db_away_score}"
    return None


def _last_pbp_score(events: list[dict[str, Any]]) -> tuple[int, int] | None:
    """
    Handles the last pbp score operation.

    Args:
        events: List of events.

    Returns:
        The result of the operation.

    """
    for event in reversed(events):
        h_sc = event.get("home_score")
        a_sc = event.get("away_score")
        if h_sc is None or a_sc is None:
            continue
        try:
            return int(h_sc), int(a_sc)
        except (ValueError, TypeError):
            continue
    return None
