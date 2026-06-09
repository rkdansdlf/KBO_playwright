"""
Play-by-play (PBP) data validation rules.
Validates structural integrity (e.g. missing innings) and score correctness.
Supports two-phase validation: live (structural) and post-game (cross-check).
"""

from __future__ import annotations

import logging
from typing import Any

from src.models.game import Game
from src.repositories.game_helpers import (
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DRAW,
)

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
    """Validate event sequence structure during live play.

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

        # 1. Score regression detection
        if home_score < prev_home_score:
            warnings.append(f"event_{i}: home_score decreased {prev_home_score}->{home_score}")
        if away_score < prev_away_score:
            warnings.append(f"event_{i}: away_score decreased {prev_away_score}->{away_score}")

        # 2. Inning regression detection
        if prev_inning is not None:
            if inning < prev_inning:
                warnings.append(f"event_{i}: inning regressed {prev_inning}->{inning}")
            elif inning == prev_inning and half is not None and prev_half is not None:
                half_order = {"top": 0, "bottom": 1}
                if half_order.get(half, 0) < half_order.get(prev_half, 1):
                    warnings.append(f"event_{i}: half regressed {prev_half}->{half}")

        # 3. Out count anomaly detection
        if outs is not None and prev_outs is not None:
            if outs < 0 or outs > 3:
                warnings.append(f"event_{i}: out count out of range {outs}")
            elif i > 0 and inning == prev_inning and half == prev_half:
                out_diff = outs - prev_outs
                if out_diff < 0:
                    warnings.append(f"event_{i}: outs decreased {prev_outs}->{outs} without inning change")
                elif out_diff > 3:
                    warnings.append(f"event_{i}: outs jumped by {out_diff} in one event")

        # 4. Event order reversal (event_seq continuity)
        seq = event.get("event_seq")
        if seq is not None and i > 0:
            prev_seq = events[i - 1].get("event_seq")
            if prev_seq is not None and seq <= prev_seq:
                warnings.append(f"event_{i}: event_seq reversed {prev_seq}->{seq}")

        home_scores.append(home_score)
        away_scores.append(away_score)
        prev_inning = inning
        prev_half = half
        prev_outs = outs
        prev_home_score = home_score
        prev_away_score = away_score

    return warnings


def cross_validate_with_box_score(
    session,
    game_id: str,
    events: list[dict[str, Any]],
) -> tuple[bool, str | None]:
    """Cross-validate PBP event scores against game_inning_scores table.

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
            "cross_validate_with_box_score: no inning_scores found for game %s — skipping validation", game_id,
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
        pbp_runs = pbp_innings.get(side, {}).get(inn, 0)
        if db_runs != pbp_runs:
            return False, (f"inning_score_mismatch_{side}_inning_{inn}: box_score={db_runs}_pbp={pbp_runs}")

    return True, None


def validate_pbp_payload(
    session,
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

    # 1. Structural Validation (Missing Inning Detection)
    innings_in_pbp = sorted(list(set(row.get("inning") for row in raw_pbp_rows if row.get("inning") is not None)))
    if not innings_in_pbp:
        innings_in_pbp = sorted(list(set(event.get("inning") for event in events if event.get("inning") is not None)))

    if not innings_in_pbp:
        return False, "no_innings_found"

    min_inn = innings_in_pbp[0]
    max_inn = innings_in_pbp[-1]

    if min_inn != 1:
        return False, f"starts_at_inning_{min_inn}_instead_of_1"

    expected_innings = set(range(1, max_inn + 1))
    missing_innings = expected_innings - set(innings_in_pbp)
    if missing_innings:
        return False, f"missing_innings_{sorted(list(missing_innings))}"

    # 2. Score Validation (Final Score Validation)
    game = session.query(Game).filter(Game.game_id == game_id).first()
    if game and game.game_status in (GAME_STATUS_COMPLETED, GAME_STATUS_DRAW, "COMPLETED", "DRAW"):
        db_home_score = game.home_score
        db_away_score = game.away_score

        if db_home_score is not None and db_away_score is not None:
            pbp_home_score = None
            pbp_away_score = None
            for event in reversed(events):
                h_sc = event.get("home_score")
                a_sc = event.get("away_score")
                if h_sc is not None and a_sc is not None:
                    try:
                        pbp_home_score = int(h_sc)
                        pbp_away_score = int(a_sc)
                        break
                    except (ValueError, TypeError):
                        continue

            if pbp_home_score is not None and pbp_away_score is not None:
                if pbp_home_score != db_home_score or pbp_away_score != db_away_score:
                    return (
                        False,
                        f"score_mismatch_pbp_{pbp_home_score}-{pbp_away_score}_vs_db_{db_home_score}-{db_away_score}",
                    )

    return True, None
