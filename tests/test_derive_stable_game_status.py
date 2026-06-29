from datetime import date

from src.utils.game_status import (
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DRAW,
    GAME_STATUS_LIVE,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_UNRESOLVED,
    derive_stable_game_status,
)


def test_derive_stable_future_is_always_scheduled():
    # Future game should be SCHEDULED regardless of input
    today = date(2026, 5, 14)
    game_date = date(2026, 5, 15)

    status = derive_stable_game_status(game_date=game_date, new_status=GAME_STATUS_LIVE, today=today)
    assert status == GAME_STATUS_SCHEDULED


def test_derive_stable_today_live_requires_evidence():
    today = date(2026, 5, 14)
    game_date = today

    # Case 1: New status is LIVE but no evidence -> SCHEDULED
    status = derive_stable_game_status(
        game_date=game_date,
        new_status=GAME_STATUS_LIVE,
        has_progress_evidence=False,
        today=today,
    )
    assert status == GAME_STATUS_SCHEDULED

    # Case 2: New status is LIVE and has evidence -> LIVE
    status = derive_stable_game_status(
        game_date=game_date,
        new_status=GAME_STATUS_LIVE,
        has_progress_evidence=True,
        today=today,
    )
    assert status == GAME_STATUS_LIVE


def test_derive_stable_terminal_with_scores():
    today = date(2026, 5, 14)
    game_date = today

    # Case 1: Scores present -> COMPLETED
    status = derive_stable_game_status(game_date=game_date, home_score=5, away_score=3, today=today)
    assert status == GAME_STATUS_COMPLETED

    # Case 2: Tied scores -> DRAW
    status = derive_stable_game_status(game_date=game_date, home_score=4, away_score=4, today=today)
    assert status == GAME_STATUS_DRAW


def test_derive_stable_past_unresolved_remains_unresolved_if_no_scores():
    today = date(2026, 5, 14)
    game_date = date(2026, 5, 13)

    status = derive_stable_game_status(game_date=game_date, current_status=GAME_STATUS_UNRESOLVED, today=today)
    assert status == GAME_STATUS_UNRESOLVED


def test_derive_stable_past_scheduled_advances_to_unresolved_if_no_scores():
    today = date(2026, 5, 14)
    game_date = date(2026, 5, 13)

    status = derive_stable_game_status(
        game_date=game_date,
        current_status=GAME_STATUS_SCHEDULED,
        new_status=GAME_STATUS_SCHEDULED,
        today=today,
    )
    assert status == GAME_STATUS_UNRESOLVED


def test_derive_stable_prevents_terminal_reversion():
    today = date(2026, 5, 14)
    game_date = date(2026, 5, 13)

    # If already COMPLETED, a new snapshot saying LIVE should be ignored if it's past
    status = derive_stable_game_status(
        game_date=game_date,
        current_status=GAME_STATUS_COMPLETED,
        new_status=GAME_STATUS_LIVE,
        home_score=5,
        away_score=3,
        today=today,
    )
    assert status == GAME_STATUS_COMPLETED
