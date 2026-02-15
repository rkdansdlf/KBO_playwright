from datetime import date

from scripts.maintenance.refresh_game_status import (
    STATUS_CANCELLED,
    STATUS_COMPLETED,
    STATUS_POSTPONED,
    STATUS_SCHEDULED,
    STATUS_UNRESOLVED,
    derive_game_status,
)


def test_completed_when_scores_exist():
    status = derive_game_status(
        game_date=date(2025, 7, 1),
        home_score=3,
        away_score=2,
        has_metadata=True,
        has_inning_scores=True,
        has_lineups=True,
        has_batting=True,
        has_pitching=True,
        today=date(2026, 2, 14),
    )
    assert status == STATUS_COMPLETED


def test_scheduled_when_missing_score_and_future_date():
    status = derive_game_status(
        game_date=date(2026, 3, 20),
        home_score=None,
        away_score=None,
        has_metadata=False,
        has_inning_scores=False,
        has_lineups=False,
        has_batting=False,
        has_pitching=False,
        today=date(2026, 2, 14),
    )
    assert status == STATUS_SCHEDULED


def test_cancelled_when_past_missing_with_metadata_and_no_details():
    status = derive_game_status(
        game_date=date(2019, 5, 10),
        home_score=None,
        away_score=None,
        has_metadata=True,
        has_inning_scores=False,
        has_lineups=False,
        has_batting=False,
        has_pitching=False,
        today=date(2026, 2, 14),
    )
    assert status == STATUS_CANCELLED


def test_unresolved_when_past_missing_without_metadata():
    status = derive_game_status(
        game_date=date(2019, 5, 10),
        home_score=None,
        away_score=None,
        has_metadata=False,
        has_inning_scores=False,
        has_lineups=False,
        has_batting=False,
        has_pitching=False,
        today=date(2026, 2, 14),
    )
    assert status == STATUS_UNRESOLVED


def test_manual_override_takes_priority_over_inferred_cancelled():
    status = derive_game_status(
        game_date=date(2019, 5, 10),
        home_score=None,
        away_score=None,
        has_metadata=True,
        has_inning_scores=False,
        has_lineups=False,
        has_batting=False,
        has_pitching=False,
        manual_status=STATUS_POSTPONED,
        today=date(2026, 2, 14),
    )
    assert status == STATUS_POSTPONED


def test_future_game_stays_scheduled_even_with_manual_override():
    status = derive_game_status(
        game_date=date(2026, 3, 20),
        home_score=None,
        away_score=None,
        has_metadata=True,
        has_inning_scores=False,
        has_lineups=False,
        has_batting=False,
        has_pitching=False,
        manual_status=STATUS_CANCELLED,
        today=date(2026, 2, 14),
    )
    assert status == STATUS_SCHEDULED


def test_past_game_is_never_scheduled():
    status = derive_game_status(
        game_date=date(2019, 5, 10),
        home_score=None,
        away_score=None,
        has_metadata=False,
        has_inning_scores=False,
        has_lineups=False,
        has_batting=False,
        has_pitching=False,
        today=date(2026, 2, 14),
    )
    assert status != STATUS_SCHEDULED
