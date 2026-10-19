"""Recovery targets must not include games from other seasons (issue #32)."""

from datetime import date
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.cli.pipelines import run_daily_update
from src.models.game import Game


def _targets(games, today):
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(engine)
    session = sessionmaker(bind=engine)()
    session.add_all(games)
    session.commit()

    with patch.object(run_daily_update, "SessionLocal", sessionmaker(bind=engine)):
        # ``today`` has to be close to the 2026 row: the collector applies
        # RECOVERY_LOOKBACK_DAYS (14) on top of the pre-2001 floor, so a today
        # in 2010 put the wanted row in the future and returned an empty list.
        # The assertion then passed for the wrong reason -- it was not the floor
        # that excluded anything, nothing was in range at all.
        targets = run_daily_update._collect_past_scheduled_recovery_targets(today)

    return [t["game_id"] for t in targets]


def test_past_scheduled_recovery_targets_exclude_prior_seasons():
    games = [
        Game(game_id="19950415LTSS0", game_date=date(1995, 4, 15), game_status="UNRESOLVED_MISSING"),
        Game(game_id="20001013LTSS0", game_date=date(2000, 10, 13), game_status="UNRESOLVED_MISSING"),
        Game(game_id="20261010LTSS0", game_date=date(2026, 10, 10), game_status="UNRESOLVED_MISSING"),
    ]

    assert _targets(games, date(2026, 10, 15)) == ["20261010LTSS0"]


def test_past_scheduled_recovery_targets_stop_at_season_boundary():
    games = [
        Game(game_id="20251228LTSS0", game_date=date(2025, 12, 28), game_status="UNRESOLVED_MISSING"),
        Game(game_id="20260102LTSS0", game_date=date(2026, 1, 2), game_status="UNRESOLVED_MISSING"),
    ]

    assert _targets(games, date(2026, 1, 5)) == ["20260102LTSS0"]
