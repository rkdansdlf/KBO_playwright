"""Recovery targets must not include pre-2001 games (issue #32)."""

from datetime import date
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import Game
from src.cli.pipelines import run_daily_update


def test_past_scheduled_recovery_targets_exclude_pre_2001():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(engine)
    session = sessionmaker(bind=engine)()
    session.add_all(
        [
            Game(game_id="19950415LTSS0", game_date=date(1995, 4, 15), game_status="UNRESOLVED_MISSING"),
            Game(game_id="20001013LTSS0", game_date=date(2000, 10, 13), game_status="UNRESOLVED_MISSING"),
            Game(game_id="20100415LTSS0", game_date=date(2010, 4, 15), game_status="UNRESOLVED_MISSING"),
        ],
    )
    session.commit()

    with patch.object(run_daily_update, "SessionLocal", sessionmaker(bind=engine)):
        targets = run_daily_update._collect_past_scheduled_recovery_targets(date(2026, 10, 15))

    ids = [t["game_id"] for t in targets]
    assert ids == ["20100415LTSS0"]
