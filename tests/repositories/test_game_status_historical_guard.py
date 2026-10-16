"""Pre-2001 guard for refresh_game_status_for_date (issue #32)."""

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import (
    Game,
    GameBattingStat,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
)
from src.repositories.game_status import refresh_game_status_for_date


def _session():
    engine = create_engine("sqlite:///:memory:")
    for table in (Game, GameMetadata, GameInningScore, GameLineup, GameBattingStat, GamePitchingStat):
        table.__table__.create(engine)
    return sessionmaker(bind=engine)()


def test_refresh_refuses_pre_2001_date():
    session = _session()
    session.add(
        Game(
            game_id="19950415LTSS0",
            game_date=date(1995, 4, 15),
            home_score=None,
            away_score=None,
            game_status="UNRESOLVED_MISSING",
        ),
    )
    session.commit()

    result = refresh_game_status_for_date("19950415", today=date(2026, 10, 15), session=session)

    assert result["total"] == 0
    assert result["updated"] == 0
    assert session.query(Game).one().game_status == "UNRESOLVED_MISSING"


def test_refresh_allows_2001_and_later():
    session = _session()
    session.add(
        Game(
            game_id="20010415LTSS0",
            game_date=date(2001, 4, 15),
            home_score=None,
            away_score=None,
            game_status="SCHEDULED",
        ),
    )
    session.commit()

    result = refresh_game_status_for_date("20010415", today=date(2026, 10, 16), session=session)

    assert result["total"] == 1
