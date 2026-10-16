"""Current-season guard for game_status writes (issue #32)."""

from datetime import date

import pytest
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
from src.repositories import game_status
from src.repositories.game_status import refresh_game_status_for_date, update_game_status

SEASON = 2026


@pytest.fixture(autouse=True)
def _pin_season(monkeypatch):
    monkeypatch.setattr(game_status, "current_season_year", lambda today=None: SEASON)


def _session(*games):
    engine = create_engine("sqlite:///:memory:")
    for table in (Game, GameMetadata, GameInningScore, GameLineup, GameBattingStat, GamePitchingStat):
        table.__table__.create(engine)
    session = sessionmaker(bind=engine)()
    session.add_all(games)
    session.commit()
    return session


@pytest.mark.parametrize(
    ("game_id", "game_date"),
    [("19950415LTSS0", date(1995, 4, 15)), ("20251004LTSS0", date(2025, 10, 4))],
)
def test_refresh_skips_prior_season_rows(game_id, game_date):
    session = _session(
        Game(game_id=game_id, game_date=game_date, home_score=None, away_score=None, game_status="UNRESOLVED_MISSING"),
    )

    result = refresh_game_status_for_date(game_date.strftime("%Y%m%d"), today=date(2026, 10, 15), session=session)

    assert result["total"] == 1
    assert result["updated"] == 0
    assert result["skipped_past_season"] == 1
    assert session.query(Game).one().game_status == "UNRESOLVED_MISSING"


def test_refresh_processes_current_season_rows():
    session = _session(
        Game(
            game_id="20261016LTSS0",
            game_date=date(2026, 10, 16),
            home_score=None,
            away_score=None,
            game_status="SCHEDULED",
        ),
    )

    result = refresh_game_status_for_date("20261016", today=date(2026, 10, 16), session=session)

    assert result["total"] == 1
    assert result["skipped_past_season"] == 0


def test_update_refuses_prior_season_without_opt_in():
    session = _session(Game(game_id="19950415LTSS0", game_date=date(1995, 4, 15), game_status="UNRESOLVED_MISSING"))

    assert update_game_status("19950415LTSS0", "CANCELLED", session=session) is False
    assert session.query(Game).one().game_status == "UNRESOLVED_MISSING"


def test_update_allows_prior_season_with_explicit_opt_in():
    session = _session(Game(game_id="19950415LTSS0", game_date=date(1995, 4, 15), game_status="UNRESOLVED_MISSING"))

    assert update_game_status("19950415LTSS0", "CANCELLED", session=session, allow_historical=True) is True
    assert session.query(Game).one().game_status == "CANCELLED"


def test_update_allows_current_season_without_opt_in():
    session = _session(Game(game_id="20261016LTSS0", game_date=date(2026, 10, 16), game_status="SCHEDULED"))

    assert update_game_status("20261016LTSS0", "CANCELLED", session=session) is True
