"""Pre-2001 writes for refresh_game_status_for_date (issue #32).

The floor is a policy decision rather than a hard-coded year. The default
policy refuses any season outside its grace window, and an approval recorded in
``Docs/config/historical_backfill_dates.yaml`` is the one thing that lets a
historical date be rewritten. ``update_game_status`` keeps its own
``allow_historical`` opt-in for a single operator-specified row; this file
covers the date-scoped scan.

A refusal still reports the games it found and the transitions it would have
made. The caller is a batch that must carry on, so "refused" has to be
answerable from the result rather than from an exception.
"""

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config.historical_backfill_policy import (
    REASON_SEASON_NOT_APPROVED,
    HistoricalBackfillPolicy,
)
from src.models.audit_trail import CorrectionAuditTrail
from src.models.game import (
    Game,
    GameBattingStat,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
)
from src.repositories import game_status
from src.repositories.game_status import (
    StatusRefreshOptions,
    refresh_game_status_for_date,
    update_game_status,
)

SEASON = 2026


@pytest.fixture(autouse=True)
def _pin_season(monkeypatch):
    monkeypatch.setattr(game_status, "current_season_year", lambda today=None: SEASON)


def _session(*games):
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game,
        GameMetadata,
        GameInningScore,
        GameLineup,
        GameBattingStat,
        GamePitchingStat,
        CorrectionAuditTrail,
    ):
        table.__table__.create(engine)
    session = sessionmaker(bind=engine)()
    session.add_all(games)
    session.commit()
    return session


def _historical_game(session, *, game_status: str = "UNRESOLVED_MISSING", with_result: bool = False) -> None:
    """Add one 1995 game with no bodies -- the shape the guard protects.

    ``with_result`` gives the row a score and metadata, which is what makes the
    refresh want to change it: a past game with a score and no body is the
    ``CANCELLED`` inference, and a row the refresh leaves alone cannot show that
    an approval reached the write path.
    """
    session.add(
        Game(
            game_id="19950415LTSS0",
            game_date=date(1995, 4, 15),
            home_score=4 if with_result else None,
            away_score=1 if with_result else None,
            game_status=game_status,
        ),
    )
    if with_result:
        session.add(GameMetadata(game_id="19950415LTSS0", stadium_code="JAMSIL"))
    session.commit()
    return session


def test_refresh_refuses_an_unapproved_pre_2001_date():
    session = _session()
    _historical_game(session)

    result = refresh_game_status_for_date("19950415", today=date(2026, 10, 15), session=session)

    assert result["skipped"] is True
    assert result["write_allowed"] is False
    assert result["updated"] == 0
    assert result["skip_reason"] is not None
    assert result["skip_reason"].startswith(REASON_SEASON_NOT_APPROVED)
    assert session.query(Game).one().game_status == "UNRESOLVED_MISSING"


def test_refresh_writes_an_approved_pre_2001_date():
    """An approval in the committed policy file is what opens the write."""
    session = _session()
    _historical_game(session, game_status="COMPLETED", with_result=True)
    policy = HistoricalBackfillPolicy(approved_dates=frozenset({"19950415"}))

    result = refresh_game_status_for_date(
        "19950415",
        today=date(2026, 10, 15),
        session=session,
        options=StatusRefreshOptions(caller="test", policy=policy),
    )

    assert result["skipped"] is False
    assert result["write_allowed"] is True
    assert result["updated"] == 1


def test_refresh_allows_2001_and_later():
    session = _session()
    session.add(
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
