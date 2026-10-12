"""Why the auto-healer skips 1982, verified against the query rather than a mock.

The comment beside the filter used to claim the 1982 season was never published
online. It was: ``data/archives/1982_namu_raw.json`` and
``1982_answer_set_final.json`` hold 241 games with home/away scores. What is
absent is the boxscore body -- no batter, pitcher or play-by-play row exists to
re-crawl into, so a healing attempt has nothing to find.

The filter is correct; the reason written beside it was not. A reason that is
wrong gets "fixed" later by deleting the filter, which is how a season that
cannot be healed turns into one that is re-crawled forever.

The other 1982 fact belongs here too, because the two are easy to confuse: the
241 games are each stored **twice**, the KBO schedule crawl and the answer-set
apply having written opposite home/away rows under different game ids. 241 rows
read CANCELLED and 241 read UNRESOLVED_MISSING, and each pair is one game. That
duplication is a separate cleanup.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.cli.backfill.auto_healer import _find_stuck_games
from src.models.game import Game
from src.utils.game_status import GAME_STATUS_CANCELLED, GAME_STATUS_SCHEDULED, GAME_STATUS_UNRESOLVED

#: The floor the filter uses. Named so a test that changes it is visible.
SEASON_FLOOR = date(2001, 1, 1)


@pytest.fixture
def session_factory() -> sessionmaker:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Game.__table__.create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def _game(game_id: str, game_date: date, status: str) -> Game:
    return Game(game_id=game_id, game_date=game_date, game_status=status)


class TestTheExclusionHoldsForEveryStatusInThatSeason:
    def test_an_unresolved_1982_row_is_not_offered_for_healing(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One half of the duplicated 1982 pair carries exactly this status."""
        with session_factory() as session:
            session.add_all(
                [
                    _game("G1982", date(1982, 3, 27), GAME_STATUS_UNRESOLVED),
                    _game("G2001", date(2001, 4, 1), GAME_STATUS_UNRESOLVED),
                ],
            )
            session.commit()
        monkeypatch.setattr("src.cli.backfill.auto_healer.SessionLocal", session_factory)

        stuck = _find_stuck_games()

        assert [g.game_id for g in stuck] == ["G2001"]

    def test_a_scheduled_1982_row_is_not_offered_either(self, session_factory: sessionmaker, monkeypatch) -> None:
        """The filter is the season, not the status.

        Half of the duplicated pair reads CANCELLED and the rest
        UNRESOLVED_MISSING; a test written against one status would pass while
        the other half of the same season sat in the healing queue.
        """
        with session_factory() as session:
            session.add_all(
                [
                    _game("G1982_SCHEDULED", date(1982, 3, 28), GAME_STATUS_SCHEDULED),
                    _game("G2001_SCHEDULED", date(2001, 4, 2), GAME_STATUS_SCHEDULED),
                ],
            )
            session.commit()
        monkeypatch.setattr("src.cli.backfill.auto_healer.SessionLocal", session_factory)

        stuck = _find_stuck_games()

        assert [g.game_id for g in stuck] == ["G2001_SCHEDULED"]

    def test_a_terminal_1982_row_was_already_excluded_by_status(
        self, session_factory: sessionmaker, monkeypatch
    ) -> None:
        """CANCELLED is terminal, so the season filter is not what excludes it.

        Worth stating because it is the other half of the duplicated pair: if the
        floor were removed tomorrow, these rows would still not be queued, and
        the floor would look redundant.
        """
        with session_factory() as session:
            session.add_all(
                [
                    _game("G1982_CANCELLED", date(1982, 3, 28), GAME_STATUS_CANCELLED),
                    _game("G2001_SCHEDULED", date(2001, 4, 3), GAME_STATUS_SCHEDULED),
                ],
            )
            session.commit()
        monkeypatch.setattr("src.cli.backfill.auto_healer.SessionLocal", session_factory)

        stuck = _find_stuck_games()

        assert [g.game_id for g in stuck] == ["G2001_SCHEDULED"]


class TestTheFloorIsASeasonAndNotAnAccident:
    def test_the_boundary_year_is_included(self, session_factory: sessionmaker, monkeypatch) -> None:
        with session_factory() as session:
            session.add(_game("G_ON_FLOOR", SEASON_FLOOR, GAME_STATUS_SCHEDULED))
            session.commit()
        monkeypatch.setattr("src.cli.backfill.auto_healer.SessionLocal", session_factory)

        assert [g.game_id for g in _find_stuck_games()] == ["G_ON_FLOOR"]

    def test_the_day_before_the_floor_is_not(self, session_factory: sessionmaker, monkeypatch) -> None:
        with session_factory() as session:
            session.add(_game("G_BEFORE", date(2000, 12, 31), GAME_STATUS_SCHEDULED))
            session.commit()
        monkeypatch.setattr("src.cli.backfill.auto_healer.SessionLocal", session_factory)

        assert _find_stuck_games() == []
