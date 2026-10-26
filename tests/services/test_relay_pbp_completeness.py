"""Completeness of a stored play-by-play record, and the two traps in judging it.

The question is not "does a row exist" -- ten 2026 games answered yes with 5 to
213 rows of a nine-inning record. It is also not simply "did it reach the last
inning the score table lists": every 2025-03-22 game is recorded to the 12th
inning while its play-by-play ends at the 9th with the feed's closing marker, so
an inning-count test alone calls 736 healthy games incomplete.

These cases pin both traps at once, because fixing either one alone reintroduces
the other.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import Game, GameInningScore, GamePlayByPlay
from src.services.relay_recovery_service import FINAL_PBP_MARKER, pbp_complete_game_ids

GAME = "20261020HTLG0"
#: The shape that was stored short: eight innings, nine played, no closing marker.
SHORT_GAME = "20260920HTNC0"


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    for table in (Game.__table__, GameInningScore.__table__, GamePlayByPlay.__table__):
        table.create(engine)
    db = sessionmaker(bind=engine)()
    for game_id, day in ((GAME, date(2026, 10, 20)), (SHORT_GAME, date(2026, 9, 20))):
        db.add(Game(game_id=game_id, game_date=day, game_status="COMPLETED"))
    db.commit()
    yield db
    db.close()


def _pbp(
    session,
    inning: int,
    *,
    description: str = "1구 볼",
    half: str = "top",
    game_id: str = GAME,
) -> None:
    session.add(
        GamePlayByPlay(
            game_id=game_id,
            inning=inning,
            inning_half=half,
            play_description=description,
            event_type="unknown",
        ),
    )


def _inning(session, inning: int, *, runs: int = 0, side: str = "away", game_id: str = GAME) -> None:
    session.add(GameInningScore(game_id=game_id, inning=inning, team_side=side, runs=runs))


def _marker(session, inning: int = 9, *, game_id: str = GAME) -> None:
    _pbp(session, inning, description=FINAL_PBP_MARKER, half="bottom", game_id=game_id)


class TestWhatProvesAFinishedFeed:
    def test_the_closing_marker_proves_the_feed_finished(self, session) -> None:
        _pbp(session, 9)
        _marker(session)
        session.commit()

        assert pbp_complete_game_ids(session, [GAME]) == {GAME}

    def test_reaching_the_last_stated_inning_also_counts(self, session) -> None:
        """A feed without the marker is judged by the innings it reached."""
        for inning in range(1, 10):
            _pbp(session, inning)
            _inning(session, inning)
        session.commit()

        assert pbp_complete_game_ids(session, [GAME]) == {GAME}

    def test_a_game_with_no_play_by_play_is_incomplete(self, session) -> None:
        for inning in range(1, 10):
            _inning(session, inning)
        session.commit()

        assert pbp_complete_game_ids(session, [GAME]) == set()

    def test_a_game_with_no_stated_length_falls_back_to_row_existence(self, session) -> None:
        """Nothing says how long the game was, so a row is the only evidence."""
        _pbp(session, 1)
        session.commit()

        assert pbp_complete_game_ids(session, [GAME]) == {GAME}

    def test_an_empty_candidate_list_returns_nothing(self, session) -> None:
        assert pbp_complete_game_ids(session, []) == set()


class TestTheTwoTraps:
    def test_a_record_stopping_short_of_the_last_inning_is_incomplete(self, session) -> None:
        """The 2026 shape: eight innings stored, nine played, no closing marker."""
        for inning in range(1, 9):
            _pbp(session, inning)
        for inning in range(1, 10):
            _inning(session, inning)
        session.commit()

        assert pbp_complete_game_ids(session, [GAME]) == set()

    def test_trailing_zero_innings_do_not_make_a_marked_game_incomplete(self, session) -> None:
        """The 2025 shape: the feed closed at the 9th, the score table runs to 12.

        Reading the innings alone calls this game short and would send 736 healthy
        2025 games back for recrawling. The marker is what the feed itself says
        about having finished.
        """
        for inning in range(1, 10):
            _pbp(session, inning)
        _marker(session, 9)
        for inning in range(1, 13):
            _inning(session, inning)
        session.commit()

        assert pbp_complete_game_ids(session, [GAME]) == {GAME}

    def test_a_marked_game_is_complete_even_with_sparse_rows(self, session) -> None:
        """Row count is not the test: a marker at the last inning is the feed's word."""
        _pbp(session, 9, description="우익수 플라이 아웃")
        _marker(session, 9)
        for inning in range(1, 10):
            _inning(session, inning)
        session.commit()

        assert pbp_complete_game_ids(session, [GAME]) == {GAME}


class TestSelectionAcrossGames:
    def test_only_the_incomplete_games_are_left_out(self, session) -> None:
        """A complete game and a short one in the same batch, judged apart."""
        for inning in range(1, 10):
            _pbp(session, inning)
            _inning(session, inning)
        _marker(session)
        # The short game really holds eight innings of a nine-inning record --
        # this is 20260920HTNC0 as it was stored, not a game missing rows.
        for inning in range(1, 9):
            _pbp(session, inning, game_id=SHORT_GAME)
        for inning in range(1, 10):
            _inning(session, inning, game_id=SHORT_GAME)
        session.commit()

        complete = pbp_complete_game_ids(session, [GAME, SHORT_GAME])

        assert complete == {GAME}
