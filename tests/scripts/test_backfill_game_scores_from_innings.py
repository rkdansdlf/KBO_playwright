"""The backfill writes arithmetic, and only where the arithmetic can be checked.

These run the script's function against real ORM tables so the write path is the
same one the CLI uses. The rules being pinned are the ones that decide whether a
number is derivable at all: both sides present, a line that looks like a whole
game, a blank to write into, and no second pass doing anything.

365 stored games are in the state this repairs, so the failure mode of getting a
rule wrong is 365 wrong totals rather than one.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from src.models.game import Game, GameInningScore
from scripts.maintenance.backfill_game_scores_from_innings import backfill_game_scores

#: A nine-inning game: away 2+2+2+1+1+1+1 = 10, home 1x6 = 6.
AWAY_LINE = [2, 2, 2, 1, 1, 1, 1, 0, 0]
HOME_LINE = [1, 1, 1, 1, 1, 1, 0, 0, 0]

#: A game called after seven innings -- or a crawl that lost the last two.
SHORT_LINE = [1, 1, 1, 1, 0, 0, 0]


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(engine)
    GameInningScore.__table__.create(engine)
    db = sessionmaker(bind=engine)()
    yield db
    db.close()


def _game(
    session,
    game_id: str,
    *,
    home: int | None = None,
    away: int | None = None,
    status: str = "COMPLETED",
) -> None:
    session.add(
        Game(
            game_id=game_id,
            game_date=date(2023, 4, 4),
            home_team="SSG",
            away_team="LT",
            home_score=home,
            away_score=away,
            game_status=status,
            is_primary=True,
        ),
    )
    session.commit()


def _innings(session, game_id: str, *, home: list[int], away: list[int]) -> None:
    for side, runs in (("home", home), ("away", away)):
        for number, value in enumerate(runs, start=1):
            session.add(GameInningScore(game_id=game_id, team_side=side, inning=number, runs=value))
    session.commit()


def _scores(session, game_id: str) -> tuple[int | None, int | None]:
    """Read the stored totals back by ``game_id``.

    ``session.get`` would need the surrogate primary key; ``game_id`` is only
    unique, so the lookup has to be a query.
    """
    row = session.execute(
        select(Game.home_score, Game.away_score).where(Game.game_id == game_id),
    ).one()
    return row.home_score, row.away_score


class TestItFillsWhatTheInningsDetermine:
    def test_both_blank_sides_are_filled_from_the_innings(self, session) -> None:
        _game(session, "20230404LTSK0")
        _innings(session, "20230404LTSK0", home=HOME_LINE, away=AWAY_LINE)

        report = backfill_game_scores(apply=True, session=session)

        assert report.filled == 1
        assert _scores(session, "20230404LTSK0") == (6, 10)

    def test_only_the_blank_side_is_filled(self, session) -> None:
        """A stated total is the source's own statement, not a value to correct."""
        _game(session, "20230404LTSK0", home=6)
        _innings(session, "20230404LTSK0", home=HOME_LINE, away=AWAY_LINE)

        backfill_game_scores(apply=True, session=session)

        assert _scores(session, "20230404LTSK0") == (6, 10)

    def test_a_stated_total_is_not_recomputed(self, session) -> None:
        """The stated side is left as it is, even when the innings disagree with it.

        The disagreement is the only trace that the two parts of the source page
        did not match, so replacing the number with a sum would destroy the
        evidence and record a value nobody read.
        """
        _game(session, "20230404LTSK0", home=99)
        _innings(session, "20230404LTSK0", home=HOME_LINE, away=AWAY_LINE)

        report = backfill_game_scores(apply=True, session=session)

        assert _scores(session, "20230404LTSK0") == (99, 10), "a stated total was replaced by a sum"
        assert report.filled == 1

    def test_innings_without_numeric_runs_are_skipped(self, session) -> None:
        """The right length is not enough; there has to be a number in it.

        The rows are inserted directly because the model gives ``runs`` a default
        of zero, so writing ``None`` through the ORM stores 0 and the state cannot
        be reached that way. A raw writer can still leave it null, and summing
        nulls must not become a 0-0 result.
        """
        _game(session, "20230404LTSK0")
        for side in ("home", "away"):
            for inning in range(1, 10):
                session.execute(
                    text(
                        "INSERT INTO game_inning_scores (game_id, team_side, inning, runs) VALUES (:g, :s, :i, NULL)",
                    ),
                    {"g": "20230404LTSK0", "s": side, "i": inning},
                )
        session.commit()

        report = backfill_game_scores(apply=True, session=session)

        assert report.filled == 0
        assert report.skipped_no_runs == ["20230404LTSK0"]
        assert _scores(session, "20230404LTSK0") == (None, None)

    def test_a_home_line_one_shorter_than_away_counts_as_whole(self, session) -> None:
        """The home side does not bat the bottom ninth when it already leads."""
        _game(session, "20230404LTSK0")
        _innings(session, "20230404LTSK0", home=[2, 2, 2, 2, 1, 1, 0, 0], away=[1, 1, 1, 1, 1, 0, 0, 0, 0])

        report = backfill_game_scores(apply=True, session=session)

        assert report.filled == 1
        assert _scores(session, "20230404LTSK0") == (10, 5)


class TestItRefusesWhatItCannotCheck:
    def test_a_short_line_is_skipped_and_named(self, session) -> None:
        """A prefix of innings is a called game and a truncated crawl at once.

        The two cannot be told apart from the rows, so the game is reported
        instead of filled -- 20230404LTSK0 is the one stored game in this shape.
        """
        _game(session, "20230404LTSK0")
        _innings(session, "20230404LTSK0", home=SHORT_LINE, away=SHORT_LINE)

        report = backfill_game_scores(apply=True, session=session)

        assert report.filled == 0
        assert report.skipped_incomplete_line == ["20230404LTSK0"]
        assert _scores(session, "20230404LTSK0") == (None, None)

    def test_a_game_with_one_sided_innings_counts_as_not_crawled(self, session) -> None:
        """Summing one side would leave the other looking shut out.

        It is grouped with the un-crawled games rather than with a short line,
        because one side is nothing to sum rather than half of something -- and
        five thousand stored games look like this.
        """
        _game(session, "20230404LTSK0")
        _innings(session, "20230404LTSK0", home=HOME_LINE, away=[])

        report = backfill_game_scores(apply=True, session=session)

        assert report.filled == 0
        assert report.skipped_not_crawled == ["20230404LTSK0"]
        assert report.skipped_incomplete_line == []

    def test_a_game_with_no_innings_counts_as_not_crawled(self, session) -> None:
        _game(session, "20230404LTSK0")

        report = backfill_game_scores(apply=True, session=session)

        assert report.filled == 0
        assert report.skipped_not_crawled == ["20230404LTSK0"]
        assert report.skipped_incomplete_line == []

    def test_a_cancelled_game_is_not_a_candidate(self, session) -> None:
        """It legitimately has no total, so it is not scanned at all."""
        _game(session, "20230404LTSK0", status="CANCELLED")
        _innings(session, "20230404LTSK0", home=HOME_LINE, away=AWAY_LINE)

        report = backfill_game_scores(apply=True, session=session)

        assert report.scanned == 0
        assert report.filled == 0


class TestItIsSafeToRunTwice:
    def test_a_dry_run_writes_nothing(self, session) -> None:
        _game(session, "20230404LTSK0")
        _innings(session, "20230404LTSK0", home=HOME_LINE, away=AWAY_LINE)

        report = backfill_game_scores(apply=False, session=session)

        assert report.filled == 1, "the run did not report what it would have done"
        assert _scores(session, "20230404LTSK0") == (None, None), "a dry run wrote to the database"

    def test_the_second_apply_finds_nothing_left_to_do(self, session) -> None:
        _game(session, "20230404LTSK0")
        _innings(session, "20230404LTSK0", home=HOME_LINE, away=AWAY_LINE)

        first = backfill_game_scores(apply=True, session=session)
        second = backfill_game_scores(apply=True, session=session)

        assert first.filled == 1
        assert second.scanned == 0, "a filled game still looked like it needed filling"
        assert second.filled == 0


class TestItCanBeScopedToOneSeason:
    def test_a_year_filter_leaves_other_seasons_alone(self, session) -> None:
        _game(session, "20230404LTSK0")
        _innings(session, "20230404LTSK0", home=HOME_LINE, away=AWAY_LINE)

        report = backfill_game_scores(apply=True, year=2022, session=session)

        assert report.scanned == 0
        assert _scores(session, "20230404LTSK0") == (None, None)
