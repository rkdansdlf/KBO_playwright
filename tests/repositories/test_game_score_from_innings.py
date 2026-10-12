"""Inning scores that cannot fill the score are a score that does not get read.

A detail crawl writes three things: the inning lines, the box score, and the two
team scores. They come from different parts of the same page, and a partial
render can produce the first two without the third.

``game_save`` derives the game status from the scores, falling back to "there is
progress evidence" when they are missing. That fallback is what let these rows be
written: a game with 18 inning lines but no scores is unmistakably played, so
``COMPLETED`` is correct -- and then nothing ever fills the score back in. The
evidence is sitting in the next table, one sum away.

369 stored games are in that state, every one of them with both sides' innings
present, so the sum is complete rather than a guess.
"""

from __future__ import annotations

import contextlib
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.models.game import (
    Game,
    GameBattingStat,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GameSummary,
)
from src.models.player import PlayerBasic, PlayerSeasonBatting
from src.models.season import KboSeason
from src.models.quarantine import QuarantinedRecord
from src.models.team import Team
from src.repositories.game_save import save_game_detail

GAME_ID = "20241015LGSS0"

#: The innings as the page reported them: home 3+1+0+2, away 5+2+1+2.
HOME_LINE = [3, 1, 0, 2]
AWAY_LINE = [5, 2, 1, 2]


@pytest.fixture
def session():
    """Real tables, because the score is written through the ORM.

    A hand-built CREATE TABLE cannot answer for this: the save path selects the
    whole ``game`` row, so a missing column fails before the behaviour under
    test is reached.
    """
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game,
        GameIdAlias,
        GameInningScore,
        GameLineup,
        GameMetadata,
        GameSummary,
        GameBattingStat,
        GamePitchingStat,
        PlayerBasic,
        PlayerSeasonBatting,
        KboSeason,
        Team,
        QuarantinedRecord,
    ):
        table.__table__.create(engine)

    db = sessionmaker(bind=engine)()
    db.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
    db.commit()

    @contextlib.contextmanager
    def _scoped():
        yield db

    with patch("src.repositories.game_save.get_db_session", side_effect=_scoped):
        yield db
    db.close()


def _payload(
    *,
    home_score: int | None,
    away_score: int | None,
    home_line: list[int] | None = None,
    away_line: list[int] | None = None,
) -> dict:
    """A detail payload whose innings may be present and whose scores may not be."""
    return {
        "game_id": GAME_ID,
        "game_date": "2024-10-15",
        "game_status": "completed",
        "teams": {
            "away": {"code": "SS", "score": away_score, "line_score": AWAY_LINE if away_line is None else away_line},
            "home": {"code": "LG", "score": home_score, "line_score": HOME_LINE if home_line is None else home_line},
        },
        "metadata": {"stadium": "Jamsil"},
        "hitters": {"away": [], "home": []},
        "pitchers": {"away": [], "home": []},
    }


def _score(session) -> tuple[int | None, int | None]:
    session.flush()
    rows = session.execute(
        text("SELECT game_id, home_score, away_score FROM game"),
    ).all()
    assert len(rows) == 1, f"expected exactly one game row, got {rows}"
    return rows[0].home_score, rows[0].away_score


class TestAScoreIsTakenFromTheInningsWhenItIsMissing:
    def test_a_game_with_innings_and_no_scores_gets_its_scores(self, session) -> None:
        """The 369 rows: the total is already written, once per inning.

        Nothing here is inferred. Each side's innings are read and summed, which
        is arithmetic on stored data rather than a judgement about what a missing
        value probably was.
        """
        assert save_game_detail(_payload(home_score=None, away_score=None), session=session)

        assert _score(session) == (6, 10), "the innings were not summed into the score"

    def test_a_half_scored_game_is_completed_from_the_other_side(self, session) -> None:
        """One side missing is not a reason to leave both blank.

        The half that is present is trusted as stored -- it came from the page --
        and only the absent side is reconstructed, so a page that reported one
        number and not the other does not get both replaced by a sum.
        """
        assert save_game_detail(_payload(home_score=4, away_score=None), session=session)

        home, away = _score(session)
        assert home == 4, "a score that was present was replaced by a computed one"
        assert away == 10, "the missing side was not filled from the innings"

    def test_a_score_the_page_reported_is_not_overwritten_by_the_sum(self, session) -> None:
        """A disagreement means the source and the innings differ, and the source wins.

        Overwriting would hide the disagreement, which is the only trace of it.
        """
        assert save_game_detail(_payload(home_score=9, away_score=8), session=session)

        assert _score(session) == (9, 8), "a reported score was replaced by a sum"


class TestNothingIsInvented:
    def test_a_game_with_neither_scores_nor_innings_is_left_alone(self, session) -> None:
        """No evidence, no number -- and no row either.

        The quality gate rejects this payload before the save runs, so the
        assertion is that nothing was written rather than that two nulls were.
        Zero is a score a team can actually post, so writing it here would
        fabricate a result for a crawl that read nothing.
        """
        save_game_detail(_payload(home_score=None, away_score=None, home_line=[], away_line=[]), session=session)
        session.flush()

        assert session.execute(text("SELECT count(*) FROM game")).scalar() == 0, (
            "a game with no scores and no innings was stored"
        )

    def test_one_sided_innings_do_not_fill_the_other_side(self, session) -> None:
        """Half an inning line is half a game, and 0-0 is a real result.

        Summing the side that is present would turn an incomplete read into a
        claimed shutout for the other.
        """
        save_game_detail(
            _payload(home_score=None, away_score=None, home_line=HOME_LINE, away_line=[]),
            session=session,
        )

        assert _score(session) == (None, None), "one side of the innings was used to score the other"

    def test_a_zero_in_the_innings_is_a_real_score_not_a_missing_one(self, session) -> None:
        """A shutout is the case a falsy check gets wrong."""
        save_game_detail(
            _payload(home_score=None, away_score=None, home_line=[0, 0, 0, 0], away_line=[0, 0, 0, 1]),
            session=session,
        )

        assert _score(session) == (0, 1), "a shutout was treated as a missing score"


class TestTheSumItself:
    """The helper, because every rule above depends on what it returns for nothing."""

    def test_a_side_with_no_rows_is_none_rather_than_zero(self) -> None:
        """Zero is what a team scores; it is not what "nothing was read" means.

        Returning 0 here is indistinguishable from a shutout once it reaches the
        column, and nothing downstream can tell those apart.
        """
        from src.repositories.game_save import _score_from_innings

        assert _score_from_innings([{"team_side": "home", "runs": 3}], "away") is None

    def test_an_empty_line_sum_is_zero(self) -> None:
        """The other half of the same distinction: present and empty is a real 0."""
        from src.repositories.game_save import _score_from_innings

        assert _score_from_innings([{"team_side": "home", "runs": 0}], "home") == 0

    def test_a_non_numeric_run_is_skipped_rather_than_coerced(self) -> None:
        """The page uses "-" for an inning that was not played."""
        from src.repositories.game_save import _score_from_innings

        rows = [{"team_side": "home", "runs": 3}, {"team_side": "home", "runs": "-"}, {"team_side": "home", "runs": 1}]

        assert _score_from_innings(rows, "home") == 4
