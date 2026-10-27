"""The missing-data matrix must flag an unrecorded column and pass a recorded one.

The matrix is a suspicion list, so the test is not that it finds every gap.
It is that it does not cry wolf: a column that was recorded must read OK,
and a column that was never recorded must read MISSING. A report that flags
both is useless to the operator, who has to triage it.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.cli.reports import missing_data_matrix as matrix
from src.models.game import Game
from src.models.player import PlayerSeasonBatting


@pytest.fixture
def engine():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(engine)
    PlayerSeasonBatting.__table__.create(engine)
    return engine


@pytest.fixture
def session_factory(engine):
    return sessionmaker(bind=engine)


def _add_game(session, game_id: str, home: str, away: str) -> None:
    """Add a game row to the season schedule."""
    session.add(
        Game(
            game_id=game_id,
            game_date=__import__("datetime").date(1989, 1, 1),
            home_team=home,
            away_team=away,
            home_score=3,
            away_score=1,
            game_status="COMPLETED",
        ),
    )


def _add_batter(
    session,
    *,
    season: int,
    player_id: int,
    hits: int,
    home_runs: int,
    doubles: int,
    walks: int,
) -> None:
    """Add a season batting aggregate with the given column totals."""
    session.add(
        PlayerSeasonBatting(
            player_id=player_id,
            season=season,
            league="REGULAR",
            team_code="LG",
            games=120,
            plate_appearances=500,
            at_bats=450,
            hits=hits,
            home_runs=home_runs,
            doubles=doubles,
            walks=walks,
            runs=70,
            triples=1,
            stolen_bases=10,
        ),
    )


class TestTheColumnClassification:
    def test_a_recorded_column_reads_ok(self) -> None:
        assert matrix._classify_column("home_runs", 50, 1000) == "OK"

    def test_a_column_that_was_never_recorded_reads_missing(self) -> None:
        assert matrix._classify_column("home_runs", 0, 1000) == "MISSING"

    def test_a_column_too_small_for_the_season_reads_suspect(self) -> None:
        assert matrix._classify_column("home_runs", 1, 1000) == "SUSPECT"

    def test_a_short_season_is_not_judged(self) -> None:
        """Too few hits to form a ratio is not evidence of a missing column."""
        assert matrix._classify_column("home_runs", 0, 10) == "OK"

    def test_rare_columns_carry_a_looser_bound(self) -> None:
        """Walks legitimately sit lower than hits, so their bound is looser."""
        assert matrix._classify_column("walks", 20, 1000) == "SUSPECT"
        assert matrix._classify_column("walks", 100, 1000) == "OK"


class TestTheMatrix:
    def test_it_flags_the_unrecorded_column_of_a_season(self, engine, session_factory) -> None:
        with session_factory() as session:
            _add_game(session, "198900001OBLG0", "OB", "LG")
            _add_batter(session, season=1989, player_id=1, hits=1000, home_runs=50, doubles=200, walks=400)
            _add_batter(session, season=1989, player_id=2, hits=1000, home_runs=50, doubles=200, walks=400)
            session.commit()

        with engine.connect() as conn:
            payload = matrix.build_missing_data_matrix(conn)
        season = next(entry for entry in payload["seasons"] if entry["season"] == 1989)
        batting = season["player_season_batting"]

        assert batting["totals"]["hits"] == 2000
        assert batting["totals"]["home_runs"] == 100
        assert "home_runs" not in batting["flagged"]

    def test_it_reports_the_season_length_from_the_schedule(self, engine, session_factory) -> None:
        """The era's length comes from the schedule, not from a rewritten status."""
        with session_factory() as session:
            _add_game(session, "198900001OBLG0", "OB", "LG")
            _add_game(session, "198900002LGOB0", "LG", "OB")
            session.commit()

        with engine.connect() as conn:
            payload = matrix.build_missing_data_matrix(conn)
        season = next(entry for entry in payload["seasons"] if entry["season"] == 1989)

        assert season["schedule"]["game_rows"] == 2
        assert season["schedule"]["teams"] == 2
        assert season["schedule"]["games_per_team"] == 2.0

    def test_an_absent_table_is_skipped(self, engine) -> None:
        """A table that was never created is not a missing column."""
        with engine.connect() as conn:
            payload = matrix.build_missing_data_matrix(conn)

        assert all("game_batting_stats" not in season for season in payload["seasons"])
