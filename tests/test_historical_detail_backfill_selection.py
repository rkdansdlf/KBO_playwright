"""What the backfill counts as missing, checked against a real database.

The existing tests in ``test_historical_detail_backfill.py`` mock the session, so
the query they exercise is never run -- they assert that a mock returned the row
it was told to return. That is the gap this file closes: the selection predicate
is the whole job of this method, and nothing else in the suite looks at it.

The predicate asks which terminal games lack box-score detail. Getting it wrong
is silent in a specific way: a game that is nearly complete still qualifies, the
crawler fetches it, and the run reports having done work it did not need to do.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.models.base import Base
from src.services.historical_detail_backfill_service import HistoricalDetailBackfillService

SEASON = 2009


def _schema(conn) -> None:
    """The four tables this query reads, with only the columns it names."""
    conn.execute(
        text(
            """
            CREATE TABLE kbo_seasons (
                season_id INTEGER PRIMARY KEY,
                season_year INTEGER,
                league_type_code TEXT,
                league_type_name TEXT
            )
            """,
        ),
    )
    conn.execute(
        text(
            """
            CREATE TABLE game (
                game_id TEXT PRIMARY KEY,
                game_date DATE,
                home_team TEXT,
                away_team TEXT,
                season_id INTEGER,
                game_status TEXT
            )
            """,
        ),
    )
    conn.execute(text("CREATE TABLE game_batting_stats (game_id TEXT)"))
    conn.execute(text("CREATE TABLE game_pitching_stats (game_id TEXT)"))


def _game(conn, game_id: str, *, batting: bool, pitching: bool) -> None:
    conn.execute(
        text(
            "INSERT INTO game (game_id, game_date, home_team, away_team, season_id, game_status)"
            " VALUES (:g, :d, 'HH', 'SK', :y, 'COMPLETED')"
        ),
        {"g": game_id, "d": f"{SEASON}-04-01", "y": SEASON},
    )
    if batting:
        conn.execute(text("INSERT INTO game_batting_stats (game_id) VALUES (:g)"), {"g": game_id})
    if pitching:
        conn.execute(text("INSERT INTO game_pitching_stats (game_id) VALUES (:g)"), {"g": game_id})


@pytest.fixture
def db() -> Session:
    """A real session, because the predicate is the thing under test."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        _schema(conn)
    Base.metadata.bind = engine
    session = Session(engine)
    yield session
    session.close()


def _service(session: Session) -> HistoricalDetailBackfillService:
    """The service without the resolver's own queries.

    ``PlayerIdResolver`` and the crawler are only used when a page is fetched;
    this tests the selection, so they are replaced rather than stood up.
    """
    service = HistoricalDetailBackfillService.__new__(HistoricalDetailBackfillService)
    service.session = session
    service.request_delay = 0.0
    return service


def test_a_game_missing_only_one_half_is_not_reported_as_missing(db: Session) -> None:
    """A half-complete game is not a missing one, and re-crawling it wastes the run.

    The predicate reads "lacks batting or lacks pitching", which is satisfied by a
    game that has one of the two. Such a game needs the other half, not a fresh
    crawl -- and since the missing half cannot be filled from what is already
    stored, the run would repeat the same work on every invocation and report a
    recovery that never completes.
    """
    _game(db.connection(), "20090404HHSK0", batting=True, pitching=False)
    _game(db.connection(), "20090405HHSK0", batting=False, pitching=True)
    db.connection().commit()

    found = _service(db).get_missing_games(SEASON)

    assert [row["game_id"] for row in found] == [], (
        "a game with one half of its box score was reported as missing the other"
    )


def test_a_game_with_no_halves_at_all_is_reported(db: Session) -> None:
    """The case the predicate is for, so narrowing it cannot quietly lose these."""
    _game(db.connection(), "20090404HHSK0", batting=False, pitching=False)
    db.connection().commit()

    found = _service(db).get_missing_games(SEASON)

    assert [row["game_id"] for row in found] == ["20090404HHSK0"]


def test_a_complete_game_is_not_reported(db: Session) -> None:
    _game(db.connection(), "20090404HHSK0", batting=True, pitching=True)
    db.connection().commit()

    assert _service(db).get_missing_games(SEASON) == []


def test_the_limit_is_spent_on_games_that_actually_need_crawling(db: Session) -> None:
    """A limit means "this many games' worth of work", not this many rows scanned.

    ``--limit`` is applied to the database query, so any game that sorts before
    the incomplete ones consumes part of the budget. Ordering is by date, so the
    work dropped is always the earliest in the season -- and a season whose
    opening games are already complete would crawl fewer than it was asked for
    while the summary reported the budget as spent.

    Two incomplete games are needed to make this observable: with only one, the
    limit has nothing to truncate and the test passes for the wrong reason.
    """
    for day in range(1, 4):
        _game(db.connection(), f"2009040{day}HHSK0", batting=False, pitching=False)
    # Complete games, sorted after the two incomplete ones by date.
    _game(db.connection(), "20090410HHSK0", batting=True, pitching=True)
    _game(db.connection(), "20090411HHSK0", batting=True, pitching=True)
    db.connection().commit()

    found = _service(db).get_missing_games(SEASON, limit=1)

    assert [row["game_id"] for row in found] == ["20090401HHSK0"], (
        "the one-game budget did not select the earliest game that needs crawling"
    )


def test_a_limit_larger_than_the_work_returns_all_of_it(db: Session) -> None:
    """The common case: a season with fewer gaps than the budget is not an error."""
    _game(db.connection(), "20090401HHSK0", batting=False, pitching=False)
    _game(db.connection(), "20090402HHSK0", batting=False, pitching=False)
    db.connection().commit()

    found = _service(db).get_missing_games(SEASON, limit=10)

    assert len(found) == 2


def test_a_preseason_game_is_left_alone(db: Session) -> None:
    """A game with no league is not one this crawler can fetch, so it is not work."""
    db.execute(
        text(
            "INSERT INTO game (game_id, game_date, home_team, away_team, season_id, game_status)"
            " VALUES ('20090401HHSK0', '2009-04-01', 'HH', 'SK', :y, 'COMPLETED')"
        ),
        {"y": SEASON},
    )
    db.execute(text("UPDATE kbo_seasons SET league_type_name = '시범경기' WHERE season_id = :y"), {"y": SEASON})
    db.execute(text("UPDATE game SET season_id = 0 WHERE game_id = '20090401HHSK0'"))
    db.execute(text("INSERT INTO kbo_seasons (season_id, season_year, league_type_name) VALUES (0, 0, '시범경기')"))
    db.execute(
        text(
            "INSERT INTO game (game_id, game_date, home_team, away_team, season_id, game_status)"
            " VALUES ('20090402HHSK0', '2009-04-02', 'HH', 'SK', :y, 'COMPLETED')"
        ),
        {"y": SEASON},
    )
    db.execute(text("UPDATE kbo_seasons SET league_type_name = 'KBO1' WHERE season_id = :y"), {"y": SEASON})
    db.connection().commit()

    found = _service(db).get_missing_games(SEASON)

    assert [row["game_id"] for row in found] == ["20090402HHSK0"], "a preseason game was queued for crawling"


def test_a_non_terminal_game_is_left_alone(db: Session) -> None:
    """A scheduled game has no box score yet, and its absence is not a gap."""
    db.execute(
        text("INSERT INTO kbo_seasons (season_id, season_year, league_type_name) VALUES (:y, :y, 'KBO1')"),
        {"y": SEASON},
    )
    db.execute(
        text(
            "INSERT INTO game (game_id, game_date, home_team, away_team, season_id, game_status)"
            " VALUES ('20090401HHSK0', '2009-04-01', 'HH', 'SK', :y, 'SCHEDULED')"
        ),
        {"y": SEASON},
    )
    db.connection().commit()

    assert _service(db).get_missing_games(SEASON) == []


def test_the_returned_date_is_the_crawlers_key_format(db: Session) -> None:
    """The service hands the date to a URL builder that wants YYYYMMDD.

    A hyphenated date silently produces a review URL that matches nothing, so the
    crawl returns no data and the game is recorded as a failure rather than as a
    request that was never correctly addressed.
    """
    db.execute(
        text("INSERT INTO kbo_seasons (season_id, season_year, league_type_name) VALUES (:y, :y, 'KBO1')"),
        {"y": SEASON},
    )
    db.execute(
        text(
            "INSERT INTO game (game_id, game_date, home_team, away_team, season_id, game_status)"
            " VALUES ('20090401HHSK0', :d, 'HH', 'SK', :y, 'COMPLETED')"
        ),
        {"d": date(2009, 4, 1), "y": SEASON},
    )
    db.connection().commit()

    found = _service(db).get_missing_games(SEASON)

    assert found[0]["game_date"] == "20090401", "the date handed to the crawler is not the format its URL needs"
