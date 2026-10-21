"""The DB fallback for today's games must compare ``game_date`` against a real date.

psycopg 3 sends a ``str`` parameter as VARCHAR, and PostgreSQL has no
``date = character varying`` operator, so ``Game.game_date == "2026-10-21"`` raised
``UndefinedFunction`` and the fallback silently returned no games. psycopg 2 had
interpolated the literal client side, which hid the problem until the scheduler moved
to psycopg 3. SQLite rejects a string for a ``Date`` column as well, so an in-memory
database reproduces the failure without PostgreSQL.
"""

from __future__ import annotations

from datetime import date

import pytest
import src.cli.live.live_crawler as live_crawler
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from src.models.base import Base
from src.models.game import Game


@pytest.fixture
def session_factory(monkeypatch: pytest.MonkeyPatch) -> sessionmaker[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(live_crawler, "SessionLocal", factory)
    return factory


def _add_games(factory: sessionmaker[Session]) -> None:
    with factory() as session:
        session.add_all(
            [
                Game(game_id="20261021LGKT0", game_date=date(2026, 10, 21), away_team="LG", home_team="KT"),
                Game(game_id="20261022SSNC0", game_date=date(2026, 10, 22), away_team="SS", home_team="NC"),
            ],
        )
        session.commit()


@pytest.mark.parametrize("today", ["20261021", "2026-10-21"])
def test_returns_only_games_scheduled_for_the_requested_day(
    session_factory: sessionmaker[Session],
    today: str,
) -> None:
    _add_games(session_factory)

    games = live_crawler._load_today_games_from_db(today)

    assert [g["game_id"] for g in games] == ["20261021LGKT0"]
    assert games[0]["away_team_code"] == "LG"
    assert games[0]["home_team_code"] == "KT"
    assert games[0]["game_date"] == "2026-10-21"


def test_returns_nothing_when_no_game_is_scheduled(session_factory: sessionmaker[Session]) -> None:
    _add_games(session_factory)

    assert live_crawler._load_today_games_from_db("20261101") == []


@pytest.mark.parametrize("today", ["20261021", "2026-10-21"])
def test_game_date_is_bound_as_a_date_for_psycopg(monkeypatch: pytest.MonkeyPatch, today: str) -> None:
    """Under psycopg a str binds as ``::VARCHAR`` (rejected); a date binds as ``::DATE``."""
    from sqlalchemy.dialects.postgresql import psycopg

    criteria: list[object] = []

    class _Query:
        def filter(self, criterion: object) -> _Query:
            criteria.append(criterion)
            return self

        def all(self) -> list[object]:
            return []

    class _Session:
        def __enter__(self) -> _Session:
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def query(self, _model: object) -> _Query:
            return _Query()

    monkeypatch.setattr(live_crawler, "SessionLocal", _Session)

    assert live_crawler._load_today_games_from_db(today) == []

    assert len(criteria) == 1
    sql = str(criteria[0].compile(dialect=psycopg.dialect()))  # type: ignore[attr-defined]
    assert "::DATE" in sql
    assert "::VARCHAR" not in sql
