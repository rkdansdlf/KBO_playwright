"""E2E atomicity tests for service-level multi-table transactions.

Exercises the real production path:

    schedule payload
        -> schedule_collection_service.save_schedule_games
            -> save_schedule_game (session=None facade)
                -> Game + GameIdAlias + GameMetadata in one transaction

Verifies that a single service call persists every table atomically and that
a mid-write failure rolls back all of them.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.models.base import Base
from src.models.game import Game, GameIdAlias, GameMetadata
from src.services.schedule_collection_service import ScheduleSaveResult, save_schedule_games

pytestmark = pytest.mark.integration


def _payload(game_id: str, game_date: str, away: str, home: str) -> dict:
    return {
        "game_id": game_id,
        "game_date": game_date,
        "away_team_code": away,
        "home_team_code": home,
        "season_year": int(game_date[:4]),
        "game_status": "scheduled",
        "game_time": "18:30",
        "stadium": "Jamsil",
    }


@pytest.fixture
def test_sessionmaker():
    """Bind a sessionmaker to an in-memory SQLite DB shared across sessions.

    Set KBO_E2E_DATABASE_URL to exercise the same atomicity contract against a
    real server (e.g. PostgreSQL) instead of the default SQLite in-memory engine.

    """
    url = os.getenv("KBO_E2E_DATABASE_URL")
    if url:
        engine = create_engine(url, pool_pre_ping=True)
    else:
        engine = create_engine("sqlite://", poolclass=StaticPool)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def test_collection_service_commits_multiple_tables_atomically(test_sessionmaker) -> None:
    """save_schedule_games persists Game/GameIdAlias/GameMetadata for every game."""
    games = [
        _payload("20241015LGSS0", "2024-10-15", "SS", "LG"),
        _payload("20241016KTHT0", "2024-10-16", "HT", "KT"),
    ]

    with patch("src.db.engine.SessionLocal", test_sessionmaker):
        result = save_schedule_games(games)

    assert isinstance(result, ScheduleSaveResult)
    assert result.saved == 2
    assert result.failed == 0
    with test_sessionmaker() as session:
        assert session.query(Game).count() == 2
        assert session.query(GameIdAlias).count() == 2
        assert session.query(GameMetadata).count() == 2


def test_collection_service_rolls_back_all_tables_on_failure(test_sessionmaker) -> None:
    """A mid-write failure rolls back every table written so far."""
    games = [
        _payload("20241015LGSS0", "2024-10-15", "SS", "LG"),
        _payload("20241016KTHT0", "2024-10-16", "HT", "KT"),
    ]

    with (
        patch("src.db.engine.SessionLocal", test_sessionmaker),
        patch(
            "src.repositories.game_save._upsert_metadata",
            side_effect=SQLAlchemyError("injected db failure"),
        ),
    ):
        result = save_schedule_games(games)

    assert isinstance(result, ScheduleSaveResult)
    assert result.saved == 0
    assert result.failed == 2
    with test_sessionmaker() as session:
        assert session.query(Game).count() == 0
        assert session.query(GameIdAlias).count() == 0
        assert session.query(GameMetadata).count() == 0


def test_collection_service_idempotent_after_commit(test_sessionmaker) -> None:
    """Re-running the same service call keeps row counts stable (UPSERT)."""
    games = [_payload("20241015LGSS0", "2024-10-15", "SS", "LG")]

    with patch("src.db.engine.SessionLocal", test_sessionmaker):
        first = save_schedule_games(games)
        second = save_schedule_games(games)

    assert first.saved == 1
    assert second.saved == 1
    with test_sessionmaker() as session:
        assert session.query(Game).count() == 1
        assert session.query(GameIdAlias).count() == 1
        assert session.query(GameMetadata).count() == 1
