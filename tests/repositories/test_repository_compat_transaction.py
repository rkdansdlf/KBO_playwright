"""Compatibility-path transaction tests for session-optional repository APIs.

Repositories accept ``session=None`` and fall back to ``get_db_session()``,
which owns the transaction: it commits on success and rolls back on failure.
These tests verify that behavior against a real SQLite database instead of
mocking the session, so the "session=None auto commit / auto rollback" claim
is fixed as executable evidence.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.models.base import Base
from src.models.game import Game, GameIdAlias, GameMetadata
from src.repositories.game_save import save_schedule_game

SCHEDULE_PAYLOAD = {
    "game_id": "20241015LGSS0",
    "game_date": "2024-10-15",
    "away_team_code": "SS",
    "home_team_code": "LG",
    "season_year": 2024,
    "game_status": "scheduled",
    "game_time": "18:30",
    "stadium": "Jamsil",
}


@pytest.fixture
def test_sessionmaker():
    """Bind a sessionmaker to an in-memory SQLite DB shared across sessions."""
    engine = create_engine("sqlite://", poolclass=StaticPool)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _patch_db_session(test_sessionmaker):
    """Return a context manager that redirects get_db_session to the test DB."""
    return patch("src.db.engine.SessionLocal", test_sessionmaker)


def test_compat_api_without_session_commits_on_success(test_sessionmaker) -> None:
    """save_schedule_game(session=None) persists rows visible to a new session."""
    with _patch_db_session(test_sessionmaker):
        result = save_schedule_game(SCHEDULE_PAYLOAD)

    assert result is True
    with test_sessionmaker() as session:
        assert session.query(Game).filter_by(game_id="20241015SSLG0").count() == 1
        assert session.query(GameIdAlias).filter_by(alias_game_id="20241015LGSS0").count() == 1
        assert session.query(GameMetadata).filter_by(game_id="20241015SSLG0").count() == 1


def test_compat_api_without_session_rolls_back_on_failure(test_sessionmaker) -> None:
    """save_schedule_game(session=None) rolls back all writes when a step fails."""
    with (
        _patch_db_session(test_sessionmaker),
        patch(
            "src.repositories.game_save._upsert_metadata",
            side_effect=RuntimeError("injected failure"),
        ),
    ):
        with pytest.raises(RuntimeError, match="injected failure"):
            save_schedule_game(SCHEDULE_PAYLOAD)

    with test_sessionmaker() as session:
        assert session.query(Game).count() == 0
        assert session.query(GameIdAlias).count() == 0
        assert session.query(GameMetadata).count() == 0


def test_compat_api_without_session_sqlalchemy_error_returns_false(test_sessionmaker) -> None:
    """SQLAlchemyError inside the facade is caught and reported as False."""
    with (
        _patch_db_session(test_sessionmaker),
        patch(
            "src.repositories.game_save._upsert_metadata",
            side_effect=SQLAlchemyError("injected db failure"),
        ),
    ):
        result = save_schedule_game(SCHEDULE_PAYLOAD)

    assert result is False
    with test_sessionmaker() as session:
        assert session.query(Game).count() == 0
        assert session.query(GameIdAlias).count() == 0
        assert session.query(GameMetadata).count() == 0
