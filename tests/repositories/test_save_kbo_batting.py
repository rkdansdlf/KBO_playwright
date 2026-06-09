from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.engine import Engine
from src.repositories.save_kbo_batting import save_kbo_batting_batch, save_kbo_player_season_batting


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    from src.models.base import Base

    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    return Session()


@pytest.fixture(autouse=True)
def patch_session_local(session):
    with patch("src.repositories.save_kbo_batting.SessionLocal", return_value=session):
        yield


@pytest.fixture(autouse=True)
def patch_dialect():
    with patch.object(Engine.dialect, "name", "sqlite"):
        yield


class TestSaveKboBatting:
    def test_save_player_season_batting(self, session):
        data = {
            "player_id": 12345,
            "year": 2025,
            "league": "KBO",
            "team_code": "LG",
            "games": 100,
            "at_bats": 300,
            "hits": 100,
        }
        assert save_kbo_player_season_batting(data) is True

    def test_save_missing_required_field_returns_false(self, session):
        data = {"player_id": 12345, "year": 2025}
        assert save_kbo_player_season_batting(data) is False

    def test_save_batting_batch(self, session):
        players_data = {
            1: {"player_id": 1, "year": 2025, "league": "KBO", "team_code": "LG", "games": 100},
            2: {"player_id": 2, "year": 2025, "league": "KBO", "team_code": "SSG", "games": 90},
        }
        assert save_kbo_batting_batch(players_data, "test-series") == 2

    def test_upsert_updates_existing(self, session):
        data = {"player_id": 1, "year": 2025, "league": "KBO", "team_code": "LG", "games": 100, "hits": 50}
        save_kbo_player_season_batting(data)
        data["hits"] = 99
        save_kbo_player_season_batting(data)
        from src.models.player import PlayerSeasonBatting
        record = session.query(PlayerSeasonBatting).filter_by(player_id=1, season=2025).first()
        assert record.hits == 99
