from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.engine import Engine
from src.repositories.player_season_batting_repository import PlayerSeasonBattingRepository


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
    with patch(
        "src.repositories.player_season_batting_repository.SessionLocal",
        return_value=session,
    ):
        yield


@pytest.fixture(autouse=True)
def patch_dialect():
    with patch.object(Engine.dialect, "name", "sqlite"):
        yield


class TestPlayerSeasonBattingRepository:
    def test_upsert_batting_stats(self, session):
        repo = PlayerSeasonBattingRepository()
        stats = [
            {
                "player_id": 1,
                "season": 2025,
                "league": "REGULAR",
                "level": "KBO1",
                "team_code": "LG",
                "games": 100,
                "hits": 50,
            }
        ]
        count = repo.upsert_batting_stats(stats)
        assert count == 1

    def test_upsert_empty(self, session):
        repo = PlayerSeasonBattingRepository()
        assert repo.upsert_batting_stats([]) == 0

    def test_upsert_updates_existing(self, session):
        repo = PlayerSeasonBattingRepository()
        repo.upsert_batting_stats([{"player_id": 1, "season": 2025, "team_code": "LG", "games": 10}])
        repo.upsert_batting_stats([{"player_id": 1, "season": 2025, "team_code": "LG", "games": 99}])
        record = repo.get_by_player_season(1, 2025)
        assert record is not None
        assert record.games == 99

    def test_get_by_player_season_not_found(self, session):
        repo = PlayerSeasonBattingRepository()
        assert repo.get_by_player_season(999, 2025) is None

    def test_get_by_player(self, session):
        repo = PlayerSeasonBattingRepository()
        repo.upsert_batting_stats(
            [
                {"player_id": 1, "season": 2025, "team_code": "LG"},
                {"player_id": 1, "season": 2024, "team_code": "LG"},
            ]
        )
        results = repo.get_by_player(1)
        assert len(results) == 2

    def test_get_by_season(self, session):
        repo = PlayerSeasonBattingRepository()
        repo.upsert_batting_stats(
            [
                {"player_id": 1, "season": 2025, "team_code": "LG"},
                {"player_id": 2, "season": 2025, "team_code": "SSG"},
                {"player_id": 3, "season": 2024, "team_code": "LG"},
            ]
        )
        results = repo.get_by_season(2025)
        assert len(results) == 2

    def test_get_by_team_season(self, session):
        repo = PlayerSeasonBattingRepository()
        repo.upsert_batting_stats(
            [
                {"player_id": 1, "season": 2025, "team_code": "LG"},
                {"player_id": 2, "season": 2025, "team_code": "SSG"},
            ]
        )
        results = repo.get_by_team_season("LG", 2025)
        assert len(results) == 1
        assert results[0].player_id == 1

    def test_count(self, session):
        repo = PlayerSeasonBattingRepository()
        repo.upsert_batting_stats(
            [
                {"player_id": 1, "season": 2025, "team_code": "LG"},
                {"player_id": 2, "season": 2025, "team_code": "SSG"},
            ]
        )
        assert repo.count() == 2
        assert repo.count(season=2025) == 2
        assert repo.count(season=2024) == 0

    def test_delete_by_player_season(self, session):
        repo = PlayerSeasonBattingRepository()
        repo.upsert_batting_stats([{"player_id": 1, "season": 2025, "team_code": "LG"}])
        assert repo.delete_by_player_season(1, 2025) is True
        assert repo.get_by_player_season(1, 2025) is None

    def test_delete_by_player_season_not_found(self, session):
        repo = PlayerSeasonBattingRepository()
        assert repo.delete_by_player_season(999, 2025) is False
