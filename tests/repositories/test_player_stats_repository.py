from __future__ import annotations

from unittest.mock import patch

from src.models.player import PlayerSeasonBaserunning, PlayerSeasonFielding
from src.repositories.player_stats_repository import (
    PlayerSeasonBaserunningRepository,
    PlayerSeasonFieldingRepository,
)


class TestPlayerSeasonFieldingRepository:
    def _fielding_session_fixture(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        engine = create_engine("sqlite:///:memory:")
        PlayerSeasonFielding.__table__.create(engine)
        return sessionmaker(bind=engine)()

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_creates_records(self, MockEngine, MockSessionLocal):
        session = self._fielding_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        repo = PlayerSeasonFieldingRepository()
        result = repo.upsert_many(
            [
                {
                    "player_id": 1,
                    "team_id": "LG",
                    "year": 2024,
                    "position_id": "C",
                    "games": 100,
                    "games_started": 90,
                    "innings": 800.0,
                    "fielding_pct": 0.991,
                },
            ],
        )
        assert result == 1
        row = session.query(PlayerSeasonFielding).one()
        assert row.player_id == 1
        assert row.games == 100

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_empty_input(self, MockEngine, MockSessionLocal):
        repo = PlayerSeasonFieldingRepository()
        assert repo.upsert_many([]) == 0

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_duplicate_updates(self, MockEngine, MockSessionLocal):
        session = self._fielding_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        repo = PlayerSeasonFieldingRepository()
        repo.upsert_many(
            [
                {
                    "player_id": 1,
                    "team_id": "LG",
                    "year": 2024,
                    "position_id": "C",
                    "games": 100,
                    "fielding_pct": 0.991,
                },
            ],
        )
        result = repo.upsert_many(
            [
                {
                    "player_id": 1,
                    "team_id": "LG",
                    "year": 2024,
                    "position_id": "C",
                    "games": 101,
                    "fielding_pct": 0.992,
                },
            ],
        )
        assert result == 1
        rows = session.query(PlayerSeasonFielding).all()
        assert len(rows) == 1
        assert rows[0].games == 101


class TestPlayerSeasonBaserunningRepository:
    def _baserunning_session_fixture(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        engine = create_engine("sqlite:///:memory:")
        PlayerSeasonBaserunning.__table__.create(engine)
        return sessionmaker(bind=engine)()

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_creates_records(self, MockEngine, MockSessionLocal):
        session = self._baserunning_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        repo = PlayerSeasonBaserunningRepository()
        result = repo.upsert_many(
            [
                {"player_id": 1, "team_id": "LG", "year": 2024, "stolen_bases": 30, "caught_stealing": 5},
            ],
        )
        assert result == 1
        row = session.query(PlayerSeasonBaserunning).one()
        assert row.player_id == 1
        assert row.stolen_bases == 30

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_empty_input(self, MockEngine, MockSessionLocal):
        repo = PlayerSeasonBaserunningRepository()
        assert repo.upsert_many([]) == 0

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_duplicate_updates(self, MockEngine, MockSessionLocal):
        session = self._baserunning_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        repo = PlayerSeasonBaserunningRepository()
        repo.upsert_many(
            [
                {"player_id": 1, "team_id": "LG", "year": 2024, "stolen_bases": 30},
            ],
        )
        repo.upsert_many(
            [
                {"player_id": 1, "team_id": "LG", "year": 2024, "stolen_bases": 35},
            ],
        )
        rows = session.query(PlayerSeasonBaserunning).all()
        assert len(rows) == 1
        assert rows[0].stolen_bases == 35
