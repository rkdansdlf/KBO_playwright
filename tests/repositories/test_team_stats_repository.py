from __future__ import annotations

from unittest.mock import patch

from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching
from src.repositories.team_stats_repository import TeamSeasonBattingRepository, TeamSeasonPitchingRepository


class TestTeamSeasonBattingRepository:
    def _batting_session_fixture(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        engine = create_engine("sqlite:///:memory:")
        TeamSeasonBatting.__table__.create(engine)
        session = sessionmaker(bind=engine)()
        return session

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_creates_records(self, MockEngine, MockSessionLocal):
        session = self._batting_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        repo = TeamSeasonBattingRepository()
        result = repo.upsert_many([
            {"team_id": "LG", "team_name": "LG Twins", "season": 2024, "league": "REGULAR",
             "games": 144, "avg": 0.285},
        ])
        assert result == 1
        row = session.query(TeamSeasonBatting).one()
        assert row.team_id == "LG"
        assert row.games == 144

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_empty(self, MockEngine, MockSessionLocal):
        repo = TeamSeasonBattingRepository()
        assert repo.upsert_many([]) == 0

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_updates_existing(self, MockEngine, MockSessionLocal):
        session = self._batting_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        repo = TeamSeasonBattingRepository()
        repo.upsert_many([
            {"team_id": "LG", "team_name": "LG Twins", "season": 2024, "league": "REGULAR", "games": 144},
        ])
        repo.upsert_many([
            {"team_id": "LG", "team_name": "LG Twins", "season": 2024, "league": "REGULAR", "games": 145, "avg": 0.290},
        ])
        rows = session.query(TeamSeasonBatting).all()
        assert len(rows) == 1
        assert rows[0].games == 145
        assert rows[0].avg == 0.290

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_multiple_teams(self, MockEngine, MockSessionLocal):
        session = self._batting_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        repo = TeamSeasonBattingRepository()
        repo.upsert_many([
            {"team_id": "LG", "team_name": "LG", "season": 2024, "league": "REGULAR", "games": 144},
            {"team_id": "SSG", "team_name": "SSG", "season": 2024, "league": "REGULAR", "games": 144},
        ])
        assert session.query(TeamSeasonBatting).count() == 2


class TestTeamSeasonPitchingRepository:
    def _pitching_session_fixture(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        engine = create_engine("sqlite:///:memory:")
        TeamSeasonPitching.__table__.create(engine)
        session = sessionmaker(bind=engine)()
        return session

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_creates_records(self, MockEngine, MockSessionLocal):
        session = self._pitching_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        repo = TeamSeasonPitchingRepository()
        result = repo.upsert_many([
            {"team_id": "LG", "team_name": "LG Twins", "season": 2024, "league": "REGULAR",
             "era": 3.75, "wins": 80},
        ])
        assert result == 1
        row = session.query(TeamSeasonPitching).one()
        assert row.era == 3.75

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_empty(self, MockEngine, MockSessionLocal):
        repo = TeamSeasonPitchingRepository()
        assert repo.upsert_many([]) == 0

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_updates_existing(self, MockEngine, MockSessionLocal):
        session = self._pitching_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        repo = TeamSeasonPitchingRepository()
        repo.upsert_many([
            {"team_id": "LG", "team_name": "LG", "season": 2024, "league": "REGULAR", "era": 3.75},
        ])
        repo.upsert_many([
            {"team_id": "LG", "team_name": "LG", "season": 2024, "league": "REGULAR", "era": 3.50, "wins": 82},
        ])
        rows = session.query(TeamSeasonPitching).all()
        assert len(rows) == 1
        assert rows[0].era == 3.50
        assert rows[0].wins == 82
