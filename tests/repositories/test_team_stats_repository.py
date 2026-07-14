from __future__ import annotations

from unittest.mock import MagicMock, patch
import pytest

from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching
from src.repositories.team_stats_repository import TeamSeasonBattingRepository, TeamSeasonPitchingRepository


class TestTeamSeasonBattingRepository:
    def _batting_session_fixture(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        engine = create_engine("sqlite:///:memory:")
        TeamSeasonBatting.__table__.create(engine)
        return sessionmaker(bind=engine)()

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_creates_records(self, MockEngine, MockSessionLocal):
        session = self._batting_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        repo = TeamSeasonBattingRepository()
        result = repo.upsert_many(
            [
                {
                    "team_id": "LG",
                    "team_name": "LG Twins",
                    "season": 2024,
                    "league": "REGULAR",
                    "games": 144,
                    "avg": 0.285,
                },
            ],
        )
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
        repo.upsert_many(
            [
                {"team_id": "LG", "team_name": "LG Twins", "season": 2024, "league": "REGULAR", "games": 144},
            ],
        )
        repo.upsert_many(
            [
                {
                    "team_id": "LG",
                    "team_name": "LG Twins",
                    "season": 2024,
                    "league": "REGULAR",
                    "games": 145,
                    "avg": 0.290,
                },
            ],
        )
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
        repo.upsert_many(
            [
                {"team_id": "LG", "team_name": "LG", "season": 2024, "league": "REGULAR", "games": 144},
                {"team_id": "SSG", "team_name": "SSG", "season": 2024, "league": "REGULAR", "games": 144},
            ],
        )
        assert session.query(TeamSeasonBatting).count() == 2


class TestTeamSeasonPitchingRepository:
    def _pitching_session_fixture(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        engine = create_engine("sqlite:///:memory:")
        TeamSeasonPitching.__table__.create(engine)
        return sessionmaker(bind=engine)()

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_creates_records(self, MockEngine, MockSessionLocal):
        session = self._pitching_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        repo = TeamSeasonPitchingRepository()
        result = repo.upsert_many(
            [
                {
                    "team_id": "LG",
                    "team_name": "LG Twins",
                    "season": 2024,
                    "league": "REGULAR",
                    "era": 3.75,
                    "wins": 80,
                },
            ],
        )
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
        repo.upsert_many(
            [
                {"team_id": "LG", "team_name": "LG", "season": 2024, "league": "REGULAR", "era": 3.75},
            ],
        )
        repo.upsert_many(
            [
                {"team_id": "LG", "team_name": "LG", "season": 2024, "league": "REGULAR", "era": 3.50, "wins": 82},
            ],
        )
        rows = session.query(TeamSeasonPitching).all()
        assert len(rows) == 1
        assert rows[0].era == 3.50
        assert rows[0].wins == 82

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_postgresql_dialect(self, MockEngine, MockSessionLocal):
        mock_session = MagicMock()
        MockSessionLocal.return_value.__enter__.return_value = mock_session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "postgresql"

        repo = TeamSeasonPitchingRepository()
        result = repo.upsert_many([{"team_id": "LG", "season": 2024, "league": "REGULAR", "era": 3.75}])
        assert result == 1
        stmt_calls = [call[0][0] for call in mock_session.execute.call_args_list if "PRAGMA" not in str(call[0][0])]
        assert len(stmt_calls) == 1
        assert "ON CONFLICT" in str(stmt_calls[0])

        mock_session.reset_mock()
        result_bulk = repo.upsert_many(
            [
                {"team_id": "LG", "season": 2024, "league": "REGULAR", "era": 3.50},
                {"team_id": "SSG", "season": 2024, "league": "REGULAR", "era": 4.00},
            ]
        )
        assert result_bulk == 2

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_mysql_dialect(self, MockEngine, MockSessionLocal):
        mock_session = MagicMock()
        MockSessionLocal.return_value.__enter__.return_value = mock_session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "mysql"

        repo = TeamSeasonPitchingRepository()
        result = repo.upsert_many([{"team_id": "LG", "season": 2024, "league": "REGULAR", "era": 3.75}])
        assert result == 1
        stmt_calls = [call[0][0] for call in mock_session.execute.call_args_list if "PRAGMA" not in str(call[0][0])]
        assert len(stmt_calls) == 1
        assert "ON DUPLICATE KEY UPDATE" in str(stmt_calls[0])

        mock_session.reset_mock()
        result_bulk = repo.upsert_many(
            [
                {"team_id": "LG", "season": 2024, "league": "REGULAR", "era": 3.50},
                {"team_id": "SSG", "season": 2024, "league": "REGULAR", "era": 4.00},
            ]
        )
        assert result_bulk == 2

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_fallback_dialect(self, MockEngine, MockSessionLocal):
        mock_session = MagicMock()
        MockSessionLocal.return_value.__enter__.return_value = mock_session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "oracle"

        repo = TeamSeasonPitchingRepository()
        result = repo.upsert_many([{"team_id": "LG", "season": 2024, "league": "REGULAR", "era": 3.75}])
        assert result == 1
        assert mock_session.merge.call_count == 1

        mock_session.reset_mock()
        result_bulk = repo.upsert_many(
            [
                {"team_id": "LG", "season": 2024, "league": "REGULAR", "era": 3.50},
                {"team_id": "SSG", "season": 2024, "league": "REGULAR", "era": 4.00},
            ]
        )
        assert result_bulk == 2
        assert mock_session.merge.call_count == 2

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_rollback_on_error(self, MockEngine, MockSessionLocal):
        mock_session = MagicMock()
        MockSessionLocal.return_value.__enter__.return_value = mock_session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        from sqlalchemy.exc import SQLAlchemyError

        mock_session.execute.side_effect = SQLAlchemyError("Execution error")

        repo = TeamSeasonPitchingRepository()
        with pytest.raises(SQLAlchemyError):
            repo.upsert_many([{"team_id": "LG", "season": 2024, "league": "REGULAR", "era": 3.75}])

        mock_session.rollback.assert_called_once()

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    @patch("src.repositories.team_stats_repository.get_database_type")
    def test_upsert_many_non_sqlite_pragma_skipped(self, mock_db_type, MockEngine, MockSessionLocal):
        mock_session = MagicMock()
        MockSessionLocal.return_value.__enter__.return_value = mock_session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"
        mock_db_type.return_value = "postgresql"

        repo = TeamSeasonPitchingRepository()
        repo.upsert_many([{"team_id": "LG", "season": 2024, "league": "REGULAR", "era": 3.75}])

        for call in mock_session.execute.call_args_list:
            arg = str(call[0][0])
            assert "PRAGMA" not in arg
