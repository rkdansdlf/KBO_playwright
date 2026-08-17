from __future__ import annotations

from unittest.mock import MagicMock, patch
import pytest
import contextlib

from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching
from src.repositories.team_stats_repository import TeamSeasonBattingRepository, TeamSeasonPitchingRepository


class TestTeamSeasonBattingRepository:
    def _batting_session_fixture(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        engine = create_engine("sqlite:///:memory:")
        TeamSeasonBatting.__table__.create(engine)
        return sessionmaker(bind=engine)()

    def test_upsert_many_creates_records(self):
        session = self._batting_session_fixture()

        repo = TeamSeasonBattingRepository(session)
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

    def test_upsert_many_empty(self):
        repo = TeamSeasonBattingRepository(MagicMock())
        assert repo.upsert_many([]) == 0

    def test_upsert_many_updates_existing(self):
        session = self._batting_session_fixture()

        repo = TeamSeasonBattingRepository(session)
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

    def test_upsert_many_multiple_teams(self):
        session = self._batting_session_fixture()

        repo = TeamSeasonBattingRepository(session)
        repo.upsert_many(
            [
                {"team_id": "LG", "team_name": "LG", "season": 2024, "league": "REGULAR", "games": 144},
                {"team_id": "SSG", "team_name": "SSG", "season": 2024, "league": "REGULAR", "games": 140},
            ],
        )
        assert session.query(TeamSeasonBatting).count() == 2
        rows = {row.team_id: row for row in session.query(TeamSeasonBatting).all()}
        assert rows["LG"].games == 144
        assert rows["SSG"].games == 140


class TestTeamSeasonPitchingRepository:
    def _pitching_session_fixture(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        engine = create_engine("sqlite:///:memory:")
        TeamSeasonPitching.__table__.create(engine)
        return sessionmaker(bind=engine)()

    def test_upsert_many_creates_records(self):
        session = self._pitching_session_fixture()

        repo = TeamSeasonPitchingRepository(session)
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

    def test_upsert_many_empty(self):
        repo = TeamSeasonPitchingRepository(MagicMock())
        assert repo.upsert_many([]) == 0

    def test_upsert_many_updates_existing(self):
        session = self._pitching_session_fixture()

        repo = TeamSeasonPitchingRepository(session)
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

    def test_upsert_many_postgresql_dialect(self):
        mock_session = MagicMock()
        mock_session.get_bind.return_value.dialect.name = "postgresql"

        repo = TeamSeasonPitchingRepository(mock_session)
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

    def test_upsert_many_mysql_dialect(self):
        mock_session = MagicMock()
        mock_session.get_bind.return_value.dialect.name = "mysql"

        repo = TeamSeasonPitchingRepository(mock_session)
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

    def test_upsert_many_fallback_dialect(self):
        mock_session = MagicMock()
        mock_session.get_bind.return_value.dialect.name = "oracle"
        mock_session.execute.return_value.scalars.return_value.first.return_value = None

        repo = TeamSeasonPitchingRepository(mock_session)
        result = repo.upsert_many([{"team_id": "LG", "season": 2024, "league": "REGULAR", "era": 3.75}])
        assert result == 1
        assert mock_session.add.call_count == 1

        mock_session.reset_mock()
        result_bulk = repo.upsert_many(
            [
                {"team_id": "LG", "season": 2024, "league": "REGULAR", "era": 3.50},
                {"team_id": "SSG", "season": 2024, "league": "REGULAR", "era": 4.00},
            ]
        )
        assert result_bulk == 2
        assert mock_session.add.call_count == 2

    def test_upsert_many_rollback_on_error(self):
        mock_session = MagicMock()

        from sqlalchemy.exc import SQLAlchemyError

        mock_session.execute.side_effect = SQLAlchemyError("Execution error")

        repo = TeamSeasonPitchingRepository(mock_session)
        with pytest.raises(SQLAlchemyError):
            repo.upsert_many([{"team_id": "LG", "season": 2024, "league": "REGULAR", "era": 3.75}])

        mock_session.rollback.assert_not_called()

    @patch("src.repositories.team_stats_repository.get_database_type")
    def test_upsert_many_non_sqlite_pragma_skipped(self, mock_db_type):
        mock_session = MagicMock()
        mock_db_type.return_value = "postgresql"

        repo = TeamSeasonPitchingRepository(mock_session)
        repo.upsert_many([{"team_id": "LG", "season": 2024, "league": "REGULAR", "era": 3.75}])

        for call in mock_session.execute.call_args_list:
            arg = str(call[0][0])
            assert "PRAGMA" not in arg
