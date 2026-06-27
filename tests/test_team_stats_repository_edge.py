from __future__ import annotations

import contextlib
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching
from src.repositories.team_stats_repository import (
    BaseStatsUpsertRepository,
    TeamSeasonBattingRepository,
    TeamSeasonPitchingRepository,
)


class TestBaseStatsUpsertRepository:
    def _batting_session_fixture(self):
        engine = create_engine("sqlite:///:memory:")
        TeamSeasonBatting.__table__.create(engine)
        return sessionmaker(bind=engine)()

    def _pitching_session_fixture(self):
        engine = create_engine("sqlite:///:memory:")
        TeamSeasonPitching.__table__.create(engine)
        return sessionmaker(bind=engine)()

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_filter_model_fields_removes_unknown_keys(self, MockEngine, MockSessionLocal):
        repo = TeamSeasonBattingRepository()
        result = repo._filter_model_fields({"team_id": "LG", "unknown_field": "value", "games": 144})
        assert "unknown_field" not in result
        assert result == {"team_id": "LG", "games": 144}

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_filter_none_removes_null_values(self, MockEngine, MockSessionLocal):
        repo = TeamSeasonBattingRepository()
        result = repo._filter_none({"team_id": "LG", "games": None, "avg": 0.285})
        assert "games" not in result
        assert result == {"team_id": "LG", "avg": 0.285}

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_filters_invalid_fields(self, MockEngine, MockSessionLocal):
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
                    "invalid_field": "should_be_removed",
                },
            ]
        )
        assert result == 1
        row = session.query(TeamSeasonBatting).one()
        assert not hasattr(row, "invalid_field")

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_with_none_values(self, MockEngine, MockSessionLocal):
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
                    "avg": None,
                },
            ]
        )
        assert result == 1

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_rolls_back_on_error(self, MockEngine, MockSessionLocal):
        from sqlalchemy.exc import SQLAlchemyError

        mock_session = MagicMock()
        mock_session.execute.side_effect = SQLAlchemyError("DB error", None, None)
        MockSessionLocal.return_value.__enter__.return_value = mock_session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        repo = TeamSeasonBattingRepository()
        with contextlib.suppress(Exception):
            repo.upsert_many([{"team_id": "LG", "season": 2024, "league": "REGULAR"}])
        mock_session.rollback.assert_called()

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_pragma_foreign_keys_sqlite(self, MockEngine, MockSessionLocal):
        session = self._batting_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        with patch("src.repositories.team_stats_repository.get_database_type", return_value="sqlite"):
            repo = TeamSeasonBattingRepository()
            result = repo.upsert_many(
                [{"team_id": "LG", "team_name": "LG", "season": 2024, "league": "REGULAR", "games": 144}]
            )
        assert result == 1

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_non_sqlite_no_pragma(self, MockEngine, MockSessionLocal):
        session = self._batting_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        with patch("src.repositories.team_stats_repository.get_database_type", return_value="postgresql"):
            repo = TeamSeasonBattingRepository()
            result = repo.upsert_many(
                [{"team_id": "LG", "team_name": "LG", "season": 2024, "league": "REGULAR", "games": 144}]
            )
        assert result == 1

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_build_insert_stmt_sqlite(self, MockEngine, MockSessionLocal):
        MockEngine.dialect.name = "sqlite"
        repo = TeamSeasonBattingRepository()
        stmt = repo._build_insert_stmt({"team_id": "LG", "season": 2024, "league": "REGULAR", "games": 144})
        assert stmt is not None

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_build_insert_stmt_postgresql(self, MockEngine, MockSessionLocal):
        MockEngine.dialect.name = "postgresql"
        repo = TeamSeasonBattingRepository()
        stmt = repo._build_insert_stmt({"team_id": "LG", "season": 2024, "league": "REGULAR", "games": 144})
        assert stmt is not None

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_build_insert_stmt_mysql(self, MockEngine, MockSessionLocal):
        MockEngine.dialect.name = "mysql"
        repo = TeamSeasonBattingRepository()
        stmt = repo._build_insert_stmt({"team_id": "LG", "season": 2024, "league": "REGULAR", "games": 144})
        assert stmt is not None

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_build_insert_stmt_unknown_dialect_fallback(self, MockEngine, MockSessionLocal):
        MockEngine.dialect.name = "unknown"
        repo = TeamSeasonBattingRepository()
        stmt = repo._build_insert_stmt({"team_id": "LG", "season": 2024, "league": "REGULAR", "games": 144})
        assert stmt is not None

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_pitching_creates_records(self, MockEngine, MockSessionLocal):
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
                    "invalid_field": "should_be_removed",
                },
            ]
        )
        assert result == 1
        row = session.query(TeamSeasonPitching).one()
        assert row.era == 3.75

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_pitching_empty(self, MockEngine, MockSessionLocal):
        repo = TeamSeasonPitchingRepository()
        assert repo.upsert_many([]) == 0

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_multiple_records(self, MockEngine, MockSessionLocal):
        session = self._batting_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        repo = TeamSeasonBattingRepository()
        result = repo.upsert_many(
            [
                {"team_id": "LG", "team_name": "LG", "season": 2024, "league": "REGULAR", "games": 144},
                {"team_id": "SSG", "team_name": "SSG", "season": 2024, "league": "REGULAR", "games": 144},
                {"team_id": "KT", "team_name": "KT", "season": 2024, "league": "REGULAR", "games": 144},
            ]
        )
        assert result == 3
        assert session.query(TeamSeasonBatting).count() == 3

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_all_none_values(self, MockEngine, MockSessionLocal):
        session = self._batting_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        repo = TeamSeasonBattingRepository()
        result = repo.upsert_many(
            [
                {
                    "team_id": "LG",
                    "team_name": "LG",
                    "season": 2024,
                    "league": "REGULAR",
                    "games": None,
                    "avg": None,
                },
            ]
        )
        assert result == 1

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_with_extra_stats(self, MockEngine, MockSessionLocal):
        session = self._batting_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        repo = TeamSeasonBattingRepository()
        result = repo.upsert_many(
            [
                {
                    "team_id": "LG",
                    "team_name": "LG",
                    "season": 2024,
                    "league": "REGULAR",
                    "games": 144,
                    "extra_stats": {"custom_metric": 42},
                },
            ]
        )
        assert result == 1
        row = session.query(TeamSeasonBatting).one()
        assert row.extra_stats == {"custom_metric": 42}

    @patch("src.repositories.team_stats_repository.SessionLocal")
    @patch("src.repositories.team_stats_repository.Engine")
    def test_upsert_many_pitching_updates_multiple(self, MockEngine, MockSessionLocal):
        session = self._pitching_session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        repo = TeamSeasonPitchingRepository()
        repo.upsert_many([{"team_id": "LG", "team_name": "LG", "season": 2024, "league": "REGULAR", "era": 3.75}])
        repo.upsert_many([{"team_id": "LG", "team_name": "LG", "season": 2024, "league": "REGULAR", "era": 3.50}])
        repo.upsert_many([{"team_id": "LG", "team_name": "LG", "season": 2024, "league": "REGULAR", "era": 3.25}])
        rows = session.query(TeamSeasonPitching).all()
        assert len(rows) == 1
        assert rows[0].era == 3.25
