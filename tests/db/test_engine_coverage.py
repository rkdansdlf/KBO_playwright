from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import (
    _ensure_game_core_tables,
    _ensure_game_identity_columns,
    _ensure_game_status_column,
    _ensure_player_basic_status_columns,
    _ensure_player_batting_team_code_column,
    _is_sqlite,
    init_db,
)


@pytest.fixture
def inmem_engine():
    engine = create_engine("sqlite:///:memory:", echo=False)
    yield engine
    engine.dispose()


def _create_table(engine, sql):
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()


def _get_column_names(engine, table):
    with engine.connect() as conn:
        rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    return {row[1] for row in rows}


class TestIsSqlite:
    def test_none(self):
        assert not _is_sqlite(None)

    def test_empty_string(self):
        assert not _is_sqlite("")

    def test_sqlite_memory(self):
        assert _is_sqlite("sqlite:///:memory:")

    def test_postgres(self):
        assert not _is_sqlite("postgresql://host/db")


class TestEnsurePlayerBattingTeamCodeColumn:
    def test_already_has_team_code(self, inmem_engine):
        _create_table(inmem_engine, "CREATE TABLE player_season_batting (id INTEGER, team_code TEXT, team_id INTEGER)")
        with patch("src.db.engine.Engine", inmem_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_player_batting_team_code_column()
        names = _get_column_names(inmem_engine, "player_season_batting")
        assert "team_code" in names
        assert "team_id" in names

    def test_migrate_team_id_to_team_code(self, inmem_engine):
        _create_table(inmem_engine, "CREATE TABLE player_season_batting (id INTEGER, team_id INTEGER)")
        with patch("src.db.engine.Engine", inmem_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_player_batting_team_code_column()
        names = _get_column_names(inmem_engine, "player_season_batting")
        assert "team_code" in names
        assert "team_id" not in names

    def test_neither_column(self, inmem_engine):
        _create_table(inmem_engine, "CREATE TABLE player_season_batting (id INTEGER)")
        with patch("src.db.engine.Engine", inmem_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_player_batting_team_code_column()
        names = _get_column_names(inmem_engine, "player_season_batting")
        assert "team_code" not in names

    def test_sqlalchemy_error_handled(self, inmem_engine):
        with patch("src.db.engine.Engine", inmem_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_player_batting_team_code_column()


class TestEnsurePlayerBasicStatusColumns:
    def test_all_columns_present(self, inmem_engine):
        _create_table(
            inmem_engine, "CREATE TABLE player_basic (id INTEGER, status TEXT, staff_role TEXT, status_source TEXT)"
        )
        with patch("src.db.engine.Engine", inmem_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_player_basic_status_columns()
        names = _get_column_names(inmem_engine, "player_basic")
        assert "status" in names and "staff_role" in names and "status_source" in names

    def test_missing_all_columns(self, inmem_engine):
        _create_table(inmem_engine, "CREATE TABLE player_basic (id INTEGER)")
        with patch("src.db.engine.Engine", inmem_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_player_basic_status_columns()
        names = _get_column_names(inmem_engine, "player_basic")
        assert "status" in names and "staff_role" in names and "status_source" in names

    def test_missing_status_only(self, inmem_engine):
        _create_table(inmem_engine, "CREATE TABLE player_basic (id INTEGER, staff_role TEXT, status_source TEXT)")
        with patch("src.db.engine.Engine", inmem_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_player_basic_status_columns()
        names = _get_column_names(inmem_engine, "player_basic")
        assert "status" in names

    def test_sqlalchemy_error_handled(self, inmem_engine):
        with patch("src.db.engine.Engine", inmem_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_player_basic_status_columns()


class TestEnsureGameStatusColumn:
    def test_already_has_game_status(self, inmem_engine):
        _create_table(inmem_engine, "CREATE TABLE game (id INTEGER, game_status TEXT)")
        with patch("src.db.engine.Engine", inmem_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_game_status_column()
        names = _get_column_names(inmem_engine, "game")
        assert "game_status" in names

    def test_missing_game_status(self, inmem_engine):
        _create_table(inmem_engine, "CREATE TABLE game (id INTEGER)")
        with patch("src.db.engine.Engine", inmem_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_game_status_column()
        names = _get_column_names(inmem_engine, "game")
        assert "game_status" in names

    def test_sqlalchemy_error_handled(self, inmem_engine):
        with patch("src.db.engine.Engine", inmem_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_game_status_column()


class TestEnsureGameIdentityColumns:
    def test_all_columns_present(self, inmem_engine):
        _create_table(
            inmem_engine,
            "CREATE TABLE game (id INTEGER, home_franchise_id INTEGER, away_franchise_id INTEGER, winning_franchise_id INTEGER, is_primary BOOLEAN)",
        )
        with patch("src.db.engine.Engine", inmem_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_game_identity_columns()
        names = _get_column_names(inmem_engine, "game")
        for col in ("home_franchise_id", "away_franchise_id", "winning_franchise_id", "is_primary"):
            assert col in names

    def test_missing_all_columns(self, inmem_engine):
        _create_table(inmem_engine, "CREATE TABLE game (id INTEGER)")
        with patch("src.db.engine.Engine", inmem_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_game_identity_columns()
        names = _get_column_names(inmem_engine, "game")
        for col in ("home_franchise_id", "away_franchise_id", "winning_franchise_id", "is_primary"):
            assert col in names

    def test_partial_columns(self, inmem_engine):
        _create_table(inmem_engine, "CREATE TABLE game (id INTEGER, home_franchise_id INTEGER)")
        with patch("src.db.engine.Engine", inmem_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_game_identity_columns()
        names = _get_column_names(inmem_engine, "game")
        assert "away_franchise_id" in names
        assert "winning_franchise_id" in names
        assert "is_primary" in names

    def test_sqlalchemy_error_handled(self, inmem_engine):
        with patch("src.db.engine.Engine", inmem_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_game_identity_columns()


class TestInitDb:
    @patch("src.models.base.Base.metadata.create_all")
    @patch("src.db.engine._ensure_player_batting_team_code_column")
    @patch("src.db.engine._ensure_player_basic_status_columns")
    @patch("src.db.engine._ensure_game_core_tables")
    @patch("src.db.engine._ensure_game_status_column")
    @patch("src.db.engine._ensure_game_identity_columns")
    def test_init_db_calls_all_ensure_functions(
        self,
        mock_identity,
        mock_status_col,
        mock_core,
        mock_basic,
        mock_batting,
        mock_meta,
    ):
        init_db()
        mock_meta.assert_called_once()
        mock_batting.assert_called_once()
        mock_basic.assert_called_once()
        mock_core.assert_called_once()
        mock_status_col.assert_called_once()
        mock_identity.assert_called_once()

    @patch("src.models.base.Base.metadata.create_all", side_effect=Exception("create_all failed"))
    def test_init_db_create_all_raises(self, mock_meta):
        with pytest.raises(Exception, match="create_all failed"):
            init_db()
        mock_meta.assert_called_once()


def _make_mock_exec(sql_results):
    """Return a function that returns different MagicMock results based on SQL content."""

    def _exec_driver_sql(sql, *args, **kwargs):
        mock_result = MagicMock()
        for pattern, rows in sql_results:
            if pattern in sql:
                mock_result.fetchall.return_value = rows
                return mock_result
        return mock_result

    return _exec_driver_sql


class TestEnsureGameCoreTables:
    def test_no_migration_needed(self):
        conn = MagicMock()
        pragma_game = [
            (0, "id", "INTEGER", 1, None, 1),
            (1, "game_id", "VARCHAR(20)", 1, None, 0),
            (2, "game_date", "DATE", 1, None, 0),
            (3, "stadium", "VARCHAR(50)", 0, None, 0),
            (4, "home_team", "VARCHAR(20)", 0, None, 0),
            (5, "away_team", "VARCHAR(20)", 0, None, 0),
            (6, "home_score", "INTEGER", 0, None, 0),
            (7, "away_score", "INTEGER", 0, None, 0),
            (8, "away_pitcher", "VARCHAR(30)", 0, None, 0),
            (9, "home_pitcher", "VARCHAR(30)", 0, None, 0),
            (10, "winning_team", "VARCHAR(20)", 0, None, 0),
            (11, "winning_score", "INTEGER", 0, None, 0),
            (12, "season_id", "INTEGER", 0, None, 0),
        ]
        conn.exec_driver_sql.side_effect = _make_mock_exec(
            [
                ("table_info(game)", pragma_game),
                (
                    "table_info(game_summary)",
                    [
                        (0, "id", "INTEGER", 1, None, 1),
                        (1, "game_id", "VARCHAR(20)", 1, None, 0),
                        (2, "summary_type", "VARCHAR(50)", 0, None, 0),
                        (3, "detail_text", "TEXT", 0, None, 0),
                    ],
                ),
                (
                    "foreign_key_list(game_summary)",
                    [
                        (0, 0, "game", "game_id", "game_id", "NO ACTION", "NO ACTION", "NONE"),
                    ],
                ),
            ]
        )
        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = conn
        with patch("src.db.engine.Engine", mock_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_game_core_tables()
        assert conn.exec_driver_sql.call_count >= 3

    def test_migration_needed_missing_columns(self):
        conn = MagicMock()
        pragma_game = [
            (0, "id", "INTEGER", 1, None, 1),
            (1, "game_id", "VARCHAR(20)", 1, None, 0),
        ]
        conn.exec_driver_sql.side_effect = _make_mock_exec(
            [
                ("table_info(game)", pragma_game),
                (
                    "table_info(game_summary)",
                    [
                        (0, "id", "INTEGER", 1, None, 1),
                        (1, "game_id", "VARCHAR(20)", 1, None, 0),
                        (2, "category", "VARCHAR(50)", 0, None, 0),
                        (3, "content", "TEXT", 0, None, 0),
                    ],
                ),
                (
                    "foreign_key_list(game_summary)",
                    [
                        (0, 0, "wrong_table", "game_id", "game_id", "NO ACTION", "NO ACTION", "NONE"),
                    ],
                ),
            ]
        )
        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = conn
        with patch("src.db.engine.Engine", mock_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_game_core_tables()
        assert conn.exec_driver_sql.call_count >= 6

    def test_sqlalchemy_error_handled(self):
        conn = MagicMock()
        conn.exec_driver_sql.side_effect = SQLAlchemyError("db error")
        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = conn
        with patch("src.db.engine.Engine", mock_engine):
            with patch("src.db.engine.DATABASE_URL", "sqlite:///:memory:"):
                _ensure_game_core_tables()
