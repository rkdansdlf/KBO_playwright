from __future__ import annotations

from unittest.mock import MagicMock, patch

from sqlalchemy import text

from src.db.engine import (
    DATABASE_URL,
    DISABLE_SQLITE_WAL,
    Engine,
    SessionLocal,
    _is_sqlite,
    create_engine_for_url,
    get_database_type,
    get_db_session,
    get_oci_url,
    get_source_db_url,
    init_db,
)


class TestIsSqlite:
    def test_sqlite_uri(self):
        assert _is_sqlite("sqlite:///data.db")

    def test_sqlite_memory(self):
        assert _is_sqlite("sqlite:///:memory:")

    def test_postgres(self):
        assert not _is_sqlite("postgresql://user:pass@localhost/db")

    def test_mysql(self):
        assert not _is_sqlite("mysql://user:pass@localhost/db")


class TestCreateEngineForUrl:
    def test_sqlite_creates_in_memory(self):
        engine = create_engine_for_url("sqlite:///:memory:")
        assert engine is not None
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            assert result == 1
        engine.dispose()

    def test_sqlite_with_pragmas(self):
        engine = create_engine_for_url("sqlite:///:memory:")
        with engine.connect() as conn:
            result = conn.execute(text("PRAGMA foreign_keys")).scalar()
            assert result == 1
        engine.dispose()

    def test_sqlite_wal_disabled(self):
        engine = create_engine_for_url("sqlite:///:memory:", disable_sqlite_wal=True)
        engine.dispose()

    def test_postgres_url_returns_engine(self):
        engine = create_engine_for_url("postgresql://user:pass@localhost/db")
        assert engine is not None
        engine.dispose()


class TestGetDatabaseType:
    def test_sqlite(self):
        with patch("src.db.engine.DATABASE_URL", "sqlite:///test.db"):
            assert get_database_type() == "sqlite"

    def test_mysql(self):
        with patch("src.db.engine.DATABASE_URL", "mysql://user:pass@localhost/db"):
            assert get_database_type() == "mysql"

    def test_postgresql(self):
        with patch("src.db.engine.DATABASE_URL", "postgresql://user:pass@localhost/db"):
            assert get_database_type() == "postgresql"

    def test_unknown(self):
        with patch("src.db.engine.DATABASE_URL", "oracle://user:pass@localhost/db"):
            assert get_database_type() == "unknown"


class TestGetDbSession:
    def test_session_yielded(self):
        with get_db_session() as session:
            assert session is not None
            result = session.execute(text("SELECT 1")).scalar()
            assert result == 1

    def test_session_rollback_on_error(self):
        mock_session = MagicMock()
        with patch("src.db.engine.SessionLocal", return_value=mock_session):
            try:
                with get_db_session() as _:
                    raise ValueError("test error")  # noqa: TRY301
            except ValueError:
                pass
            mock_session.rollback.assert_called_once()
            mock_session.close.assert_called_once()

    def test_session_commit_success(self):
        mock_session = MagicMock()
        with patch("src.db.engine.SessionLocal", return_value=mock_session):
            with get_db_session() as session:
                assert session is mock_session
            mock_session.commit.assert_called_once()
            mock_session.close.assert_called_once()


class TestInitDb:
    @patch("src.models.base.Base.metadata.create_all")
    @patch("src.db.engine._ensure_player_batting_team_code_column")
    @patch("src.db.engine._ensure_player_basic_status_columns")
    @patch("src.db.engine._ensure_game_core_tables")
    @patch("src.db.engine._ensure_game_status_column")
    @patch("src.db.engine._ensure_game_identity_columns")
    def test_init_db_calls_ensure_functions(
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


class TestGetOciUrl:
    @patch("src.db.engine.os.getenv")
    def test_oci_db_url(self, mock_getenv):
        mock_getenv.side_effect = lambda k, d=None: "postgresql://oci/db" if k == "OCI_DB_URL" else d
        assert get_oci_url() == "postgresql://oci/db"

    @patch("src.db.engine.os.getenv")
    def test_target_db_url(self, mock_getenv):
        mock_getenv.side_effect = lambda k, d=None: "postgresql://target/db" if k == "TARGET_DATABASE_URL" else d
        assert get_oci_url() == "postgresql://target/db"

    @patch("src.db.engine.os.getenv", return_value=None)
    def test_no_url(self, mock_getenv):
        assert get_oci_url() is None


class TestGetSourceDbUrl:
    @patch("src.db.engine.os.getenv")
    def test_custom_url(self, mock_getenv):
        mock_getenv.return_value = "sqlite:///custom.db"
        assert get_source_db_url() == "sqlite:///custom.db"

    @patch("src.db.engine.os.getenv")
    def test_default_url(self, mock_getenv):
        mock_getenv.side_effect = lambda k, d=None: d
        result = get_source_db_url()
        assert result == "sqlite:///./data/kbo_dev.db"


class TestModuleLevel:
    def test_engine_is_created(self):
        assert Engine is not None

    def test_session_local_is_created(self):
        assert SessionLocal is not None

    def test_database_url_default(self):
        assert DATABASE_URL is not None

    def test_disable_sqlite_wal_default(self):
        assert DISABLE_SQLITE_WAL is not None
