from __future__ import annotations

from unittest.mock import MagicMock, patch

from sqlalchemy import text

from src.db.engine import (
    DATABASE_URL,
    DISABLE_SQLITE_WAL,
    Engine,
    SessionLocal,
    _is_sqlite,
    _normalize_sqlite_synchronous,
    create_engine_for_url,
    get_database_type,
    get_db_session,
    get_rag_index_session,
    init_rag_index_db,
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


class TestNormalizeSqliteSynchronous:
    def test_none_defaults_to_normal(self):
        assert _normalize_sqlite_synchronous(None) == "NORMAL"

    def test_accepts_full_case_insensitive(self):
        assert _normalize_sqlite_synchronous(" full ") == "FULL"

    def test_accepts_normal_case_insensitive(self):
        assert _normalize_sqlite_synchronous("normal") == "NORMAL"

    def test_unsupported_defaults_to_normal(self, caplog):
        with caplog.at_level("WARNING"):
            assert _normalize_sqlite_synchronous("OFF") == "NORMAL"
        assert "Unsupported SQLITE_SYNCHRONOUS" in caplog.text


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

    def test_sqlite_synchronous_full(self):
        engine = create_engine_for_url("sqlite:///:memory:", disable_sqlite_wal=True, sqlite_synchronous="FULL")
        with engine.connect() as conn:
            result = conn.execute(text("PRAGMA synchronous")).scalar()
            assert result == 2
        engine.dispose()

    def test_sqlite_wal_disabled(self):
        engine = create_engine_for_url("sqlite:///:memory:", disable_sqlite_wal=True)
        engine.dispose()

    def test_postgres_url_returns_engine(self):
        engine = create_engine_for_url("postgresql://user:pass@localhost/db")
        assert engine is not None
        engine.dispose()

    def test_non_sqlite_engine_uses_pool_options(self):
        with patch("src.db.engine.create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine

            result = create_engine_for_url("mysql://user:pass@localhost/db")

        assert result is mock_engine
        mock_create_engine.assert_called_once_with(
            "mysql://user:pass@localhost/db",
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            echo=False,
        )


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

    def test_oracle(self):
        with patch("src.db.engine.DATABASE_URL", "oracle+oracledb://user:pass@db/service"):
            assert get_database_type() == "oracle"

    def test_unknown(self):
        with patch("src.db.engine.DATABASE_URL", "mssql://user:pass@localhost/db"):
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
                    raise ValueError("test error")
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


class TestRagIndexDatabase:
    def test_uses_index_database_when_configured(self, monkeypatch):
        index_engine = MagicMock()
        index_session = MagicMock()
        session_factory = MagicMock(return_value=index_session)
        monkeypatch.setenv("RAG_INDEX_DB_URL", "sqlite:///rag-index.db")

        with (
            patch("src.db.engine.create_engine_for_url", return_value=index_engine) as create_engine_mock,
            patch("src.db.engine.sessionmaker", return_value=session_factory) as sessionmaker_mock,
        ):
            with get_rag_index_session() as session:
                assert session is index_session

        create_engine_mock.assert_called_once_with("sqlite:///rag-index.db")
        sessionmaker_mock.assert_called_once_with(
            bind=index_engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
        index_session.commit.assert_called_once()
        index_session.close.assert_called_once()
        index_engine.dispose.assert_called_once()

    def test_initializes_only_sparse_tables_on_index_database(self, monkeypatch):
        index_engine = MagicMock()
        monkeypatch.setenv("RAG_INDEX_DB_URL", "sqlite:///rag-index.db")

        with (
            patch("src.db.engine.create_engine_for_url", return_value=index_engine),
            patch("src.models.base.Base.metadata.create_all") as create_all,
        ):
            init_rag_index_db()

        tables = create_all.call_args.kwargs["tables"]
        assert {table.name for table in tables} == {"rag_chunks", "embedding_cache"}
        index_engine.dispose.assert_called_once()


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


class TestModuleLevel:
    def test_engine_is_created(self):
        assert Engine is not None

    def test_session_local_is_created(self):
        assert SessionLocal is not None

    def test_database_url_default(self):
        assert DATABASE_URL is not None

    def test_disable_sqlite_wal_default(self):
        assert DISABLE_SQLITE_WAL is not None


class TestPostgresConnectTimeout:
    """A dead database must not hold a caller for the OS TCP timeout."""

    def test_postgres_urls_get_a_bounded_connect_timeout(self):
        from src.db.engine import DB_CONNECT_TIMEOUT_SECONDS, _postgres_connect_args

        assert _postgres_connect_args("postgresql://user:pw@host:5432/db") == {
            "connect_timeout": DB_CONNECT_TIMEOUT_SECONDS
        }
        assert 0 < DB_CONNECT_TIMEOUT_SECONDS < 300

    def test_other_dialects_get_no_connect_timeout(self):
        from src.db.engine import _postgres_connect_args

        assert _postgres_connect_args("sqlite:///./data/x.db") == {}
        assert _postgres_connect_args("oracle+oracledb://user:pw@host/db") == {}

    def test_postgres_engine_is_built_with_the_bounded_timeout(self):
        from src.db.engine import create_engine_for_url

        with patch("src.db.engine.create_engine") as mock_create:
            create_engine_for_url("postgresql://user:pw@host:5432/db")

        assert mock_create.call_args.kwargs["connect_args"]["connect_timeout"] > 0
