"""Database engine configuration for Oracle, PostgreSQL, and SQLite."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any
from urllib.parse import quote_plus, unquote, urlsplit

from dotenv import load_dotenv
from sqlalchemy import Engine as SQLAlchemyEngine
from sqlalchemy import create_engine, event, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from src.db.sqlite_integrity import is_sqlite_corruption_error

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)

load_dotenv()

# DATABASE_URL is always the application primary/target database. A separate
# RAG_SOURCE_DB_URL is opened only by the explicit source-read path below.
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("RAG_TEST_DB_URL", "sqlite:///./data/kbo_dev.db")
DISABLE_SQLITE_WAL = os.getenv("DISABLE_SQLITE_WAL", "0") == "1"
DB_SESSION_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError)


def normalize_oracle_url(url: str) -> str:
    """Normalize Oracle connection URL, canonicalizing percent-encoding in credentials."""
    if not url or not url.startswith("oracle"):
        return url
    try:
        parts = urlsplit(url)
        if not parts.netloc or "@" not in parts.netloc:
            return url
        auth, host = parts.netloc.rsplit("@", 1)
        if ":" not in auth:
            return url
        user, password = auth.split(":", 1)
        decoded = unquote(password)
        encoded = quote_plus(decoded)

        # Upper-case percent-encoded hex sequences (e.g. %2f -> %2F)
        res = []
        idx = 0
        while idx < len(encoded):
            if encoded[idx] == "%" and idx + 2 < len(encoded):
                res.append(encoded[idx : idx + 3].upper())
                idx += 3
            else:
                res.append(encoded[idx])
                idx += 1

        netloc = f"{user}:{''.join(res)}@{host}"
        return parts._replace(netloc=netloc).geturl()
    except (ValueError, IndexError, TypeError):
        return url


def _is_sqlite(url: str | None) -> bool:
    if not url:
        return False
    return url.startswith("sqlite:")


def _normalize_sqlite_synchronous(value: str | None) -> str:
    raw_value = (value or "NORMAL").strip().upper()
    if raw_value in {"FULL", "NORMAL"}:
        return raw_value
    logger.warning("Unsupported SQLITE_SYNCHRONOUS=%r; defaulting to NORMAL", value)
    return "NORMAL"


SQLITE_SYNCHRONOUS = _normalize_sqlite_synchronous(os.getenv("SQLITE_SYNCHRONOUS", "NORMAL"))


def _custom_json_deserializer(val: object) -> object:
    """Deserialize JSON strings or Postgres-style string arrays."""
    if not val:
        return val
    val_str = str(val).strip()
    if val_str.startswith('{"') and val_str.endswith('"}') and ":" not in val_str:
        return [e.strip(' "') for e in val_str[1:-1].split(",")]
    try:
        return json.loads(val_str)
    except json.JSONDecodeError:
        return val


def _install_oracle_json_compiler() -> None:
    """Ensure Oracle dialect compiles JSON column types as CLOB."""
    try:
        from sqlalchemy.dialects.oracle.base import OracleTypeCompiler

        if not hasattr(OracleTypeCompiler, "visit_JSON"):

            def visit_JSON(self: Any, type_: Any, **kw: Any) -> str:  # noqa: ANN401, ARG001, N802
                return "CLOB"

            OracleTypeCompiler.visit_JSON = visit_JSON

        def visit_TIME(self: Any, type_: Any, **kw: Any) -> str:  # noqa: ANN401, ARG001, N802
            return "VARCHAR2(8 CHAR)"

        OracleTypeCompiler.visit_TIME = visit_TIME
    except ImportError:
        pass


_oracle_fk_restrict_compiler_installed = False


def _install_oracle_fk_restrict_compiler() -> None:
    """Omit ON DELETE RESTRICT clause for Oracle foreign keys as Oracle doesn't support RESTRICT keyword."""
    global _oracle_fk_restrict_compiler_installed  # noqa: PLW0603
    if _oracle_fk_restrict_compiler_installed:
        return
    try:
        from sqlalchemy.dialects.oracle.base import OracleDDLCompiler

        orig_visit_fk = OracleDDLCompiler.visit_foreign_key_constraint

        def visit_foreign_key_constraint(self: Any, constraint: Any, **kw: Any) -> str:  # noqa: ANN401
            original_ondelete = constraint.ondelete
            if original_ondelete and str(original_ondelete).upper() == "RESTRICT":
                constraint.ondelete = None
            try:
                res = orig_visit_fk(self, constraint, **kw)
            finally:
                constraint.ondelete = original_ondelete
            return res

        OracleDDLCompiler.visit_foreign_key_constraint = visit_foreign_key_constraint
        _oracle_fk_restrict_compiler_installed = True
    except ImportError:
        pass


_install_oracle_json_compiler()
_install_oracle_fk_restrict_compiler()


def _create_sqlite_engine(url: str, *, sqlite_synchronous: str | None = None) -> SQLAlchemyEngine:
    """Create a SQLite engine and register the connection pragmas."""
    synchronous_mode = _normalize_sqlite_synchronous(sqlite_synchronous or SQLITE_SYNCHRONOUS)
    engine = create_engine(
        url,
        connect_args={"check_same_thread": False, "timeout": 120},
        pool_pre_ping=True,
        echo=False,
    )

    @event.listens_for(engine, "connect")
    def _sqlite_pragmas(dbapi_con: sqlite3.Connection, _: object) -> None:
        try:
            cursor = dbapi_con.cursor()
            cursor.execute("PRAGMA foreign_keys = ON;")
            # NOTE: journal_mode = WAL is set once during init_db() via
            # _ensure_wal_mode(), NOT per-connection. Setting it here caused
            # exclusive-lock contention in multi-threaded environments
            # (sqlite3.OperationalError: database is locked).
            cursor.execute("PRAGMA busy_timeout = 120000;")
            if synchronous_mode == "FULL":
                cursor.execute("PRAGMA synchronous = FULL;")
            else:
                cursor.execute("PRAGMA synchronous = NORMAL;")
            cursor.execute("PRAGMA cache_size = -64000;")  # 64MB page cache
            cursor.execute("PRAGMA mmap_size = 268435456;")  # 256MB memory-mapped I/O
            cursor.execute("PRAGMA temp_store = MEMORY;")  # temp tables in memory
            cursor.execute("PRAGMA wal_autocheckpoint = 1000;")  # checkpoint every 1000 pages
            cursor.close()
        except sqlite3.Error:
            logger.warning("Failed to configure SQLite pragmas")

    return engine


def _parse_oracle_connection(
    url: str,
    tns_admin: str | None,
    wallet_password: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Build an Oracle target URL and wallet connection arguments."""
    normalized = normalize_oracle_url(url)
    connect_args: dict[str, Any] = {}
    target_url = normalized

    try:
        parts = urlsplit(normalized)
        auth = parts.netloc.split("@")[0] if "@" in parts.netloc else ""
        host_part = parts.netloc.split("@")[1] if "@" in parts.netloc else parts.netloc
        user = password = None
        if auth:
            if ":" in auth:
                user, password_enc = auth.split(":", 1)
                password = unquote(password_enc)
            else:
                user = auth
        user = user or os.getenv("ORACLE_APP_USER")
        password = password or os.getenv("ORACLE_APP_PASSWORD")

        if tns_admin:
            connect_args["config_dir"] = tns_admin
            connect_args["wallet_location"] = tns_admin
            resolved_wallet_password = wallet_password or os.getenv("OCI_WALLET_PASSWORD")
            if resolved_wallet_password:
                connect_args["wallet_password"] = resolved_wallet_password

        if not parts.path or parts.path == "/":
            target_url = f"{parts.scheme}://@"
            if user:
                connect_args["user"] = user
            if password:
                connect_args["password"] = password
            connect_args["dsn"] = host_part
    except (ValueError, IndexError, AttributeError):
        pass

    return target_url, connect_args


def _create_oracle_engine(
    url: str,
    *,
    tns_admin: str | None = None,
    wallet_password: str | None = None,
) -> SQLAlchemyEngine:
    """Create an Oracle engine with wallet and dialect compatibility settings."""
    _install_oracle_json_compiler()
    _install_oracle_fk_restrict_compiler()
    target_url, connect_args = _parse_oracle_connection(
        url,
        tns_admin or os.getenv("TNS_ADMIN"),
        wallet_password,
    )

    extra_kwargs: dict[str, Any] = {}
    if connect_args:
        extra_kwargs["connect_args"] = connect_args

    eng = create_engine(
        target_url,
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=2,
        echo=False,
        **extra_kwargs,
    )
    if hasattr(eng, "dialect"):
        eng.dialect._json_serializer = json.dumps  # noqa: SLF001
        eng.dialect._json_deserializer = _custom_json_deserializer  # noqa: SLF001
    return eng


def create_engine_for_url(
    url: str,
    *,
    disable_sqlite_wal: bool = False,  # noqa: ARG001
    sqlite_synchronous: str | None = None,
    tns_admin: str | None = None,
    wallet_password: str | None = None,
) -> SQLAlchemyEngine:
    """Create engine for url.

    Args:
        url: Url.
        disable_sqlite_wal: Disable Sqlite Wal.
        sqlite_synchronous: SQLite durability mode (``FULL`` or ``NORMAL``).
        tns_admin: Optional Oracle wallet/TNS configuration directory.
        wallet_password: Optional Oracle wallet password.

    Returns:
        SQLAlchemyEngine instance.

    """
    if _is_sqlite(url):
        return _create_sqlite_engine(url, sqlite_synchronous=sqlite_synchronous)

    if url.startswith("oracle"):
        return _create_oracle_engine(url, tns_admin=tns_admin, wallet_password=wallet_password)

    return create_engine(url, pool_pre_ping=True, pool_size=10, max_overflow=20, echo=False)


Engine = create_engine_for_url(
    DATABASE_URL,
    disable_sqlite_wal=DISABLE_SQLITE_WAL,
    sqlite_synchronous=SQLITE_SYNCHRONOUS,
)
SessionLocal = sessionmaker(bind=Engine, autoflush=False, autocommit=False, expire_on_commit=False)


@contextmanager
def get_db_session() -> Iterator[Session]:
    """Get db session.

    Returns:
        The result of the operation.

    """
    session = SessionLocal()

    try:
        yield session
        session.commit()
    except DB_SESSION_EXCEPTIONS:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def get_rag_source_session() -> Iterator[Session]:
    """Open the optional RAG source database without changing the primary target."""
    source_url = os.getenv("RAG_SOURCE_DB_URL")
    if not source_url or source_url == DATABASE_URL:
        with get_db_session() as session:
            yield session
        return

    source_engine = create_engine_for_url(source_url)
    source_session_factory = sessionmaker(
        bind=source_engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )
    session = source_session_factory()
    try:
        yield session
    except DB_SESSION_EXCEPTIONS:
        session.rollback()
        raise
    finally:
        session.close()
        source_engine.dispose()


@contextmanager
def get_rag_index_session() -> Iterator[Session]:
    """Open the sparse RAG index session, separate from the source database when configured."""
    index_url = os.getenv("RAG_INDEX_DB_URL")
    if not index_url or index_url == DATABASE_URL:
        with get_db_session() as session:
            yield session
        return

    index_engine = create_engine_for_url(index_url)
    index_session_factory = sessionmaker(bind=index_engine, autoflush=False, autocommit=False, expire_on_commit=False)
    session = index_session_factory()
    try:
        yield session
        session.commit()
    except DB_SESSION_EXCEPTIONS:
        session.rollback()
        raise
    finally:
        session.close()
        index_engine.dispose()


def init_rag_index_db() -> None:
    """Create only the sparse RAG tables on a separately configured index database."""
    index_url = os.getenv("RAG_INDEX_DB_URL")
    if not index_url or index_url == DATABASE_URL:
        return

    from src.models.base import Base
    from src.models.embedding_cache import EmbeddingCache
    from src.models.rag_chunk import RagChunk

    index_engine = create_engine_for_url(index_url)
    try:
        Base.metadata.create_all(bind=index_engine, tables=[RagChunk.__table__, EmbeddingCache.__table__])
        if index_engine.dialect.name == "postgresql":
            from sqlalchemy import text

            with index_engine.begin() as connection:
                connection.execute(
                    text(
                        "CREATE UNIQUE INDEX IF NOT EXISTS uq_rag_chunks_source "
                        "ON rag_chunks (source_table, source_row_id)"
                    )
                )
    finally:
        index_engine.dispose()


def get_database_type() -> str:
    """Return the database type based on DATABASE_URL."""
    if DATABASE_URL.startswith("sqlite:"):
        return "sqlite"
    if DATABASE_URL.startswith("oracle"):
        return "oracle"
    if DATABASE_URL.startswith("mysql"):
        return "mysql"
    if DATABASE_URL.startswith("postgresql"):
        return "postgresql"
    return "unknown"


def _ensure_player_batting_team_code_column() -> None:
    """Rename player_season_batting.team_id -> team_code for legacy SQLite DBs."""
    if not _is_sqlite(DATABASE_URL):
        return
    try:
        with Engine.begin() as conn:
            info_rows = conn.exec_driver_sql("PRAGMA table_info(player_season_batting);").fetchall()
            column_names = {row[1] for row in info_rows}
            if "team_code" not in column_names and "team_id" in column_names:
                logger.info("[DB] Migrating player_season_batting.team_id -> team_code")
                conn.exec_driver_sql("ALTER TABLE player_season_batting RENAME COLUMN team_id TO team_code;")
    except SQLAlchemyError as exc:
        logger.warning("Could not migrate player_season_batting.team_id column: %s", exc)


def _ensure_source_capture_columns() -> None:
    """Ensure raw source snapshots can retain immutable capture metadata on SQLite."""
    if not _is_sqlite(DATABASE_URL):
        return
    try:
        with Engine.begin() as conn:
            info_rows = conn.exec_driver_sql("PRAGMA table_info(raw_source_snapshots);").fetchall()
            column_names = {row[1] for row in info_rows}
            if not column_names:
                return
            if "source_url" not in column_names:
                conn.exec_driver_sql("ALTER TABLE raw_source_snapshots ADD COLUMN source_url VARCHAR(1000);")
            if "content_type" not in column_names:
                conn.exec_driver_sql("ALTER TABLE raw_source_snapshots ADD COLUMN content_type VARCHAR(100);")
            if "raw_size" not in column_names:
                conn.exec_driver_sql("ALTER TABLE raw_source_snapshots ADD COLUMN raw_size INTEGER;")
            if "capture_metadata" not in column_names:
                conn.exec_driver_sql("ALTER TABLE raw_source_snapshots ADD COLUMN capture_metadata JSON;")
    except SQLAlchemyError as exc:
        logger.warning("Could not ensure source capture columns: %s", exc)


def _ensure_relay_full_hash_column() -> None:
    """Ensure relay metrics retain the full payload hash on SQLite."""
    if not _is_sqlite(DATABASE_URL):
        return
    try:
        with Engine.begin() as conn:
            info_rows = conn.exec_driver_sql("PRAGMA table_info(game_validation_metrics);").fetchall()
            column_names = {row[1] for row in info_rows}
            if column_names and "payload_hash_full" not in column_names:
                conn.exec_driver_sql("ALTER TABLE game_validation_metrics ADD COLUMN payload_hash_full VARCHAR(64);")
    except SQLAlchemyError as exc:
        logger.warning("Could not ensure relay full hash column: %s", exc)


def _ensure_player_basic_status_columns() -> None:
    """Ensure player_basic has status/staff_role/status_source columns (SQLite)."""
    if not _is_sqlite(DATABASE_URL):
        return
    try:
        with Engine.begin() as conn:
            info_rows = conn.exec_driver_sql("PRAGMA table_info(player_basic);").fetchall()
            column_names = {row[1] for row in info_rows}
            alterations = []
            if "status" not in column_names:
                alterations.append("ADD COLUMN status TEXT")
            if "staff_role" not in column_names:
                alterations.append("ADD COLUMN staff_role TEXT")
            if "status_source" not in column_names:
                alterations.append("ADD COLUMN status_source TEXT")
            for clause in alterations:
                conn.exec_driver_sql(f"ALTER TABLE player_basic {clause};")
    except SQLAlchemyError as exc:
        logger.warning("Could not ensure player_basic status columns: %s", exc)


def _ensure_game_core_tables() -> None:
    """Align game, box_score, and game_summary tables with CSV schema for SQLite."""
    if not _is_sqlite(DATABASE_URL):
        return
    try:
        with Engine.begin() as conn:
            _migrate_game_table(conn)
            _migrate_game_summary_table(conn)
    except SQLAlchemyError as exc:
        logger.warning("Could not align game tables: %s", exc)


def _ensure_game_status_column() -> None:
    """Ensure game table has game_status column (SQLite)."""
    if not _is_sqlite(DATABASE_URL):
        return
    try:
        with Engine.begin() as conn:
            info_rows = conn.exec_driver_sql("PRAGMA table_info(game);").fetchall()
            column_names = {row[1] for row in info_rows}
            if "game_status" not in column_names:
                conn.exec_driver_sql("ALTER TABLE game ADD COLUMN game_status VARCHAR(32);")
    except SQLAlchemyError as exc:
        logger.warning("Could not ensure game.game_status column: %s", exc)


def _ensure_game_identity_columns() -> None:
    """Ensure game identity repair columns exist on SQLite databases."""
    if not _is_sqlite(DATABASE_URL):
        return
    try:
        with Engine.begin() as conn:
            info_rows = conn.exec_driver_sql("PRAGMA table_info(game);").fetchall()
            column_names = {row[1] for row in info_rows}
            if "home_franchise_id" not in column_names:
                conn.exec_driver_sql("ALTER TABLE game ADD COLUMN home_franchise_id INTEGER;")
            if "away_franchise_id" not in column_names:
                conn.exec_driver_sql("ALTER TABLE game ADD COLUMN away_franchise_id INTEGER;")
            if "winning_franchise_id" not in column_names:
                conn.exec_driver_sql("ALTER TABLE game ADD COLUMN winning_franchise_id INTEGER;")
            if "is_primary" not in column_names:
                conn.exec_driver_sql("ALTER TABLE game ADD COLUMN is_primary BOOLEAN DEFAULT 1;")
    except SQLAlchemyError as exc:
        logger.warning("Could not ensure game identity columns: %s", exc)


def _migrate_game_table(conn: Connection) -> None:
    info_rows = conn.exec_driver_sql("PRAGMA table_info(game);").fetchall()
    column_names = {row[1] for row in info_rows}
    required_cols = {
        "id",
        "game_id",
        "game_date",
        "stadium",
        "home_team",
        "away_team",
        "home_score",
        "away_score",
        "away_pitcher",
        "home_pitcher",
        "winning_team",
        "winning_score",
        "season_id",
    }
    extra_cols = {"attendance", "start_time", "end_time", "game_time_minutes", "attendance_source"}
    needs_migration = not required_cols.issubset(column_names) or bool(column_names & extra_cols)
    if not needs_migration:
        return

    has_away_pitcher = "away_pitcher" in column_names
    has_home_pitcher = "home_pitcher" in column_names
    has_winning_team = "winning_team" in column_names
    has_winning_score = "winning_score" in column_names
    has_season_id = "season_id" in column_names
    has_game_status = "game_status" in column_names
    has_home_franchise_id = "home_franchise_id" in column_names
    has_away_franchise_id = "away_franchise_id" in column_names
    has_winning_franchise_id = "winning_franchise_id" in column_names
    has_is_primary = "is_primary" in column_names

    conn.exec_driver_sql("PRAGMA foreign_keys=OFF;")
    conn.exec_driver_sql("ALTER TABLE game RENAME TO game_old;")
    conn.exec_driver_sql(
        """
        CREATE TABLE game (

            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id VARCHAR(20) NOT NULL UNIQUE,
            game_date DATE NOT NULL,
            stadium VARCHAR(50),
            home_team VARCHAR(20),
            away_team VARCHAR(20),
            away_score INTEGER,
            home_score INTEGER,
            away_pitcher VARCHAR(30),
            home_pitcher VARCHAR(30),
            winning_team VARCHAR(20),
            winning_score INTEGER,
            season_id INTEGER,
            game_status VARCHAR(32),
            home_franchise_id INTEGER,
            away_franchise_id INTEGER,
            winning_franchise_id INTEGER,
            is_primary BOOLEAN DEFAULT 1,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL
        );
        """,
    )
    insert_sql = f"""
        INSERT INTO game (
            game_id, game_date, stadium, home_team, away_team,
            away_score, home_score, away_pitcher, home_pitcher,
            winning_team, winning_score, season_id, game_status,
            home_franchise_id, away_franchise_id, winning_franchise_id, is_primary,
            created_at, updated_at
        )
                    SELECT
            game_id,
            game_date,
            stadium,
            home_team,
            away_team,
            away_score,
            home_score,
            {"away_pitcher" if has_away_pitcher else "NULL"},
            {"home_pitcher" if has_home_pitcher else "NULL"},
            {"winning_team" if has_winning_team else "NULL"},
            {"winning_score" if has_winning_score else "NULL"},
            {"season_id" if has_season_id else "NULL"},
            {"game_status" if has_game_status else "NULL"},
            {"home_franchise_id" if has_home_franchise_id else "NULL"},
            {"away_franchise_id" if has_away_franchise_id else "NULL"},
            {"winning_franchise_id" if has_winning_franchise_id else "NULL"},
            {"is_primary" if has_is_primary else "1"},
            created_at,
            updated_at
        FROM game_old;
        """  # noqa: S608
    conn.exec_driver_sql(insert_sql)

    conn.exec_driver_sql("DROP TABLE game_old;")
    conn.exec_driver_sql("PRAGMA foreign_keys=ON;")


def _migrate_game_summary_table(conn: Connection) -> None:
    info_rows = conn.exec_driver_sql("PRAGMA table_info(game_summary);").fetchall()
    column_names = {row[1] for row in info_rows}
    fk_rows = conn.exec_driver_sql("PRAGMA foreign_key_list(game_summary);").fetchall()
    needs_column_fix = "summary_type" not in column_names or "detail_text" not in column_names
    needs_fk_fix = not fk_rows or any(row[2] != "game" for row in fk_rows)
    needs_cascade_fix = not fk_rows or any(row[6] != "CASCADE" for row in fk_rows)
    if not needs_column_fix and not needs_fk_fix and not needs_cascade_fix:
        return

    select_summary = "summary_type" if "summary_type" in column_names else "category"
    select_detail = "detail_text" if "detail_text" in column_names else "content"

    conn.exec_driver_sql("PRAGMA foreign_keys=OFF;")
    conn.exec_driver_sql("ALTER TABLE game_summary RENAME TO game_summary_old;")
    conn.exec_driver_sql(
        """
        CREATE TABLE game_summary (

            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id VARCHAR(20) NOT NULL,
            summary_type VARCHAR(50),
            player_id INTEGER,
            player_name VARCHAR(50),
            detail_text TEXT,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            FOREIGN KEY(game_id) REFERENCES game (game_id) ON DELETE CASCADE
        );
        """,
    )
    has_player_id = "player_id" in column_names
    conn.exec_driver_sql(
        f"""
        INSERT INTO game_summary
            (id, game_id, summary_type, player_id, player_name, detail_text, created_at, updated_at)
        SELECT id, game_id, {select_summary}, {"player_id" if has_player_id else "NULL"},
            player_name, {select_detail}, created_at, updated_at
        FROM game_summary_old;
        """,  # noqa: S608
    )
    conn.exec_driver_sql("DROP TABLE game_summary_old;")
    conn.exec_driver_sql("PRAGMA foreign_keys=ON;")


def _ensure_stat_recalc_view(engine: SQLAlchemyEngine | None = None) -> None:
    """Ensure vw_player_season_batting_recalc view exists."""
    target_engine = engine or Engine
    try:
        with target_engine.begin() as conn:
            inspector = inspect(conn)
            existing_objects = set(inspector.get_table_names()) | set(inspector.get_view_names())
            if "vw_player_season_batting_recalc" in existing_objects:
                return
            conn.exec_driver_sql(
                """
                CREATE VIEW vw_player_season_batting_recalc AS
                SELECT
                    b.player_id,
                    b.player_name,
                    g.season_id AS season,
                    b.team_code,
                    COUNT(DISTINCT b.game_id) AS games,
                    SUM(COALESCE(b.plate_appearances, 0)) AS plate_appearances,
                    SUM(COALESCE(b.at_bats, 0)) AS at_bats,
                    SUM(COALESCE(b.runs, 0)) AS runs,
                    SUM(COALESCE(b.hits, 0)) AS hits,
                    SUM(COALESCE(b.doubles, 0)) AS doubles,
                    SUM(COALESCE(b.triples, 0)) AS triples,
                    SUM(COALESCE(b.home_runs, 0)) AS home_runs,
                    SUM(COALESCE(b.rbi, 0)) AS rbi,
                    SUM(COALESCE(b.walks, 0)) AS walks,
                    SUM(COALESCE(b.strikeouts, 0)) AS strikeouts,
                    CASE WHEN SUM(COALESCE(b.at_bats, 0)) > 0
                         THEN ROUND(CAST(SUM(COALESCE(b.hits, 0)) AS NUMERIC) / SUM(COALESCE(b.at_bats, 0)), 3)
                         ELSE 0.0 END AS avg
                FROM game_batting_stats b
                JOIN game g ON b.game_id = g.game_id
                WHERE b.player_id IS NOT NULL
                GROUP BY b.player_id, b.player_name, g.season_id, b.team_code;
                """
            )
    except SQLAlchemyError as exc:
        logger.warning("Could not ensure vw_player_season_batting_recalc view: %s", exc)
        if target_engine.dialect.name == "oracle":
            raise


def init_db() -> None:
    # Import all models to ensure they are registered in Base.metadata
    """Initialize db."""
    import src.models  # noqa: F401
    from src.models.base import Base

    try:
        Base.metadata.create_all(bind=Engine)
    except SQLAlchemyError as exc:
        if is_sqlite_corruption_error(exc):
            logger.exception(
                "[DB] SQLite database appears corrupt; run src.cli.sqlite_integrity_guard before init_db",
            )
        else:
            logger.exception("[DB] Failed to create tables")
        raise

    _ensure_player_batting_team_code_column()
    _ensure_source_capture_columns()
    _ensure_relay_full_hash_column()
    _ensure_player_basic_status_columns()
    _ensure_game_core_tables()
    _ensure_game_status_column()
    _ensure_game_identity_columns()
    _ensure_stat_recalc_view()
    logger.info("[DB] Database initialized: %s", DATABASE_URL)
