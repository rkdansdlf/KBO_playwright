"""Database engine configuration.

Supports both SQLite (dev) and MySQL (production).

"""

from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any
from urllib.parse import quote_plus, unquote

from dotenv import load_dotenv
from sqlalchemy import Engine as SQLAlchemyEngine
from sqlalchemy import ForeignKeyConstraint, create_engine, event
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.types import Time

from src.db.sqlite_integrity import is_sqlite_corruption_error

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data/kbo_dev.db")
DISABLE_SQLITE_WAL = os.getenv("DISABLE_SQLITE_WAL", "0") == "1"
DB_SESSION_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError)


def _install_oracle_json_compiler() -> None:
    """Provide JSON-to-CLOB compilation for SQLAlchemy Oracle dialects."""
    try:
        from sqlalchemy.dialects.oracle.base import OracleTypeCompiler
    except ImportError:
        logger.debug("Oracle dialect is unavailable")
        return

    if hasattr(OracleTypeCompiler, "visit_JSON"):
        return

    def _visit_json(_compiler: object, _type: object, **_kwargs: object) -> str:
        return "CLOB"

    OracleTypeCompiler.visit_JSON = _visit_json  # type: ignore[attr-defined]


_install_oracle_json_compiler()


def _install_oracle_fk_restrict_compiler() -> None:
    """Ignore ON DELETE RESTRICT clause for Oracle foreign keys (Oracle's default)."""
    try:
        from sqlalchemy.dialects.oracle.base import OracleDDLCompiler
    except ImportError:
        logger.debug("Oracle dialect is unavailable")
        return

    current_visit_fk = OracleDDLCompiler.visit_foreign_key_constraint
    if getattr(current_visit_fk, "_kbo_fk_restrict_patch", False):
        return

    orig_visit_fk = current_visit_fk

    def patch_visit_fk(compiler: object, constraint: ForeignKeyConstraint, **kw: object) -> str:
        old_ondelete = constraint.ondelete
        if isinstance(old_ondelete, str) and old_ondelete.upper() == "RESTRICT":
            constraint.ondelete = None
        try:
            return orig_visit_fk(compiler, constraint, **kw)
        finally:
            constraint.ondelete = old_ondelete

    patch_visit_fk._kbo_fk_restrict_patch = True  # noqa: SLF001  # type: ignore[attr-defined]
    OracleDDLCompiler.visit_foreign_key_constraint = patch_visit_fk  # type: ignore[method-assign]


_install_oracle_fk_restrict_compiler()


@compiles(Time, "oracle")
def _compile_time_oracle(_type: Time, _compiler: object, **_kw: object) -> str:
    return "DATE"


def get_oci_url() -> str | None:
    """Resolve the OCI/Target database URL from environment variables."""
    return os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")


def get_source_db_url() -> str:
    """Resolve the source/local database URL (with SQLite default)."""
    return os.getenv("SOURCE_DATABASE_URL", "sqlite:///./data/kbo_dev.db")


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


def normalize_oracle_url(url: str) -> str:
    """Normalize an Oracle URL while preserving encoded credentials."""
    if not url.startswith("oracle+oracledb://"):
        return url
    try:
        rest = url.split("oracle+oracledb://", 1)[1]
        auth_part, dsn = rest.rsplit("@", 1)
        if ":" in auth_part:
            user, password = auth_part.split(":", 1)
            encoded_password = quote_plus(unquote(password))
            return f"oracle+oracledb://{user}:{encoded_password}@{dsn}"
    except (IndexError, ValueError):
        logger.debug("Could not normalize Oracle URL")
    return url


def _oracle_connect_args(url: str) -> dict[str, Any]:
    tns_admin = os.getenv("TNS_ADMIN")
    if not tns_admin:
        return {}

    connect_args: dict[str, Any] = {"config_dir": tns_admin, "wallet_location": tns_admin}
    try:
        auth_part = url.split("oracle+oracledb://", 1)[1].rsplit("@", 1)[0]
        if ":" in auth_part:
            _, password = auth_part.split(":", 1)
            connect_args["wallet_password"] = unquote(password)
    except (IndexError, ValueError):
        logger.debug("Could not parse Oracle wallet credentials from URL")
    return connect_args


def _create_oracle_engine(url: str) -> SQLAlchemyEngine:
    engine = create_engine(
        normalize_oracle_url(url),
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        echo=False,
        connect_args=_oracle_connect_args(url),
    )
    if not hasattr(engine.dialect, "_json_deserializer"):
        engine.dialect._json_deserializer = None  # noqa: SLF001
    return engine


def create_engine_for_url(
    url: str,
    *,
    disable_sqlite_wal: bool = False,
    sqlite_synchronous: str | None = None,
) -> SQLAlchemyEngine:
    """Create engine for url.

        Args:
            url: Url.
            disable_sqlite_wal: Disable Sqlite Wal.
            sqlite_synchronous: SQLite durability mode (``FULL`` or ``NORMAL``).
            url: Url.
        disable_sqlite_wal: Disable Sqlite Wal.
        url: Url.

    Returns:
        SQLAlchemyEngine instance.

    """
    if _is_sqlite(url):
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
                if not disable_sqlite_wal:
                    cursor.execute("PRAGMA journal_mode = WAL;")
                cursor.execute("PRAGMA busy_timeout = 120000;")
                if synchronous_mode == "FULL":
                    cursor.execute("PRAGMA synchronous = FULL;")
                else:
                    cursor.execute("PRAGMA synchronous = NORMAL;")
                cursor.close()
            except sqlite3.Error:
                logger.warning("Failed to configure SQLite pragmas")

        return engine

    if url.startswith("oracle"):
        return _create_oracle_engine(url)

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


def get_database_type() -> str:
    """Return the database type based on DATABASE_URL."""
    if DATABASE_URL.startswith("sqlite:"):
        return "sqlite"
    if DATABASE_URL.startswith("mysql"):
        return "mysql"
    if DATABASE_URL.startswith("postgresql"):
        return "postgresql"
    if DATABASE_URL.startswith("oracle"):
        return "oracle"
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
    _ensure_player_basic_status_columns()
    _ensure_game_core_tables()
    _ensure_game_status_column()
    _ensure_game_identity_columns()
    logger.info("[DB] Database initialized: %s", DATABASE_URL)
