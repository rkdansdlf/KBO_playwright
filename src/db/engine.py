"""Database engine configuration for PostgreSQL and SQLite."""

from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import contextmanager
from functools import lru_cache
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

from sqlalchemy import Engine as SQLAlchemyEngine
from sqlalchemy import create_engine, event, inspect
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from src.config.env_loader import load_project_env
from src.db.sqlite_integrity import is_sqlite_corruption_error

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)

load_project_env()

# DATABASE_URL is always the application primary/target database. A separate
# RAG_SOURCE_DB_URL is opened only by the explicit source-read path below.
DATABASE_URL: str = os.getenv("DATABASE_URL") or os.getenv("RAG_TEST_DB_URL") or "sqlite:///./data/kbo_dev.db"
DISABLE_SQLITE_WAL = os.getenv("DISABLE_SQLITE_WAL", "0") == "1"
DB_SESSION_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError)


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


#: Bound the connect phase for the operational database. Without it psycopg2 waits
#: for the OS TCP timeout (~300s against the remote Tailscale host), so during the
#: 2026-10-03 outage every DB-bound scheduler job stalled for minutes while holding
#: its tier lock and starved the jobs that shared it. A ``connect_timeout`` bounds
#: only establishing a connection, never query execution.
DB_CONNECT_TIMEOUT_SECONDS = int(os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "10"))


def _postgres_connect_args(url: str) -> dict[str, Any]:
    """Return bounded connect arguments for a PostgreSQL URL."""
    if url.startswith(("postgresql", "postgres")):
        return {"connect_timeout": DB_CONNECT_TIMEOUT_SECONDS}
    return {}


def create_engine_for_url(
    url: str,
    *,
    disable_sqlite_wal: bool = False,  # noqa: ARG001
    sqlite_synchronous: str | None = None,
) -> SQLAlchemyEngine:
    """Create engine for url.

    Args:
        url: Url.
        disable_sqlite_wal: Disable Sqlite Wal.
        sqlite_synchronous: SQLite durability mode (``FULL`` or ``NORMAL``).

    Returns:
        SQLAlchemyEngine instance.

    """
    if _is_sqlite(url):
        return _create_sqlite_engine(url, sqlite_synchronous=sqlite_synchronous)

    if url.startswith("oracle"):
        msg = "Oracle support has been removed; point DATABASE_URL at PostgreSQL (or SQLite for tests)."
        raise ValueError(msg)

    connect_args = _postgres_connect_args(url)
    if connect_args:
        return create_engine(
            url,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            echo=False,
            connect_args=connect_args,
        )
    return create_engine(url, pool_pre_ping=True, pool_size=10, max_overflow=20, echo=False)


Engine = create_engine_for_url(
    DATABASE_URL,
    disable_sqlite_wal=DISABLE_SQLITE_WAL,
    sqlite_synchronous=SQLITE_SYNCHRONOUS,
)

#: Connect timeout for the fail-fast reachability probe. A dead database must
#: cost a scheduler job seconds rather than the OS-level connect timeout, because
#: the probe runs *before* the job takes a tier lock.
DB_PROBE_CONNECT_TIMEOUT_SECONDS = 3

#: How many probe engines to keep. A probe is per *URL*, not per database, and a
#: job may name several: one RAG build can open ``DATABASE_URL``, ``RAG_SOURCE_DB_URL``,
#: ``RAG_INDEX_DB_URL`` and ``PGVECTOR_URL`` between them, and each URL is also a
#: separate key by timeout. Four was enough while the probe asked about one
#: database; with more it silently evicts, so every probe builds a fresh engine
#: and its connection pool, which is the cost the cache exists to avoid. The
#: bound is generous rather than exact -- an undersized cache fails quietly, and a
#: slightly oversized one costs a few idle objects.
DB_PROBE_ENGINE_CACHE_SIZE = 16


@lru_cache(maxsize=DB_PROBE_ENGINE_CACHE_SIZE)
def _probe_engine(url: str, timeout_seconds: int) -> SQLAlchemyEngine:
    """Build and cache a one-connection engine with a short connect timeout."""
    if _is_sqlite(url):
        # SQLite answers immediately and rejects ``connect_timeout``.
        return create_engine(url, pool_pre_ping=True)
    parsed = make_url(url)
    query = {**dict(parsed.query), "connect_timeout": str(timeout_seconds)}
    return create_engine(parsed.set(query=query), pool_pre_ping=True, pool_size=1, max_overflow=0)


def database_reachable(
    *,
    urls: Sequence[str] | None = None,
    timeout_seconds: int = DB_PROBE_CONNECT_TIMEOUT_SECONDS,
) -> bool:
    """Return whether the named databases answer ``SELECT 1`` quickly.

    Scheduler jobs run on fixed cron ticks, and a job that blocks on connect
    while holding a tier lock starves every other job sharing that lock. During
    the 2026-10-03 outage each dead-letter job spent ~150s on connect retries
    under ``MAINTENANCE_LOCK`` and other maintenance jobs were skipped, so
    DB-bound jobs gate on this probe **before** acquiring a lock and return early
    when it is false. Alerting is owned by the Prometheus ``kbo_db_available``
    gauge, so this gate stays silent apart from a warning from its caller.

    Args:
        urls: Databases the job needs, all of which must answer. ``None`` means
            the operational database, which is what most jobs need. A job that
            reaches a second store -- a RAG build opening ``PGVECTOR_URL``, say --
            must name it here: a gate that only knows ``DATABASE_URL`` lets that
            job discover the dead store by timing out on it *while holding the
            lock*, which is the failure this probe was added to prevent.
        timeout_seconds: Connect timeout per database.

    Returns:
        Whether every named database answered.

    """
    from src.monitoring.db_availability import probe_engine

    targets = tuple(urls) if urls else (Engine.url.render_as_string(hide_password=False),)
    for url in targets:
        available, _latency = probe_engine(_probe_engine(url, timeout_seconds))
        if not available:
            return False
    return True


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
        Base.metadata.create_all(
            bind=index_engine,
            tables=[RagChunk.__table__, EmbeddingCache.__table__],  # type: ignore[list-item]
        )
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
