"""
Database engine configuration
Supports both SQLite (dev) and MySQL (production)
"""
import os
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data/kbo_dev.db")
DISABLE_SQLITE_WAL = os.getenv("DISABLE_SQLITE_WAL", "0") == "1"

def _is_sqlite(url: str) -> bool:
    return url.startswith("sqlite:")

def create_engine_for_url(url: str, *, disable_sqlite_wal: bool = False):
    if _is_sqlite(url):
        engine = create_engine(url, connect_args={"check_same_thread": False}, pool_pre_ping=True, echo=False)
        @event.listens_for(engine, "connect")
        def _sqlite_pragmas(dbapi_con, _):
            try:
                cursor = dbapi_con.cursor()
                cursor.execute("PRAGMA foreign_keys = ON;")
                if not disable_sqlite_wal: cursor.execute("PRAGMA journal_mode = WAL;")
                cursor.execute("PRAGMA synchronous = NORMAL;")
                cursor.close()
            except: pass
        return engine
    return create_engine(url, pool_pre_ping=True, pool_size=10, max_overflow=20, echo=False)

Engine = create_engine_for_url(DATABASE_URL, disable_sqlite_wal=DISABLE_SQLITE_WAL)
SessionLocal = sessionmaker(bind=Engine, autoflush=False, autocommit=False, expire_on_commit=False)

from contextlib import contextmanager

@contextmanager
def get_db_session():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_database_type() -> str:
    """Return the database type based on DATABASE_URL."""
    if DATABASE_URL.startswith("sqlite:"):
        return "sqlite"
    elif DATABASE_URL.startswith("mysql"):
        return "mysql"
    elif DATABASE_URL.startswith("postgresql"):
        return "postgresql"
    else:
        return "unknown"


def _ensure_player_batting_team_code_column():
    """Rename player_season_batting.team_id -> team_code for legacy SQLite DBs."""
    if not _is_sqlite(DATABASE_URL):
        return
    try:
        with Engine.begin() as conn:
            info_rows = conn.exec_driver_sql("PRAGMA table_info(player_season_batting);").fetchall()
            column_names = {row[1] for row in info_rows}
            if "team_code" not in column_names and "team_id" in column_names:
                print("[DB] Migrating player_season_batting.team_id -> team_code")
                conn.exec_driver_sql("ALTER TABLE player_season_batting RENAME COLUMN team_id TO team_code;")
    except Exception as exc:
        print(f"[WARN] Could not migrate player_season_batting.team_id column: {exc}")

def _ensure_player_basic_status_columns():
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
    except Exception as exc:
        print(f"[WARN] Could not ensure player_basic status columns: {exc}")


def _ensure_game_core_tables():
    """Align game, box_score, and game_summary tables with CSV schema for SQLite."""
    if not _is_sqlite(DATABASE_URL):
        return
    try:
        with Engine.begin() as conn:
            _migrate_game_table(conn)
            _migrate_box_score_table(conn)
            _migrate_game_summary_table(conn)
    except Exception as exc:
        print(f"[WARN] Could not align game tables: {exc}")


def _migrate_game_table(conn):
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
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL
        );
        """
    )
    insert_sql = f"""
        INSERT INTO game (
            game_id, game_date, stadium, home_team, away_team,
            away_score, home_score, away_pitcher, home_pitcher,
            winning_team, winning_score, season_id, created_at, updated_at
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
            created_at,
            updated_at
        FROM game_old;
        """
    conn.exec_driver_sql(insert_sql)
    conn.exec_driver_sql("DROP TABLE game_old;")
    conn.exec_driver_sql("PRAGMA foreign_keys=ON;")


def _migrate_box_score_table(conn):
    info_rows = conn.exec_driver_sql("PRAGMA table_info(box_score);").fetchall()
    column_names = {row[1] for row in info_rows}
    removed_cols = {"stadium", "crowd", "start_time", "end_time", "game_time", "away_b", "home_b"}
    needs_migration = bool(column_names & removed_cols)
    if not needs_migration:
        return

    conn.exec_driver_sql("PRAGMA foreign_keys=OFF;")
    conn.exec_driver_sql("ALTER TABLE box_score RENAME TO box_score_old;")
    conn.exec_driver_sql(
        """
        CREATE TABLE box_score (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id VARCHAR(20) NOT NULL UNIQUE,
            away_1 INTEGER, away_2 INTEGER, away_3 INTEGER, away_4 INTEGER, away_5 INTEGER,
            away_6 INTEGER, away_7 INTEGER, away_8 INTEGER, away_9 INTEGER, away_10 INTEGER,
            away_11 INTEGER, away_12 INTEGER, away_13 INTEGER, away_14 INTEGER, away_15 INTEGER,
            home_1 INTEGER, home_2 INTEGER, home_3 INTEGER, home_4 INTEGER, home_5 INTEGER,
            home_6 INTEGER, home_7 INTEGER, home_8 INTEGER, home_9 INTEGER, home_10 INTEGER,
            home_11 INTEGER, home_12 INTEGER, home_13 INTEGER, home_14 INTEGER, home_15 INTEGER,
            away_r INTEGER, away_h INTEGER, away_e INTEGER,
            home_r INTEGER, home_h INTEGER, home_e INTEGER,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            FOREIGN KEY(game_id) REFERENCES game(game_id)
        );
        """
    )
    conn.exec_driver_sql(
        """
        INSERT INTO box_score (
            game_id,
            away_1, away_2, away_3, away_4, away_5,
            away_6, away_7, away_8, away_9, away_10,
            away_11, away_12, away_13, away_14, away_15,
            home_1, home_2, home_3, home_4, home_5,
            home_6, home_7, home_8, home_9, home_10,
            home_11, home_12, home_13, home_14, home_15,
            away_r, away_h, away_e,
            home_r, home_h, home_e,
            created_at, updated_at
        )
        SELECT
            game_id,
            away_1, away_2, away_3, away_4, away_5,
            away_6, away_7, away_8, away_9, away_10,
            away_11, away_12, away_13, away_14, away_15,
            home_1, home_2, home_3, home_4, home_5,
            home_6, home_7, home_8, home_9, home_10,
            home_11, home_12, home_13, home_14, home_15,
            away_r, away_h, away_e,
            home_r, home_h, home_e,
            created_at, updated_at
        FROM box_score_old;
        """
    )
    conn.exec_driver_sql("DROP TABLE box_score_old;")
    conn.exec_driver_sql("PRAGMA foreign_keys=ON;")


def _migrate_game_summary_table(conn):
    info_rows = conn.exec_driver_sql("PRAGMA table_info(game_summary);").fetchall()
    column_names = {row[1] for row in info_rows}
    fk_rows = conn.exec_driver_sql("PRAGMA foreign_key_list(game_summary);").fetchall()
    needs_column_fix = "summary_type" not in column_names or "detail_text" not in column_names
    needs_fk_fix = any(row[2] != "game" for row in fk_rows)
    if not needs_column_fix and not needs_fk_fix:
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
            player_name VARCHAR(50),
            detail_text TEXT,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            FOREIGN KEY(game_id) REFERENCES game (game_id)
        );
        """
    )
    conn.exec_driver_sql(
        f"""
        INSERT INTO game_summary (id, game_id, summary_type, player_name, detail_text, created_at, updated_at)
        SELECT id, game_id, {select_summary}, player_name, {select_detail}, created_at, updated_at
        FROM game_summary_old;
        """
    )
    conn.exec_driver_sql("DROP TABLE game_summary_old;")
    conn.exec_driver_sql("PRAGMA foreign_keys=ON;")

def init_db():
    from src.models.base import Base
    from src.models import team, player, season, game, team_stats, rankings, crawl, award, kbo_embedding
    Base.metadata.create_all(bind=Engine)
    _ensure_player_batting_team_code_column()
    _ensure_player_basic_status_columns()
    _ensure_game_core_tables()
    print(f"[DB] Database initialized: {DATABASE_URL}")
