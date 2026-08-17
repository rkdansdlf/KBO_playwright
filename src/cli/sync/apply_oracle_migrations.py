"""Bootstrap and verify the Oracle Autonomous Database schema."""

from __future__ import annotations

import argparse
import logging
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import DATABASE_URL, create_engine_for_url

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)

MIGRATION_DIR = Path(__file__).resolve().parents[3] / "migrations" / "oracle"
MIGRATION_TABLE = "schema_migrations"
ORM_BASELINE_VERSION = "000_orm_baseline"
MIGRATION_NAME_RE = re.compile(r"^(\d+)_.*\.sql$")
ORM_BASELINE_TABLES = ("game", "kbo_seasons", "player_basic", "rag_chunks")
ORACLE_OBJECT_ALREADY_EXISTS = 955
SAFETY_GATED_MIGRATIONS = frozenset(
    {
        "024_deletion_anomaly_integrity.sql",
        "024_game_stat_partial_unique_indexes.sql",
        "025_player_movement_position_backfill.sql",
        "026_player_movement_profile_mirror_backfill.sql",
        "027_player_movement_roster_backfill.sql",
        "028_player_movement_franchise_history_backfill.sql",
        "032_fix_team_season_fielding_float_columns.sql",
    },
)


def _migration_paths(
    directory: Path = MIGRATION_DIR,
    *,
    include_safety_gated: bool = False,
) -> list[Path]:
    """Return Oracle migration files ordered by numeric version."""
    paths = sorted(
        (path for path in directory.glob("*.sql") if MIGRATION_NAME_RE.match(path.name)),
        key=lambda path: (int(MIGRATION_NAME_RE.match(path.name).group(1)), path.name),  # type: ignore[union-attr]
    )
    if include_safety_gated:
        return paths
    return [path for path in paths if path.name not in SAFETY_GATED_MIGRATIONS]


def _is_already_exists_error(exc: SQLAlchemyError) -> bool:
    """Return whether an Oracle error means that an object already exists."""
    original = getattr(exc, "orig", None)
    code = getattr(original, "code", None)
    if code is None:
        return False
    try:
        return int(code) == ORACLE_OBJECT_ALREADY_EXISTS
    except (TypeError, ValueError):
        return False


def _ensure_tracking_table(connection: Connection) -> None:
    """Create the Oracle migration tracking table when it is missing."""
    try:
        connection.exec_driver_sql(
            "CREATE TABLE schema_migrations ("
            "version VARCHAR2(128) PRIMARY KEY, "
            "applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL"
            ")",
        )
    except SQLAlchemyError as exc:
        if not _is_already_exists_error(exc):
            raise


def _tracking_table_exists(connection: Connection) -> bool:
    """Return whether the migration tracking table exists for the current user."""
    return bool(
        connection.execute(
            text("SELECT 1 FROM user_tables WHERE table_name = :table_name"),
            {"table_name": MIGRATION_TABLE.upper()},
        ).first(),
    )


def _applied_versions(connection: Connection) -> set[str]:
    """Return migration versions already recorded in Oracle."""
    return {str(row[0]) for row in connection.execute(text("SELECT version FROM schema_migrations"))}


def _ensure_orm_baseline(connection: Connection) -> None:
    """Require the core ORM tables before applying incremental migrations."""
    inspector = inspect(connection)
    missing = [table for table in ORM_BASELINE_TABLES if not inspector.has_table(table)]
    if missing:
        missing_tables = ", ".join(missing)
        message = f"Oracle ORM baseline is incomplete; missing tables: {missing_tables}"
        raise RuntimeError(message)


def _bootstrap_orm_schema(engine: Engine) -> None:
    """Create the current SQLAlchemy baseline and its derived view."""
    import src.models  # noqa: F401
    from src.db.engine import _ensure_stat_recalc_view
    from src.models.base import Base

    Base.metadata.create_all(bind=engine)
    _ensure_stat_recalc_view(engine)


def _record_version(connection: Connection, version: str) -> None:
    """Record a migration version if it is not already present."""
    connection.execute(
        text("INSERT INTO schema_migrations (version) VALUES (:version)"),
        {"version": version},
    )


def _execute_migration(connection: Connection, path: Path) -> None:
    """Execute one Oracle migration file, supporting SQL and SQL*Plus blocks."""
    content = path.read_text(encoding="utf-8")
    blocks = re.split(r"(?m)^\s*/\s*$", content)
    for block in blocks:
        statement = block.strip()
        if not statement:
            continue
        if statement.endswith(";"):
            statement = statement[:-1].rstrip()
        connection.exec_driver_sql(statement)


def apply_migrations(
    engine: Engine,
    *,
    check: bool = False,
    include_safety_gated: bool = False,
) -> list[str]:
    """Apply Oracle migrations or return pending versions in read-only mode."""
    paths = _migration_paths(include_safety_gated=include_safety_gated)
    versions = [ORM_BASELINE_VERSION, *(path.name for path in paths)]

    with engine.connect() as connection:
        _ensure_orm_baseline(connection)
        if check:
            if not _tracking_table_exists(connection):
                return versions
            applied = _applied_versions(connection)
            return [version for version in versions if version not in applied]

    with engine.begin() as connection:
        tracking_exists = _tracking_table_exists(connection)
        if not tracking_exists:
            _ensure_tracking_table(connection)
            applied: set[str] = set()
        else:
            applied = _applied_versions(connection)

        if ORM_BASELINE_VERSION not in applied:
            _record_version(connection, ORM_BASELINE_VERSION)
        pending_paths = [path for path in paths if path.name not in applied]
        for path in pending_paths:
            _execute_migration(connection, path)
            _record_version(connection, path.name)
    baseline_applied = ORM_BASELINE_VERSION not in applied
    return (
        [ORM_BASELINE_VERSION, *(path.name for path in pending_paths)]
        if baseline_applied
        else [path.name for path in pending_paths]
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Bootstrap or verify an Oracle Autonomous Database schema."""
    parser = argparse.ArgumentParser(description="Apply Oracle Autonomous Database schema migrations")
    parser.add_argument("--url", help="Override DATABASE_URL with an Oracle URL")
    parser.add_argument("--check", action="store_true", help="Return non-zero when migrations are pending")
    parser.add_argument(
        "--include-safety-gated",
        action="store_true",
        help="Include data-rewrite migrations that require separate review",
    )
    args = parser.parse_args(argv)

    configured_url = os.getenv("DATABASE_URL")
    url = args.url or os.getenv("ORACLE_TARGET_URL") or os.getenv("OCI_DB_URL")
    if not url and configured_url and configured_url.startswith("oracle"):
        url = configured_url
    if not url and DATABASE_URL.startswith("oracle"):
        url = DATABASE_URL
    if not url or not url.startswith("oracle"):
        parser.error("DATABASE_URL must be an Oracle URL (oracle+oracledb://...)")

    engine = create_engine_for_url(url)
    try:
        if not args.check:
            _bootstrap_orm_schema(engine)
        pending = apply_migrations(
            engine,
            check=args.check,
            include_safety_gated=args.include_safety_gated,
        )
    except (SQLAlchemyError, RuntimeError, OSError):
        logger.exception("Oracle migration failed")
        return 1
    finally:
        engine.dispose()

    if pending:
        logger.info("Oracle migrations pending/applied: %s", pending)
    return 1 if args.check and pending else 0


if __name__ == "__main__":
    raise SystemExit(main())
