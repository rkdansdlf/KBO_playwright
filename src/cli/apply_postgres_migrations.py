"""Bootstrap and verify the PostgreSQL schema and incremental migrations."""

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
MIGRATION_DIR = Path(__file__).resolve().parents[2] / "migrations" / "postgresql"
MIGRATION_TABLE = "schema_migrations"
MIGRATION_NAME_RE = re.compile(r"^(\d+)_.*\.sql$")
ORM_BASELINE_TABLES = ("game", "kbo_seasons")


def _migration_paths(directory: Path = MIGRATION_DIR) -> list[Path]:
    """Return PostgreSQL migration files ordered by numeric version."""
    return sorted(
        (path for path in directory.glob("*.sql") if MIGRATION_NAME_RE.match(path.name)),
        key=lambda path: int(MIGRATION_NAME_RE.match(path.name).group(1)),  # type: ignore[union-attr]
    )


def _ensure_tracking_table(connection: Connection) -> None:
    """Create the migration tracking table when applying migrations."""
    connection.execute(
        text(
            "CREATE TABLE IF NOT EXISTS schema_migrations "
            "(version VARCHAR(128) PRIMARY KEY, applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        ),
    )


def _ensure_orm_baseline(connection: Connection) -> None:
    """Require the ORM-created baseline schema before incremental migrations."""
    inspector = inspect(connection)
    missing = [table for table in ORM_BASELINE_TABLES if not inspector.has_table(table)]
    if missing:
        msg = (
            "PostgreSQL migrations require the ORM baseline schema; "
            f"missing tables: {', '.join(missing)}. Run init_db() first."
        )
        raise RuntimeError(msg)


def _bootstrap_orm_schema(engine: Engine) -> None:
    """Create the SQLAlchemy baseline for a new PostgreSQL database."""
    import src.models  # noqa: F401
    from src.db.engine import _ensure_stat_recalc_view
    from src.models.base import Base

    Base.metadata.create_all(bind=engine)
    _ensure_stat_recalc_view(engine)


def _tracking_table_exists(connection: Connection) -> bool:
    return inspect(connection).has_table(MIGRATION_TABLE)


def _applied_versions(connection: Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(text(f"SELECT version FROM {MIGRATION_TABLE}"))  # noqa: S608
    }


def apply_migrations(engine: Engine, *, directory: Path = MIGRATION_DIR, check: bool = False) -> list[str]:
    """Apply incremental PostgreSQL migrations or return pending versions."""
    if engine.dialect.name == "oracle":
        msg = "PostgreSQL migrations do not support Oracle engines"
        raise RuntimeError(msg)
    paths = _migration_paths(directory)

    if check:
        with engine.connect() as connection:
            _ensure_orm_baseline(connection)
            if not _tracking_table_exists(connection):
                return [path.name for path in paths]
            applied = _applied_versions(connection)
            return [path.name for path in paths if path.name not in applied]

    with engine.begin() as connection:
        _ensure_orm_baseline(connection)
        _ensure_tracking_table(connection)
        applied = _applied_versions(connection)
        pending = [path for path in paths if path.name not in applied]
        for path in pending:
            for statement in path.read_text(encoding="utf-8").split(";"):
                sql = statement.strip()
                if sql:
                    connection.exec_driver_sql(sql)
            connection.execute(
                text(f"INSERT INTO {MIGRATION_TABLE} (version) VALUES (:version)"),  # noqa: S608
                {"version": path.name},
            )
    return [path.name for path in pending]


def main(argv: Sequence[str] | None = None) -> int:
    """Apply PostgreSQL incremental migrations or check pending versions."""
    parser = argparse.ArgumentParser(description="Apply PostgreSQL incremental migrations")
    parser.add_argument("--url", help="Override DATABASE_URL")
    parser.add_argument("--check", action="store_true", help="Return non-zero when migrations are pending")
    args = parser.parse_args(argv)
    url = args.url or os.getenv("DATABASE_URL") or DATABASE_URL
    if not url:
        msg = "DATABASE_URL is required"
        raise SystemExit(msg)
    engine = create_engine_for_url(url)
    try:
        if not args.check:
            _bootstrap_orm_schema(engine)
        pending = apply_migrations(engine, check=args.check)
    except (SQLAlchemyError, RuntimeError, OSError):
        logger.exception("PostgreSQL migration failed")
        return 1
    finally:
        engine.dispose()
    if pending:
        logger.info("PostgreSQL migrations pending/applied: %s", pending)
    return 1 if args.check and pending else 0


if __name__ == "__main__":
    raise SystemExit(main())
