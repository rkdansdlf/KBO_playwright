"""Multi-engine SQL migration runner and DDL versioning orchestrator."""

from __future__ import annotations

import hashlib
import logging
import re
import time
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from src.db.dto import (
    MigrationDialect,
    MigrationExecutionResult,
    MigrationFileMeta,
    MigrationStatusReport,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)

MIGRATIONS_ROOT = Path(__file__).resolve().parents[2] / "migrations"
MIGRATION_TABLE = "schema_migrations"
MIGRATION_NAME_RE = re.compile(r"^(\d+)_.*\.sql$")
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


def _split_sql_statements(sql_text: str) -> list[str]:
    """Split SQL migration text into individual executable statements."""
    # Check explicit statement separator first
    if "-- Statement Separator" in sql_text:
        raw_parts = sql_text.split("-- Statement Separator")
    elif "\n/\n" in sql_text:
        raw_parts = sql_text.split("\n/\n")
    else:
        raw_parts = sql_text.split(";")

    statements: list[str] = []
    for part in raw_parts:
        cleaned = part.strip()
        if not cleaned:
            continue
        # Remove trailing slash if present
        if cleaned.endswith("/"):
            cleaned = cleaned[:-1].strip()
        # Filter out comments-only blocks
        lines = [line for line in cleaned.split("\n") if not line.strip().startswith("--")]
        if "".join(lines).strip():
            statements.append(cleaned)
    return statements


class MigrationEngine:
    """Orchestrates SQL schema migrations across Oracle, SQLite, and PostgreSQL."""

    def __init__(self, migrations_root: Path | None = None) -> None:
        """Initialize migration engine with migrations directory."""
        self.migrations_root = migrations_root or MIGRATIONS_ROOT

    def get_dialect_dir(self, dialect: MigrationDialect | str) -> Path:
        """Return the directory containing migration files for a dialect."""
        val = dialect.value if isinstance(dialect, MigrationDialect) else str(dialect).lower()
        return self.migrations_root / val

    def get_available_migrations(
        self,
        dialect: MigrationDialect | str,
        *,
        include_safety_gated: bool = False,
    ) -> list[MigrationFileMeta]:
        """Discover and return all valid versioned migration files for a dialect."""
        target_dir = self.get_dialect_dir(dialect)
        if not target_dir.exists():
            return []

        migrations: list[MigrationFileMeta] = []
        d_enum = MigrationDialect(dialect) if isinstance(dialect, str) else dialect

        for path in target_dir.glob("*.sql"):
            match = MIGRATION_NAME_RE.match(path.name)
            if not match:
                continue
            version = int(match.group(1))
            is_gated = path.name in SAFETY_GATED_MIGRATIONS
            if is_gated and not include_safety_gated:
                continue

            content = path.read_bytes()
            checksum = hashlib.sha256(content).hexdigest()[:16]

            migrations.append(
                MigrationFileMeta(
                    version=version,
                    filename=path.name,
                    path=str(path),
                    dialect=d_enum,
                    is_safety_gated=is_gated,
                    checksum=checksum,
                )
            )

        migrations.sort(key=lambda m: (m.version, m.filename))
        return migrations

    def ensure_schema_migrations_table(self, connection: Connection) -> None:
        """Ensure schema_migrations table exists for tracking applied migrations."""
        dialect_name = connection.dialect.name.lower()
        inspector = inspect(connection)

        table_names = [t.lower() for t in inspector.get_table_names()]
        if MIGRATION_TABLE.lower() in table_names:
            return

        if "oracle" in dialect_name:
            ddl = f"""
            CREATE TABLE {MIGRATION_TABLE} (
                version NUMBER(10) PRIMARY KEY,
                filename VARCHAR2(255) NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
            )
            """
        else:
            ddl = f"""
            CREATE TABLE {MIGRATION_TABLE} (
                version INTEGER PRIMARY KEY,
                filename VARCHAR(255) NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
            )
            """

        connection.execute(text(ddl))
        connection.commit()

    def get_applied_versions(self, connection: Connection) -> set[int]:
        """Fetch all migration version numbers that have been applied."""
        self.ensure_schema_migrations_table(connection)
        stmt = text(f"SELECT version FROM {MIGRATION_TABLE}")  # noqa: S608
        rows = connection.execute(stmt).fetchall()
        return {int(row[0]) for row in rows}

    def record_applied_migration(self, connection: Connection, meta: MigrationFileMeta) -> None:
        """Record an applied migration version into schema_migrations."""
        stmt = text(
            f"INSERT INTO {MIGRATION_TABLE} (version, filename) VALUES (:version, :filename)"  # noqa: S608
        )
        connection.execute(stmt, {"version": meta.version, "filename": meta.filename})

    def get_status(
        self,
        engine: Engine,
        dialect: MigrationDialect | str,
        *,
        include_safety_gated: bool = False,
    ) -> MigrationStatusReport:
        """Inspect and return current migration status report."""
        available = self.get_available_migrations(dialect, include_safety_gated=include_safety_gated)
        d_val = dialect.value if isinstance(dialect, MigrationDialect) else str(dialect).lower()

        with engine.connect() as conn:
            applied_set = self.get_applied_versions(conn)

        applied_versions = sorted([m.version for m in available if m.version in applied_set])
        pending_versions = sorted([m.version for m in available if m.version not in applied_set])

        return MigrationStatusReport(
            dialect=d_val,
            total_available=len(available),
            applied_count=len(applied_versions),
            pending_count=len(pending_versions),
            applied_versions=applied_versions,
            pending_versions=pending_versions,
        )

    def apply_migrations(
        self,
        engine: Engine,
        dialect: MigrationDialect | str,
        *,
        dry_run: bool = False,
        include_safety_gated: bool = False,
    ) -> MigrationStatusReport:
        """Apply all pending migrations for the specified dialect."""
        available = self.get_available_migrations(dialect, include_safety_gated=include_safety_gated)
        d_val = dialect.value if isinstance(dialect, MigrationDialect) else str(dialect).lower()
        results: list[MigrationExecutionResult] = []

        with engine.connect() as conn:
            applied_set = self.get_applied_versions(conn)

            for mig in available:
                if mig.version in applied_set:
                    results.append(
                        MigrationExecutionResult(
                            filename=mig.filename,
                            version=mig.version,
                            status="SKIPPED",
                        )
                    )
                    continue

                if dry_run:
                    logger.info("[DRY RUN] Would apply migration: %s (v%d)", mig.filename, mig.version)
                    results.append(
                        MigrationExecutionResult(
                            filename=mig.filename,
                            version=mig.version,
                            status="PENDING_DRY_RUN",
                        )
                    )
                    continue

                logger.info("Applying migration: %s (v%d)...", mig.filename, mig.version)
                start_mono = time.monotonic()
                try:
                    sql_content = Path(mig.path).read_text(encoding="utf-8")
                    statements = _split_sql_statements(sql_content)

                    for stmt_text in statements:
                        conn.execute(text(stmt_text))

                    self.record_applied_migration(conn, mig)
                    conn.commit()
                    duration = time.monotonic() - start_mono
                    logger.info("Applied migration '%s' successfully in %.3fs", mig.filename, duration)
                    results.append(
                        MigrationExecutionResult(
                            filename=mig.filename,
                            version=mig.version,
                            status="APPLIED",
                            duration_seconds=duration,
                        )
                    )
                    applied_set.add(mig.version)
                except SQLAlchemyError:
                    conn.rollback()
                    duration = time.monotonic() - start_mono
                    logger.exception("Failed to apply migration '%s'", mig.filename)
                    results.append(
                        MigrationExecutionResult(
                            filename=mig.filename,
                            version=mig.version,
                            status="FAILED",
                            duration_seconds=duration,
                            error_message=f"Failed to apply {mig.filename}",
                        )
                    )
                    break

        applied_versions = sorted([m.version for m in available if m.version in applied_set])
        pending_versions = sorted([m.version for m in available if m.version not in applied_set])

        return MigrationStatusReport(
            dialect=d_val,
            total_available=len(available),
            applied_count=len(applied_versions),
            pending_count=len(pending_versions),
            applied_versions=applied_versions,
            pending_versions=pending_versions,
            results=results,
        )
