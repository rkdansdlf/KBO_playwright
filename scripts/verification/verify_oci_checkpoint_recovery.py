"""Verify Oracle sync checkpoint recovery and repeated MERGE idempotency."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlsplit

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from src.db.engine import create_engine_for_url
from src.sync.checkpoint import SyncCheckpoint
from src.sync.table_dag import SyncStrategy, TableMeta
from src.cli.sync_sqlite_to_oci import SqliteToOciSynchronizer, SyncOptions

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from src.cli.sync_sqlite_to_oci import TableSyncResult

TEST_TABLE = "E2E_CDC_CHECKPOINT_GAME"
TEST_META = TableMeta(
    TEST_TABLE,
    level=0,
    strategy=SyncStrategy.INCREMENTAL,
    timestamp_col="updated_at",
    natural_keys=["game_id"],
)
logger = logging.getLogger("verify_oci_checkpoint_recovery")


@dataclass(frozen=True, slots=True)
class CheckpointRecoveryReport:
    """Result of the isolated checkpoint recovery verification."""

    timestamp: str
    database_user: str
    initial_synced_count: int
    resumed_synced_count: int
    repeated_synced_count: int
    source_checkpoint_recovered: bool
    target_row_count: int
    duplicate_count: int
    cleanup_success: bool
    elapsed_seconds: float
    status: str
    details: dict[str, Any]


def _resolve_target_url() -> str | None:
    """Build the Oracle URL using the configured application account."""
    db_url = os.getenv("ORACLE_TARGET_URL") or os.getenv("OCI_DB_URL") or os.getenv("DATABASE_URL")
    if not db_url or not db_url.startswith("oracle"):
        return db_url
    user = os.getenv("ORACLE_APP_USER")
    password = os.getenv("ORACLE_APP_PASSWORD")
    if not user or not password:
        return db_url
    parsed = urlsplit(db_url)
    dsn = parsed.netloc.rsplit("@", 1)[-1]
    return f"{parsed.scheme}://{quote(user, safe='')}:{quote(password, safe='')}@{dsn}"


def _create_source(source_path: Path) -> None:
    """Create an isolated SQLite source with three initial rows."""
    with sqlite3.connect(source_path) as connection:
        connection.execute(
            """
            CREATE TABLE E2E_CDC_CHECKPOINT_GAME (
                game_id TEXT PRIMARY KEY,
                game_date TEXT,
                away_team TEXT,
                home_team TEXT,
                away_score INTEGER,
                home_score INTEGER,
                game_status TEXT,
                updated_at TEXT
            )
            """,
        )
        connection.executemany(
            "INSERT INTO E2E_CDC_CHECKPOINT_GAME VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("20990101TEST0", "2099-01-01", "KIA", "LG", 1, 2, "FINAL", "2026-01-01 00:00:00"),
                ("20990102TEST0", "2099-01-02", "SS", "LT", 3, 4, "FINAL", "2026-01-01 00:01:00"),
                ("20990103TEST0", "2099-01-03", "DB", "HH", 5, 6, "FINAL", "2026-01-01 00:02:00"),
            ],
        )


def _insert_restart_row(source_path: Path) -> None:
    """Insert a source row after the simulated process interruption."""
    with sqlite3.connect(source_path) as connection:
        connection.execute(
            "INSERT INTO E2E_CDC_CHECKPOINT_GAME VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("20990104TEST0", "2099-01-04", "NC", "KH", 7, 8, "FINAL", "2099-01-01 00:00:00"),
        )


def _prepare_target(engine: Engine) -> None:
    """Create the isolated Oracle target table, replacing any stale copy."""
    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE E2E_CDC_CHECKPOINT_GAME';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
            """,
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE E2E_CDC_CHECKPOINT_GAME (
                game_id VARCHAR2(20) PRIMARY KEY,
                game_date DATE,
                away_team VARCHAR2(10),
                home_team VARCHAR2(10),
                away_score NUMBER(4),
                home_score NUMBER(4),
                game_status VARCHAR2(20),
                updated_at TIMESTAMP
            )
            """,
        )


def _cleanup_target(engine: Engine) -> bool:
    """Drop the isolated Oracle target table."""
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                """
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE E2E_CDC_CHECKPOINT_GAME';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN RAISE; END IF;
                END;
                """,
            )
    except Exception:
        logger.exception("Failed to clean up checkpoint verification table")
        return False
    return True


def _sync_once(
    source_path: Path,
    target_url: str,
    tns_admin: str,
    wallet_password: str | None,
    mode: str,
) -> tuple[TableSyncResult, SyncCheckpoint | None]:
    """Run one isolated sync process and return its result and checkpoint."""
    synchronizer = SqliteToOciSynchronizer(
        sqlite_path=str(source_path),
        oci_url=target_url,
        tns_admin=tns_admin,
        wallet_password=wallet_password,
        options=SyncOptions(apply_changes=True, batch_size=2, commit_every=2, concurrency=1),
    )
    try:
        writer = synchronizer.get_writer()
        result = synchronizer.sync_single_table(TEST_META, mode, None, None, writer)
        checkpoint = synchronizer.checkpoint_mgr.get_checkpoint(TEST_TABLE)
        return result, checkpoint
    finally:
        synchronizer.close()


def _record_simulated_failure(
    source_path: Path,
    target_url: str,
    tns_admin: str,
    wallet_password: str | None,
) -> SyncCheckpoint | None:
    """Persist a failure marker as if the sync process had been interrupted."""
    synchronizer = SqliteToOciSynchronizer(
        sqlite_path=str(source_path),
        oci_url=target_url,
        tns_admin=tns_admin,
        wallet_password=wallet_password,
        options=SyncOptions(apply_changes=True, concurrency=1),
    )
    try:
        synchronizer.checkpoint_mgr.record_failure(TEST_TABLE, "simulated process interruption")
        return synchronizer.checkpoint_mgr.get_checkpoint(TEST_TABLE)
    finally:
        synchronizer.close()


def _target_counts(engine: Engine) -> tuple[int, int]:
    """Return total and distinct target row counts."""
    with engine.connect() as connection:
        total, distinct = connection.execute(
            text("SELECT COUNT(*), COUNT(DISTINCT game_id) FROM E2E_CDC_CHECKPOINT_GAME"),
        ).one()
    return int(total), int(total - distinct)


def _validate_database_user(database_user: str) -> None:
    """Require the configured application account for the verification."""
    expected_user = os.getenv("ORACLE_APP_USER")
    if expected_user and database_user.upper() != expected_user.upper():
        message = f"Expected {expected_user}, found {database_user}"
        raise RuntimeError(message)


def run_verification() -> CheckpointRecoveryReport:
    """Run isolated Oracle checkpoint, restart, and idempotency checks."""
    started = time.perf_counter()
    timestamp = datetime.now(UTC).isoformat()
    target_url = _resolve_target_url()
    tns_admin = os.getenv("TNS_ADMIN")
    wallet_password = os.getenv("OCI_WALLET_PASSWORD")
    if not target_url or not target_url.startswith("oracle") or not tns_admin:
        report = CheckpointRecoveryReport(
            timestamp=timestamp,
            database_user="",
            initial_synced_count=0,
            resumed_synced_count=0,
            repeated_synced_count=0,
            source_checkpoint_recovered=False,
            target_row_count=0,
            duplicate_count=0,
            cleanup_success=True,
            elapsed_seconds=round(time.perf_counter() - started, 3),
            status="SKIPPED_NO_ORACLE_CONFIG",
            details={"message": "Oracle URL and TNS_ADMIN are required."},
        )

    engine = create_engine_for_url(target_url, tns_admin=tns_admin, wallet_password=wallet_password)
    database_user = ""
    cleanup_success = False
    try:
        with engine.connect() as connection:
            database_user = str(connection.execute(text("SELECT USER FROM DUAL")).scalar())
        _validate_database_user(database_user)

        _prepare_target(engine)
        with tempfile.TemporaryDirectory(prefix="kbo_checkpoint_") as temp_dir:
            source_path = Path(temp_dir) / "source.sqlite"
            _create_source(source_path)
            initial_result, initial_checkpoint = _sync_once(
                source_path,
                target_url,
                tns_admin,
                wallet_password,
                "full",
            )
            failed_checkpoint = _record_simulated_failure(source_path, target_url, tns_admin, wallet_password)
            _insert_restart_row(source_path)
            resumed_result, resumed_checkpoint = _sync_once(
                source_path,
                target_url,
                tns_admin,
                wallet_password,
                "incremental",
            )
            repeated_result, _ = _sync_once(source_path, target_url, tns_admin, wallet_password, "incremental")
            target_count, duplicate_count = _target_counts(engine)

        checkpoint_recovered = bool(
            initial_checkpoint
            and failed_checkpoint
            and resumed_checkpoint
            and initial_checkpoint.last_status == "SUCCESS"
            and failed_checkpoint.last_status.startswith("FAILED")
            and resumed_checkpoint.last_status == "SUCCESS"
            and resumed_checkpoint.rows_synced >= initial_checkpoint.rows_synced + 1
        )
        status = (
            "VERIFIED_COMPLETE" if checkpoint_recovered and duplicate_count == 0 and target_count == 4 else "FAILED"
        )
        report = CheckpointRecoveryReport(
            timestamp=timestamp,
            database_user=database_user,
            initial_synced_count=initial_result.synced_count,
            resumed_synced_count=resumed_result.synced_count,
            repeated_synced_count=repeated_result.synced_count,
            source_checkpoint_recovered=checkpoint_recovered,
            target_row_count=target_count,
            duplicate_count=duplicate_count,
            cleanup_success=False,
            elapsed_seconds=round(time.perf_counter() - started, 3),
            status=status,
            details={
                "initial_status": initial_result.status,
                "resumed_status": resumed_result.status,
                "repeated_status": repeated_result.status,
            },
        )
    except Exception as exc:
        logger.exception("Checkpoint recovery verification failed")
        return CheckpointRecoveryReport(
            timestamp=timestamp,
            database_user=database_user,
            initial_synced_count=0,
            resumed_synced_count=0,
            repeated_synced_count=0,
            source_checkpoint_recovered=False,
            target_row_count=0,
            duplicate_count=0,
            cleanup_success=False,
            elapsed_seconds=round(time.perf_counter() - started, 3),
            status="FAILED",
            details={"error": str(exc)},
        )
    finally:
        cleanup_success = _cleanup_target(engine)
        engine.dispose()
        logger.info("Checkpoint verification cleanup_success=%s", cleanup_success)
    return replace(
        report,
        cleanup_success=cleanup_success,
        elapsed_seconds=round(time.perf_counter() - started, 3),
    )


def main() -> int:
    """Run the checkpoint recovery verification CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Output JSON")
    args = parser.parse_args()
    report = run_verification()
    if args.json:
        sys.stdout.write(json.dumps(asdict(report), ensure_ascii=False) + "\n")
    else:
        logger.info("Checkpoint recovery verification: %s", report.status)
    return 0 if report.status in {"VERIFIED_COMPLETE", "SKIPPED_NO_ORACLE_CONFIG"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
