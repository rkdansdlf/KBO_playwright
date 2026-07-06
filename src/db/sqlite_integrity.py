"""SQLite integrity guard helpers."""

from __future__ import annotations

import logging
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import unquote

logger = logging.getLogger(__name__)

SQLITE_CORRUPTION_PATTERNS = (
    "malformed database schema",
    "invalid rootpage",
    "database disk image is malformed",
    "file is not a database",
)
SQLITE_FAMILY_SUFFIXES = ("", "-wal", "-shm")
DEFAULT_QUARANTINE_ROOT = Path("data/archive/corrupt_sqlite")

IntegrityStatus = Literal[
    "ok",
    "skipped",
    "missing",
    "empty",
    "corrupt",
    "quarantined",
    "quarantine_failed",
]


@dataclass(frozen=True)
class SqliteIntegrityReport:
    """SQLite integrity guard result."""

    database_url: str
    database_path: str | None
    status: IntegrityStatus
    ok: bool
    reason: str
    quick_check: str | None = None
    quarantine_dir: str | None = None
    moved_files: tuple[str, ...] = ()
    error: str | None = None


def is_sqlite_url(database_url: str | None) -> bool:
    """Return whether the URL targets SQLite."""
    return bool(database_url) and database_url.startswith("sqlite")  # type: ignore[union-attr]


def sqlite_path_from_url(database_url: str | None) -> Path | None:
    """Extract a filesystem path from a SQLite database URL."""
    if not is_sqlite_url(database_url):
        return None

    url_without_query = str(database_url).split("?", 1)[0]
    _, separator, raw_path = url_without_query.partition(":///")
    if not separator or raw_path in {"", ":memory:"}:
        return None

    return Path(unquote(raw_path))


def is_sqlite_corruption_error(exc: BaseException) -> bool:
    """Return whether an exception message looks like SQLite file corruption."""
    message = str(exc).lower()
    return any(pattern in message for pattern in SQLITE_CORRUPTION_PATTERNS)


def sqlite_family_paths(database_path: Path) -> tuple[Path, ...]:
    """Return the SQLite database, WAL, and SHM file paths."""
    return tuple(Path(f"{database_path}{suffix}") for suffix in SQLITE_FAMILY_SUFFIXES)


def quarantine_sqlite_family(
    database_path: Path,
    *,
    quarantine_root: Path = DEFAULT_QUARANTINE_ROOT,
) -> tuple[Path, tuple[Path, ...]]:
    """Move a SQLite database file family into a timestamped quarantine directory."""
    from datetime import UTC, datetime

    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
    quarantine_dir = quarantine_root / timestamp
    quarantine_dir.mkdir(parents=True, exist_ok=False)

    moved_files: list[Path] = []
    for path in sqlite_family_paths(database_path):
        if path.exists():
            target = quarantine_dir / path.name
            path.replace(target)
            moved_files.append(target)

    return quarantine_dir, tuple(moved_files)


def check_sqlite_database(
    database_url: str,
    *,
    strict: bool = False,
    action: str | None = None,
    quarantine_root: Path = DEFAULT_QUARANTINE_ROOT,
) -> SqliteIntegrityReport:
    """Check SQLite integrity and optionally quarantine corrupt files."""
    database_path = sqlite_path_from_url(database_url)
    if database_path is None:
        return SqliteIntegrityReport(
            database_url=database_url,
            database_path=None,
            status="skipped",
            ok=True,
            reason="not a file-backed SQLite database",
        )

    if not database_path.exists():
        return SqliteIntegrityReport(
            database_url=database_url,
            database_path=str(database_path),
            status="missing",
            ok=not strict,
            reason="SQLite database file is missing",
        )

    if database_path.stat().st_size == 0:
        return SqliteIntegrityReport(
            database_url=database_url,
            database_path=str(database_path),
            status="empty",
            ok=not strict,
            reason="SQLite database file is empty",
        )

    try:
        quick_check = _run_sqlite_integrity_check(database_path)
    except sqlite3.Error as exc:
        return _corrupt_report(
            database_url,
            database_path,
            action=action,
            quarantine_root=quarantine_root,
            error=str(exc),
        )

    if quick_check != "ok":
        return _corrupt_report(
            database_url,
            database_path,
            action=action,
            quarantine_root=quarantine_root,
            error=quick_check,
        )

    return SqliteIntegrityReport(
        database_url=database_url,
        database_path=str(database_path),
        status="ok",
        ok=True,
        reason="SQLite integrity check passed",
        quick_check=quick_check,
    )


def sqlite_guard_exit_code(report: SqliteIntegrityReport, *, strict: bool = False) -> int:
    """Map an integrity report to a CLI exit code."""
    if report.status == "quarantine_failed":
        return 3
    if report.status == "corrupt":
        return 2
    if strict and report.status in {"missing", "empty"}:
        return 2
    return 0 if report.ok else 2


def default_corrupt_action() -> str:
    """Return the configured corrupt SQLite action."""
    return os.getenv("SQLITE_CORRUPT_ACTION", "none").strip().lower() or "none"


def _run_sqlite_integrity_check(database_path: Path) -> str:
    connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True, timeout=5.0)
    try:
        rows = connection.execute("PRAGMA quick_check").fetchall()
        quick_check = "; ".join(str(row[0]) for row in rows) if rows else "no quick_check rows"
        connection.execute("SELECT name, type, rootpage FROM sqlite_master ORDER BY name LIMIT 1").fetchall()
    finally:
        connection.close()
    return quick_check


def _corrupt_report(
    database_url: str,
    database_path: Path,
    *,
    action: str | None,
    quarantine_root: Path,
    error: str,
) -> SqliteIntegrityReport:
    normalized_action = (action or default_corrupt_action()).strip().lower()
    if normalized_action != "quarantine":
        return SqliteIntegrityReport(
            database_url=database_url,
            database_path=str(database_path),
            status="corrupt",
            ok=False,
            reason="SQLite database failed integrity checks",
            quick_check=error,
            error=error,
        )

    try:
        quarantine_dir, moved_files = quarantine_sqlite_family(database_path, quarantine_root=quarantine_root)
    except OSError as exc:
        logger.exception("Failed to quarantine corrupt SQLite database")
        return SqliteIntegrityReport(
            database_url=database_url,
            database_path=str(database_path),
            status="quarantine_failed",
            ok=False,
            reason="SQLite database is corrupt and quarantine failed",
            quick_check=error,
            error=str(exc),
        )

    return SqliteIntegrityReport(
        database_url=database_url,
        database_path=str(database_path),
        status="quarantined",
        ok=True,
        reason="SQLite database was corrupt and has been quarantined",
        quick_check=error,
        quarantine_dir=str(quarantine_dir),
        moved_files=tuple(str(path) for path in moved_files),
        error=error,
    )
