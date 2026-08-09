"""SQLite WAL checkpoint and optional VACUUM utility.

Usage:
    python scripts/maintenance/wal_checkpoint.py [--vacuum] [--db-path PATH]

Performs PRAGMA wal_checkpoint(TRUNCATE) to merge the WAL file back into
the main database, recovering disk space and improving read performance.
Optionally runs VACUUM to fully defragment the database.
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DEFAULT_DB_PATH = Path("data/kbo_dev.db")


def _resolve_db_path(db_path: str | None = None) -> Path:
    """Resolve database file path."""
    if db_path:
        return Path(db_path)
    env_url = os.getenv("DATABASE_URL", "")
    if env_url.startswith("sqlite:///"):
        return Path(env_url.replace("sqlite:///", "", 1))
    return DEFAULT_DB_PATH


def run_wal_checkpoint(db_path: Path, *, vacuum: bool = False) -> dict[str, object]:
    """Execute WAL checkpoint and optional VACUUM.

    Args:
        db_path: Path to the SQLite database file.
        vacuum: If True, run VACUUM after checkpoint.

    Returns:
        Dict with checkpoint results and file size changes.

    """
    if not db_path.exists():
        logger.error("Database file not found: %s", db_path)
        return {"error": f"Database not found: {db_path}"}

    wal_path = Path(f"{db_path}-wal")

    # Pre-checkpoint sizes
    db_size_before = db_path.stat().st_size
    wal_size_before = wal_path.stat().st_size if wal_path.exists() else 0

    logger.info(
        "Before checkpoint: DB=%.1f MB, WAL=%.1f MB",
        db_size_before / 1024 / 1024,
        wal_size_before / 1024 / 1024,
    )

    conn = sqlite3.connect(str(db_path), timeout=300)
    try:
        # Run WAL checkpoint (TRUNCATE mode: checkpoint + truncate WAL to zero)
        t0 = time.monotonic()
        result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE);").fetchone()
        checkpoint_ms = (time.monotonic() - t0) * 1000

        # result = (blocked, pages_modified, pages_moved)
        blocked, pages_modified, pages_moved = result or (0, 0, 0)
        logger.info(
            "WAL checkpoint completed in %.0f ms: blocked=%s, modified=%s, moved=%s",
            checkpoint_ms,
            blocked,
            pages_modified,
            pages_moved,
        )

        if vacuum:
            logger.info("Running VACUUM (this may take several minutes for large databases)...")
            t0 = time.monotonic()
            conn.execute("VACUUM;")
            vacuum_ms = (time.monotonic() - t0) * 1000
            logger.info("VACUUM completed in %.0f ms", vacuum_ms)
        else:
            vacuum_ms = 0
    finally:
        conn.close()

    # Post-checkpoint sizes
    db_size_after = db_path.stat().st_size
    wal_size_after = wal_path.stat().st_size if wal_path.exists() else 0

    saved_bytes = (db_size_before + wal_size_before) - (db_size_after + wal_size_after)

    logger.info(
        "After checkpoint: DB=%.1f MB, WAL=%.1f MB (saved %.1f MB)",
        db_size_after / 1024 / 1024,
        wal_size_after / 1024 / 1024,
        saved_bytes / 1024 / 1024,
    )

    return {
        "db_path": str(db_path),
        "db_size_before_mb": round(db_size_before / 1024 / 1024, 1),
        "wal_size_before_mb": round(wal_size_before / 1024 / 1024, 1),
        "db_size_after_mb": round(db_size_after / 1024 / 1024, 1),
        "wal_size_after_mb": round(wal_size_after / 1024 / 1024, 1),
        "saved_mb": round(saved_bytes / 1024 / 1024, 1),
        "checkpoint_ms": round(checkpoint_ms),
        "vacuum_ms": round(vacuum_ms) if vacuum else None,
        "blocked": blocked,
        "pages_modified": pages_modified,
        "pages_moved": pages_moved,
    }


def main() -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="SQLite WAL checkpoint utility")
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Path to SQLite database (default: data/kbo_dev.db or DATABASE_URL env)",
    )
    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Run VACUUM after checkpoint (rebuilds entire DB, slow for large files)",
    )
    args = parser.parse_args()

    db_path = _resolve_db_path(args.db_path)
    result = run_wal_checkpoint(db_path, vacuum=args.vacuum)

    if "error" in result:
        sys.exit(1)

    import json

    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
