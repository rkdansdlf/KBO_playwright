"""SQLite Automated Backup & Integrity Check Maintenance Utility.

Features:
  1. Performs safe online backup using `VACUUM INTO 'data/backups/kbo_dev_YYYYMMDD_HHMMSS.db'`
  2. Runs `PRAGMA quick_check;` on the newly created backup database to verify integrity.
  3. Automatically cleans up old backups, keeping only the N most recent backups (default: 7).

Usage:
    python -m scripts.maintenance.backup_db
    python -m scripts.maintenance.backup_db --keep-count 10 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

# Ensure project root is in sys.path
_PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.constants import KST

logger = logging.getLogger("backup_db")


def run_backup(
    *, db_path: str = "data/kbo_dev.db", backup_dir: str = "data/backups", keep_count: int = 7, dry_run: bool = False
) -> str | None:
    """Run SQLite online backup and integrity check."""
    src = Path(db_path).resolve()
    if not src.exists():
        logger.error("Source database %s does not exist.", db_path)
        return None

    out_dir = Path(backup_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    target = out_dir / f"kbo_dev_{timestamp}.db"

    if dry_run:
        logger.info("[DRY-RUN] Would create backup at: %s", target)
        logger.info("[DRY-RUN] Would perform PRAGMA quick_check on %s", target)
        return str(target)

    logger.info("📦 Initiating SQLite online backup to: %s", target)
    try:
        if target.exists():
            target.unlink()

        src_conn = sqlite3.connect(str(src), timeout=30.0)
        src_conn.execute(f"VACUUM INTO '{target.as_posix()}';")
        src_conn.close()

        size_mb = target.stat().st_size / (1024 * 1024)
        logger.info("✅ Backup created successfully (Size: %.2f MB)", size_mb)

        # Integrity Check
        logger.info("🔍 Running PRAGMA quick_check on backup...")
        check_conn = sqlite3.connect(target)
        cursor = check_conn.cursor()
        res = cursor.execute("PRAGMA quick_check;").fetchone()
        check_conn.close()

        if res and res[0] == "ok":
            logger.info("✅ Integrity check PASSED (ok)")
        else:
            logger.error("❌ Integrity check FAILED: %s", res)
            return None

        # Rotation
        _rotate_backups(out_dir, keep_count=keep_count)
        return str(target)

    except Exception:
        logger.exception("Backup failed")
        if target.exists():
            target.unlink()
        return None


def _rotate_backups(backup_dir: Path, keep_count: int) -> None:
    backups = sorted(backup_dir.glob("kbo_dev_*.db"), key=lambda p: p.stat().st_mtime)
    if len(backups) > keep_count:
        to_delete = backups[:-keep_count]
        for p in to_delete:
            try:
                p.unlink()
                logger.info("🗑️ Removed old backup: %s", p.name)
            except Exception as e:
                logger.warning("Failed to remove old backup %s: %s", p.name, e)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KBO SQLite Automated Backup Utility")
    parser.add_argument("--db-path", default="data/kbo_dev.db", help="Path to main SQLite DB")
    parser.add_argument("--backup-dir", default="data/backups", help="Directory to store backups")
    parser.add_argument("--keep-count", type=int, default=7, help="Number of backups to keep")
    parser.add_argument("--dry-run", action="store_true", help="Preview backup without writing")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    result = run_backup(
        db_path=args.db_path,
        backup_dir=args.backup_dir,
        keep_count=args.keep_count,
        dry_run=args.dry_run,
    )
    return 0 if result is not None else 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
