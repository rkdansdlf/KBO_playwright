# noqa: INP001
"""SQLite deletion-anomaly integrity migration.

SQLite requires table rebuilds for most FK and ON DELETE changes, so this
migration delegates to the idempotent repair/rebuild tool.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.maintenance.repair_deletion_anomalies import DEFAULT_DB_PATH, repair


def main() -> None:
    """Apply SQLite deletion anomaly integrity migration from CLI."""
    parser = argparse.ArgumentParser(description="Apply SQLite deletion anomaly integrity migration.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--dry-run", action="store_true", help="Report planned changes without applying.")
    args = parser.parse_args()

    actions = repair(Path(args.db_path), apply=not args.dry_run, schema=True)
    for action in actions:
        print(f"[{action.status}] {action.name}: rows={action.row_count}")


if __name__ == "__main__":
    main()
