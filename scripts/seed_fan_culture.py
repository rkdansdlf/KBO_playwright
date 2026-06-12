"""
Seed team rivalry data from CSV into the team_rivalries table.

Usage:
    python scripts/seed_fan_culture.py
    python scripts/seed_fan_culture.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.db.engine import SessionLocal
from src.repositories.fan_culture_repository import FanCultureRepository

RIVALRIES_CSV = ROOT / "data" / "seed" / "team_rivalries.csv"


def seed_rivalries(dry_run: bool = False) -> int:
    rows = []
    with open(RIVALRIES_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows.extend(
            {
                "team_id_a": row["team_id_a"].strip(),
                "team_id_b": row["team_id_b"].strip(),
                "rivalry_name": row["rivalry_name"].strip(),
                "description": row.get("description", "").strip() or None,
                "intensity": row.get("intensity", "MEDIUM").strip(),
            }
            for row in reader
        )

    logger.info("Loaded %s rivalries from %s", len(rows), RIVALRIES_CSV)

    if dry_run:
        for r in rows:
            logger.info("  %s vs %s: %s (%s)", r["team_id_a"], r["team_id_b"], r["rivalry_name"], r["intensity"])
        return len(rows)

    with SessionLocal() as session:
        repo = FanCultureRepository(session)
        saved = 0
        skipped = 0
        for r in rows:
            try:
                repo.save_rivalry(r)
                session.flush()
                saved += 1
            except Exception as exc:  # noqa: BLE001
                session.rollback()
                skipped += 1
                logger.warning("Skipped rivalry %s-%s: %s", r["team_id_a"], r["team_id_b"], exc)
        session.commit()
        logger.info("Seeded %s rivalries into team_rivalries. (%s skipped/already exist)", saved, skipped)
    return saved


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed KBO team rivalry data")
    parser.add_argument("--dry-run", action="store_true", help="Print without saving")
    args = parser.parse_args()
    seed_rivalries(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
