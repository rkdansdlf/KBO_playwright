"""Diagnose and fix kbo_seasons NULL season_year / league_type_code issues.

Usage:
    python3 -m scripts.maintenance.diagnose_seasons --dry-run
    python3 -m scripts.maintenance.diagnose_seasons --fix
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CORE_TYPES = {
    0: "정규시즌",
    1: "시범경기",
    2: "와일드카드",
    3: "준플레이오프",
    4: "플레이오프",
    5: "한국시리즈",
}


def diagnose_null_seasons() -> dict:
    """Find kbo_seasons rows with NULL season_year or league_type_code."""
    with SessionLocal() as session:
        result = session.execute(
            text(
                """
                SELECT season_id, season_year, league_type_code, league_type_name
                FROM kbo_seasons
                WHERE season_year IS NULL OR league_type_code IS NULL
                ORDER BY season_id
                """,
            ),
        )
        rows = result.fetchall()
        return {
            "null_rows": [
                {
                    "season_id": r[0],
                    "season_year": r[1],
                    "league_type_code": r[2],
                    "league_type_name": r[3],
                }
                for r in rows
            ],
            "count": len(rows),
        }


def diagnose_missing_core_types() -> dict:
    """Find (year, code) combos missing from kbo_seasons for core types."""
    with SessionLocal() as session:
        result = session.execute(
            text(
                """
                SELECT season_year, league_type_code
                FROM kbo_seasons
                WHERE league_type_code IN (0, 1, 2, 3, 4, 5)
                """,
            ),
        )
        existing = {(r[0], r[1]) for r in result.fetchall()}

    all_combos = [(year, code) for year in range(1982, 2031) for code in CORE_TYPES]
    missing = [{"year": y, "code": c, "name": CORE_TYPES[c]} for y, c in all_combos if (y, c) not in existing]
    return {
        "missing": missing,
        "count": len(missing),
        "existing_count": len(existing),
    }


def diagnose_orphan_games() -> dict:
    """Find games whose season_id doesn't reference kbo_seasons."""
    with SessionLocal() as session:
        result = session.execute(
            text(
                """
                SELECT COUNT(*) AS orphan_count
                FROM game g
                LEFT JOIN kbo_seasons s ON g.season_id = s.season_id
                WHERE s.season_id IS NULL
                """,
            ),
        )
        count = result.scalar() or 0
        return {"orphan_game_count": count}


def fix_null_seasons(dry_run: bool) -> int:
    """Auto-create missing kbo_seasons rows for core types."""
    diag = diagnose_missing_core_types()
    missing = diag["missing"]
    if not missing:
        logger.info("No missing core type seasons found.")
        return 0

    logger.info("Found %d missing season entries (dry_run=%s)", len(missing), dry_run)
    created = 0
    with SessionLocal() as session:
        for entry in missing:
            sid = entry["year"] * 100 + entry["code"]
            name = CORE_TYPES.get(entry["code"], f"Type {entry['code']}")
            if dry_run:
                logger.info(
                    "  DRY-RUN: would INSERT season_id=%s year=%s code=%s name=%s",
                    sid,
                    entry["year"],
                    entry["code"],
                    name,
                )
            else:
                session.execute(
                    text(
                        """
                        INSERT INTO kbo_seasons
                            (season_id, season_year, league_type_code, league_type_name,
                             created_at, updated_at)
                        VALUES (:sid, :year, :code, :name,
                                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT DO NOTHING
                        """,
                    ),
                    {
                        "sid": sid,
                        "year": entry["year"],
                        "code": entry["code"],
                        "name": name,
                    },
                )
                logger.info(
                    "  CREATED: season_id=%s year=%s code=%s name=%s",
                    sid,
                    entry["year"],
                    entry["code"],
                    name,
                )
            created += 1
        if not dry_run:
            session.commit()
            logger.info("Committed %d new season rows.", created)
    return created


def fix_orphan_games(dry_run: bool) -> int:
    """Re-point orphan games to the correct season_id based on game_date."""
    with SessionLocal() as session:
        result = session.execute(
            text(
                """
                SELECT g.game_id, g.game_date, g.season_id
                FROM games g
                LEFT JOIN kbo_seasons s ON g.season_id = s.season_id
                WHERE s.season_id IS NULL
                LIMIT 1000
                """,
            ),
        )
        orphans = result.fetchall()
        if not orphans:
            logger.info("No orphan games found.")
            return 0

        logger.info("Found %d orphan games to fix (dry_run=%s)", len(orphans), dry_run)
        fixed = 0
        for game_id, game_date, old_sid in orphans:
            year = game_date.year if hasattr(game_date, "year") else int(str(game_date)[:4])
            sid = year * 100
            if dry_run:
                logger.info(
                    "  DRY-RUN: game_id=%s date=%s old_sid=%s -> new_sid=%s",
                    game_id,
                    game_date,
                    old_sid,
                    sid,
                )
            else:
                session.execute(
                    text("UPDATE games SET season_id = :sid WHERE game_id = :gid"),
                    {"sid": sid, "gid": game_id},
                )
            fixed += 1
        if not dry_run:
            session.commit()
            logger.info("Fixed %d orphan games.", fixed)
    return fixed


def main() -> None:
    parser = argparse.ArgumentParser(description="Diagnose/fix kbo_seasons NULL or missing entries")
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Apply fixes (default is dry-run)",
    )
    parser.add_argument(
        "--fix-orphans",
        action="store_true",
        help="Also fix orphan game.season_id references",
    )
    args = parser.parse_args()

    logger.info("=== Diagnosing kbo_seasons ===")
    null_diag = diagnose_null_seasons()
    logger.info("NULL season_year/league_type_code rows: %d", null_diag["count"])
    for row in null_diag["null_rows"]:
        logger.info("  %s", row)

    missing_diag = diagnose_missing_core_types()
    logger.info("Missing core type entries: %d", missing_diag["count"])
    if missing_diag["count"] <= 20:
        for m in missing_diag["missing"]:
            logger.info("  %s", m)

    orphan_diag = diagnose_orphan_games()
    logger.info("Orphan games: %d", orphan_diag["orphan_game_count"])

    if args.fix:
        logger.info("\n=== Applying fixes ===")
        created = fix_null_seasons(dry_run=False)
        logger.info("Created %d season rows.", created)
    else:
        logger.info("\n(Run with --fix to apply changes)")

    if args.fix_orphans:
        logger.info("\n=== Fixing orphan games ===")
        fixed = fix_orphan_games(dry_run=False)
        logger.info("Fixed %d orphan games.", fixed)


if __name__ == "__main__":
    main()
