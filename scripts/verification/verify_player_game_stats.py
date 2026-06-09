"""
PlayerGame data quality verification.

Verifies integrity of player_game_batting and player_game_pitching tables:
  - No duplicate (game_id, player_id) pairs
  - No NULL player_id or player_name
  - Rate stats within valid ranges (avg, obp, era, whip)
  - Coverage by game status and season year

Usage:
  python -m scripts.verification.verify_player_game_stats
  python -m scripts.verification.verify_player_game_stats --exit-code
  python -m scripts.verification.verify_player_game_stats --verbose
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from typing import Sequence

from sqlalchemy import text

from src.db.engine import SessionLocal

logger = logging.getLogger(__name__)


def check_duplicates(session) -> list[str]:
    issues = []
    for tbl in ("player_game_batting", "player_game_pitching"):
        count = session.execute(
            text(
                f"SELECT COALESCE(COUNT(*), 0) FROM (SELECT game_id, player_id, COUNT(*) FROM {tbl} GROUP BY game_id, player_id HAVING COUNT(*) > 1)"
            )
        ).scalar()
        if count:
            issues.append(f"{tbl}: {count} duplicate (game_id, player_id) pair(s)")
    return issues


def check_nulls(session) -> list[str]:
    issues = []
    for tbl in ("player_game_batting", "player_game_pitching"):
        null_player_id = session.execute(text(f"SELECT COUNT(*) FROM {tbl} WHERE player_id IS NULL")).scalar()
        if null_player_id:
            issues.append(f"{tbl}: {null_player_id} row(s) with NULL player_id")
        null_name = session.execute(text(f"SELECT COUNT(*) FROM {tbl} WHERE player_name IS NULL")).scalar()
        if null_name:
            issues.append(f"{tbl}: {null_name} row(s) with NULL player_name")
        null_side = session.execute(text(f"SELECT COUNT(*) FROM {tbl} WHERE team_side IS NULL")).scalar()
        if null_side:
            issues.append(f"{tbl}: {null_side} row(s) with NULL team_side")
    return issues


def check_rate_stats(session) -> list[str]:
    issues = []
    # Batting rate stat ranges
    checks = [
        ("player_game_batting", "avg", 0, 1),
        ("player_game_batting", "obp", 0, 1),
        ("player_game_batting", "slg", 0, 5),
    ]
    for tbl, col, lo, hi in checks:
        count = session.execute(
            text(f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NOT NULL AND ({col} < {lo} OR {col} > {hi})")
        ).scalar()
        if count:
            issues.append(f"{tbl}.{col}: {count} row(s) outside [{lo}, {hi}]")

    # avg > obp is possible with sacrifice flies — log as info, not error
    avg_gt_obp = session.execute(
        text("SELECT COUNT(*) FROM player_game_batting WHERE avg IS NOT NULL AND obp IS NOT NULL AND avg > obp")
    ).scalar()
    if avg_gt_obp:
        issues.append(f"player_game_batting avg > obp: {avg_gt_obp} row(s) (expected with sacrifice flies)")

    # Pitching rate stat ranges
    # ERA can exceed 100 with very short outings (1 out, 4+ ER → ERA=108+)
    checks = [
        ("player_game_pitching", "era", 0, 200),
        ("player_game_pitching", "whip", 0, 30),
    ]
    for tbl, col, lo, hi in checks:
        count = session.execute(
            text(f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NOT NULL AND ({col} < {lo} OR {col} > {hi})")
        ).scalar()
        if count:
            issues.append(f"{tbl}.{col}: {count} row(s) outside [{lo}, {hi}]")

    return issues


def check_coverage(session, verbose: bool = False) -> list[str]:
    issues = []
    rows = session.execute(
        text("""
        SELECT CAST(SUBSTR(g.game_id, 1, 4) AS INTEGER) as yr,
               g.game_status,
               COUNT(DISTINCT g.game_id) as total_games,
               COUNT(DISTINCT pgb.game_id) as covered_games
        FROM game g
        LEFT JOIN player_game_batting pgb ON pgb.game_id = g.game_id
        WHERE g.game_status IN ('COMPLETED', 'DRAW')
        GROUP BY yr, g.game_status
        ORDER BY yr, g.game_status
    """)
    ).fetchall()

    for yr, status, total, covered in rows:
        pct = 100.0 * covered / total if total else 0
        if verbose:
            issues.append(f"{yr} {status:<12} games={total:>5} covered={covered:>5} {pct:5.1f}%")
        if yr >= 2018 and pct < 90:
            issues.append(f"WARN: {yr} {status} coverage {pct:.1f}%")

    return issues


def check_draw_missing_sources(session) -> list[str]:
    """Diagnose DRAW games that lack source-level stats."""
    issues = []
    for src in ("game_batting_stats", "game_pitching_stats", "game_play_by_play"):
        count = session.execute(
            text(f"""
                SELECT COUNT(*)
                FROM game g
                LEFT JOIN {src} s ON s.game_id = g.game_id
                WHERE g.game_status = 'DRAW'
                  AND s.game_id IS NULL
            """)
        ).scalar()
        if count:
            issues.append(f"DRAW games missing {src}: {count}")
    return issues


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify PlayerGame data quality")
    parser.add_argument("--exit-code", action="store_true", help="Return non-zero exit code on issues")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed coverage breakdown")
    args = parser.parse_args(argv)

    logger.info(f"PlayerGame Data Quality Verification — {datetime.now():%Y-%m-%d %H:%M:%S}")
    logger.info("=" * 60)

    with SessionLocal() as session:
        all_bad = []
        for label, fn in [
            ("Duplicates", check_duplicates),
            ("NULL fields", check_nulls),
            ("Rate stats", check_rate_stats),
            ("DRAW source gaps", check_draw_missing_sources),
            ("Coverage", lambda s: check_coverage(s, verbose=args.verbose)),
        ]:
            logger.info(f"\n--- {label} ---")
            issues = fn(session)
            if issues:
                for i in issues:
                    logger.info(f"  {i}")
                all_bad.extend(issues)
            else:
                logger.info("  OK")

        total_batting = session.execute(text("SELECT COUNT(*) FROM player_game_batting")).scalar()
        total_pitching = session.execute(text("SELECT COUNT(*) FROM player_game_pitching")).scalar()
        total_games = session.execute(text("SELECT COUNT(DISTINCT game_id) FROM player_game_batting")).scalar()

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Summary: batting={total_batting:,} pitching={total_pitching:,} games={total_games:,}")
    if all_bad:
        error_count = sum(1 for i in all_bad if i.startswith("WARN"))
        info_count = len(all_bad) - error_count
        logger.info(f"Issues: {len(all_bad)} ({error_count} warnings, {info_count} info)")
        if args.exit_code:
            return 2 if any(i.startswith("WARN") for i in all_bad) else 1
    else:
        logger.info("All checks passed")
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
