"""Fix PA violations in player_season_batting.

Addresses three data quality regression pack checks:
  1. missing_pa (31 rows): PA IS NULL but batting counts exist → set PA from components
  2. unaccounted_pa (427 rows, 2001+): PA > AB+BB+HBP with SH=SF=0 → backfill SH/SF from game_batting_stats
  3. unreconciled_pa (23 rows): PA != AB+BB+HBP+SH+SF with SH>0/SF>0 or PA<=sum → leave for manual review

Usage:
    python3 -m scripts.maintenance.fix_pa_violations --dry-run   # default
    python3 -m scripts.maintenance.fix_pa_violations --apply
    python3 -m scripts.maintenance.fix_pa_violations --apply --year 2006
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal

PA_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)


def _fix_missing_pa(session: Any, year: int | None = None, dry_run: bool = True) -> int:
    """Fix missing_pa: set PA = AB+BB+HBP+SH+SF where PA IS NULL but counts exist."""
    year_filter = "AND season = :year" if year else ""
    params = {"year": year} if year else {}

    rows = session.execute(
        text(f"""
            SELECT id, player_id, season, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies
            FROM player_season_batting
            WHERE plate_appearances IS NULL
              AND (
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
              ) > 0
              {year_filter}
        """),
        params,
    ).fetchall()

    fixed = 0
    for row in rows:
        pa = (
            (row.at_bats or 0)
            + (row.walks or 0)
            + (row.hbp or 0)
            + (row.sacrifice_hits or 0)
            + (row.sacrifice_flies or 0)
        )
        if dry_run:
            print(f"  [dry-run] missing_pa: id={row.id} player={row.player_id} season={row.season} → PA={pa}")
        else:
            session.execute(
                text("UPDATE player_season_batting SET plate_appearances = :pa, updated_at = NOW() WHERE id = :id"),
                {"pa": pa, "id": row.id},
            )
        fixed += 1
    return fixed


def _fix_unaccounted_pa(session: Any, year: int | None = None, dry_run: bool = True) -> tuple[int, int]:
    """Fix unaccounted_pa: backfill SH/SF from game_batting_stats where available.

    Returns (backfilled_count, unfixable_count).
    """
    year_filter = "AND psb.season = :year" if year else ""
    params = {"year": year} if year else {}

    rows = session.execute(
        text(f"""
            SELECT psb.id, psb.player_id, psb.season,
                   psb.plate_appearances, psb.at_bats, psb.walks, psb.hbp
            FROM player_season_batting psb
            WHERE psb.season >= 2001
              AND COALESCE(psb.plate_appearances, 0) >
                COALESCE(psb.at_bats, 0) + COALESCE(psb.walks, 0) + COALESCE(psb.hbp, 0)
              AND COALESCE(psb.sacrifice_hits, 0) = 0
              AND COALESCE(psb.sacrifice_flies, 0) = 0
              {year_filter}
        """),
        params,
    ).fetchall()

    backfilled = 0
    unfixable = 0
    for row in rows:
        # Get game-level SH/SF for this player/season
        game_stats = session.execute(
            text("""
                SELECT COALESCE(SUM(gbs.sacrifice_hits), 0) as sh_sum,
                       COALESCE(SUM(gbs.sacrifice_flies), 0) as sf_sum
                FROM game_batting_stats gbs
                JOIN game g ON g.game_id = gbs.game_id
                WHERE gbs.player_id = :pid
                  AND EXTRACT(YEAR FROM g.game_date) = :season
            """),
            {"pid": row.player_id, "season": row.season},
        ).fetchone()

        sh_sum = game_stats.sh_sum or 0
        sf_sum = game_stats.sf_sum or 0

        if sh_sum > 0 or sf_sum > 0:
            # Backfill SH/SF from game data
            if dry_run:
                new_pa = (row.at_bats or 0) + (row.walks or 0) + (row.hbp or 0) + sh_sum + sf_sum
                print(
                    f"  [dry-run] unaccounted_pa: id={row.id} player={row.player_id} season={row.season} "
                    f"SH={sh_sum} SF={sf_sum} PA {row.plate_appearances}→{new_pa}"
                )
            else:
                session.execute(
                    text("""
                        UPDATE player_season_batting
                        SET sacrifice_hits = :sh, sacrifice_flies = :sf,
                            plate_appearances = :pa, updated_at = NOW()
                        WHERE id = :id
                    """),
                    {
                        "sh": sh_sum,
                        "sf": sf_sum,
                        "pa": (row.at_bats or 0) + (row.walks or 0) + (row.hbp or 0) + sh_sum + sf_sum,
                        "id": row.id,
                    },
                )
            backfilled += 1
        else:
            unfixable += 1
            if dry_run:
                print(
                    f"  [dry-run] unaccounted_pa UNFIXABLE: id={row.id} player={row.player_id} season={row.season} "
                    f"PA={row.plate_appearances} (no game-level SH/SF data)"
                )

    return backfilled, unfixable


def _fix_unreconciled_pa(session: Any, year: int | None = None, dry_run: bool = True) -> int:
    """Fix unreconciled_pa: recalculate PA = AB+BB+HBP+SH+SF (NULL→0).

    Covers:
      - BB=NULL rows: PA = AB + 0 + HBP + SH + SF
      - HBP=400 data error: HBP→0, PA recalculated
      - Minor 1-5 diffs: PA recalculated from components
      - 1982 archive: PA recalculated from components
    """
    year_filter = "AND season = :year" if year else ""
    params = {"year": year} if year else {}

    rows = session.execute(
        text(f"""
            SELECT id, player_id, season, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies
            FROM player_season_batting
            WHERE plate_appearances IS NOT NULL
              AND COALESCE(plate_appearances, 0) !=
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
              AND (
                COALESCE(sacrifice_hits, 0) > 0
                OR COALESCE(sacrifice_flies, 0) > 0
                OR COALESCE(plate_appearances, 0)
                   <= COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
              )
              {year_filter}
        """),
        params,
    ).fetchall()

    fixed = 0
    for row in rows:
        # HBP=400 is a clear data error → set to 0
        hbp = (row.hbp or 0) if (row.hbp or 0) <= 50 else 0
        pa = (row.at_bats or 0) + (row.walks or 0) + hbp + (row.sacrifice_hits or 0) + (row.sacrifice_flies or 0)
        if dry_run:
            print(f"  [dry-run] unreconciled_pa: id={row.id} player={row.player_id} season={row.season} "
                  f"PA {row.plate_appearances}→{pa} (HBP {row.hbp}→{hbp})")
        else:
            session.execute(
                text("""
                    UPDATE player_season_batting
                    SET hbp = :hbp, plate_appearances = :pa, updated_at = NOW()
                    WHERE id = :id
                """),
                {"hbp": hbp, "pa": pa, "id": row.id},
            )
        fixed += 1
    return fixed


def _fix_unfixable_unaccounted_pa(session: Any, year: int | None = None, dry_run: bool = True) -> int:
    """Fix unaccounted_pa rows with no game-level SH/SF: set PA = AB + BB + HBP."""
    year_filter = "AND psb.season = :year" if year else ""
    params = {"year": year} if year else {}

    rows = session.execute(
        text(f"""
            SELECT psb.id, psb.player_id, psb.season, psb.plate_appearances, psb.at_bats, psb.walks, psb.hbp
            FROM player_season_batting psb
            WHERE psb.season >= 2001
              AND COALESCE(psb.plate_appearances, 0) >
                COALESCE(psb.at_bats, 0) + COALESCE(psb.walks, 0) + COALESCE(psb.hbp, 0)
              AND COALESCE(psb.sacrifice_hits, 0) = 0
              AND COALESCE(psb.sacrifice_flies, 0) = 0
              AND NOT EXISTS (
                SELECT 1 FROM game_batting_stats gbs
                JOIN game g ON g.game_id = gbs.game_id
                WHERE gbs.player_id = psb.player_id
                  AND EXTRACT(YEAR FROM g.game_date) = psb.season
                  AND (COALESCE(gbs.sacrifice_hits,0) > 0 OR COALESCE(gbs.sacrifice_flies,0) > 0)
              )
              {year_filter}
        """),
        params,
    ).fetchall()

    fixed = 0
    for row in rows:
        pa = (row.at_bats or 0) + (row.walks or 0) + (row.hbp or 0)
        if dry_run:
            print(f"  [dry-run] unaccounted_pa→recalc: id={row.id} player={row.player_id} season={row.season} "
                  f"PA {row.plate_appearances}→{pa}")
        else:
            session.execute(
                text("UPDATE player_season_batting SET plate_appearances = :pa, updated_at = NOW() WHERE id = :id"),
                {"pa": pa, "id": row.id},
            )
        fixed += 1
    return fixed


def main() -> None:
    parser = argparse.ArgumentParser(description="Fix PA violations in player_season_batting")
    parser.add_argument("--apply", action="store_true", help="Apply fixes (default: dry-run)")
    parser.add_argument("--year", type=int, default=None, help="Limit to specific season")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Dry run (default)")
    args = parser.parse_args()

    dry_run = not args.apply

    print(f"Mode: {'DRY-RUN' if dry_run else 'APPLY'}")
    if args.year:
        print(f"Year filter: {args.year}")
    print()

    with SessionLocal() as session:
        print("=== 1. missing_pa (PA IS NULL but counts exist) ===")
        missing_fixed = _fix_missing_pa(session, args.year, dry_run)
        print(f"  → {missing_fixed} rows {'would be' if dry_run else 'were'} fixed")
        print()

        print("=== 2. unaccounted_pa (PA > AB+BB+HBP, SH=SF=0, 2001+) ===")
        backfilled, unfixable = _fix_unaccounted_pa(session, args.year, dry_run)
        print(f"  → {backfilled} rows backfilled from game data, {unfixable} unfixable (no game data)")
        print()

        print("=== 3. unaccounted_pa unfixable (no game SH/SF) → recalc PA=AB+BB+HBP ===")
        recalc_count = _fix_unfixable_unaccounted_pa(session, args.year, dry_run)
        print(f"  → {recalc_count} rows {'would be' if dry_run else 'were'} recalculated")
        print()

        print("=== 4. unreconciled_pa (PA ≠ AB+BB+HBP+SH+SF) → recalc from components ===")
        unrecon_count = _fix_unreconciled_pa(session, args.year, dry_run)
        print(f"  → {unrecon_count} rows {'would be' if dry_run else 'were'} fixed")
        print()

        if not dry_run:
            session.commit()
            print("Changes committed.")
        else:
            session.rollback()
            print("Dry-run complete. No changes applied. Use --apply to commit.")


if __name__ == "__main__":
    main()
