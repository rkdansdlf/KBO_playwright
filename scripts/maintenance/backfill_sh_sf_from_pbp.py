"""One-time backfill: derive SH/SF from PBP events for existing game_batting_stats.

Scans completed games where sacrifice_hits=0 and sacrifice_flies=0,
queries game_events for SAC_BUNT (SH) and qualifying FLYOUT (SF) events,
and updates game_batting_stats with the derived values.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal
from src.services.pbp_sh_sf_derivation import apply_sh_sf_to_batting_stats, derive_sh_sf_for_game

BACKFILL_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)


def find_candidate_games(session: Any, year: int | None = None) -> list[str]:
    """Find completed games where SH=SF=0 for all batting rows but PBP events exist."""
    year_filter = ""
    params: dict = {}
    if year:
        year_filter = "AND g.game_date LIKE :year_pattern"
        params["year_pattern"] = f"{year}%"

    sql = text(f"""
        SELECT DISTINCT g.game_id
        FROM game g
        WHERE g.game_status IN ('COMPLETED', 'DRAW')
          {year_filter}
          AND EXISTS (
              SELECT 1 FROM game_events e WHERE e.game_id = g.game_id
          )
          AND EXISTS (
              SELECT 1 FROM game_batting_stats b
              WHERE b.game_id = g.game_id
                AND b.plate_appearances IS NOT NULL
                AND b.plate_appearances != COALESCE(b.at_bats,0) + COALESCE(b.walks,0) + COALESCE(b.hbp,0) + COALESCE(b.sacrifice_hits,0) + COALESCE(b.sacrifice_flies,0)
          )
        ORDER BY g.game_date
    """)

    with SessionLocal() as session:
        rows = session.execute(sql, params).all()
        return [str(row[0]) for row in rows]


def count_rows_in_need(session: Any, year: int | None = None) -> int:
    """Count batting stat rows where PA formula fails (SH/SF likely missing)."""
    year_filter = ""
    params: dict = {}
    if year:
        year_filter = "AND g.game_date LIKE :year_pattern"
        params["year_pattern"] = f"{year}%"

    sql = text(f"""
        SELECT COUNT(*)
        FROM game_batting_stats b
        JOIN game g ON b.game_id = g.game_id
        WHERE g.game_status IN ('COMPLETED', 'DRAW')
          {year_filter}
          AND b.plate_appearances IS NOT NULL
          AND b.plate_appearances != COALESCE(b.at_bats,0) + COALESCE(b.walks,0) + COALESCE(b.hbp,0) + COALESCE(b.sacrifice_hits,0) + COALESCE(b.sacrifice_flies,0)
    """)
    return session.execute(sql, params).scalar() or 0


def backfill_game(session: Any, game_id: str, dry_run: bool = False) -> int:
    """Derive SH/SF for one game and update. Returns rows updated."""
    if dry_run:
        derived = derive_sh_sf_for_game(session, game_id)
        return sum(1 for c in derived.values() if c["sh"] > 0 or c["sf"] > 0)
    return apply_sh_sf_to_batting_stats(session, game_id)


def main():
    parser = argparse.ArgumentParser(description="Backfill SH/SF from PBP events")
    parser.add_argument("--year", type=int, help="Only process games from this year")
    parser.add_argument("--game-id", help="Only process a specific game")
    parser.add_argument("--dry-run", action="store_true", help="Preview without updating")
    parser.add_argument("--batch", type=int, default=100, help="Games per commit batch (default 100)")
    args = parser.parse_args()

    with SessionLocal() as session:
        total_needed = count_rows_in_need(session, year=args.year)
        print(f"Rows with PA formula violation: {total_needed}")

        if args.game_id:
            game_ids = [args.game_id]
        else:
            game_ids = find_candidate_games(session, year=args.year)

        print(f"Candidate games with PBP events available: {len(game_ids)}")

    if args.dry_run:
        print("[DRY RUN] No changes will be made.")
        for gid in game_ids[:10]:
            derived = derive_sh_sf_for_game(session := SessionLocal(), gid)
            if derived:
                total_sh = sum(c["sh"] for c in derived.values())
                total_sf = sum(c["sf"] for c in derived.values())
                print(f"  {gid}: would update {len(derived)} players (SH={total_sh}, SF={total_sf})")
        if len(game_ids) > 10:
            print(f"  ... and {len(game_ids) - 10} more games")
        return

    total_updated = 0
    total_games = 0
    for i, gid in enumerate(game_ids, 1):
        with SessionLocal() as session:
            try:
                updated = backfill_game(session, gid)
                if updated:
                    session.commit()
                    total_updated += updated
                    total_games += 1
            except BACKFILL_EXCEPTIONS as e:
                session.rollback()
                print(f"Error processing {gid}: {e}")

        if i % 100 == 0:
            print(f"Progress: {i}/{len(game_ids)} games, {total_updated} rows updated")
            time.sleep(0.1)

    print(f"\nDone. Updated {total_updated} rows across {total_games} games.")


if __name__ == "__main__":
    main()
