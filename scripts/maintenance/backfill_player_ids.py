#!/usr/bin/env python3
"""
Batch-resolve NULL player_ids in game_batting_stats, game_pitching_stats, game_lineups.

Uses PlayerIdResolver (name + team_code + season) to match against
player_basic / player_season_batting / player_season_pitching.

Usage:
    python scripts/maintenance/backfill_player_ids.py                # all years
    python scripts/maintenance/backfill_player_ids.py --year 2010    # single year
    python scripts/maintenance/backfill_player_ids.py --start 2010 --end 2017
    python scripts/maintenance/backfill_player_ids.py --dry-run      # preview only
"""
import argparse
import sys
import os
from collections import defaultdict

sys.path.insert(0, os.getcwd())

from sqlalchemy import text
from src.db.engine import SessionLocal
from src.services.player_id_resolver import PlayerIdResolver


TABLES = [
    ("game_batting_stats", "id"),
    ("game_pitching_stats", "id"),
    ("game_lineups", "id"),
]


def backfill_year(session, resolver: PlayerIdResolver, year: int, dry_run: bool = False):
    """Resolve NULL player_ids for a single season year."""
    resolver.preload_season_index(year)

    stats = defaultdict(int)  # resolved, skipped, failed

    for table_name, pk_col in TABLES:
        rows = session.execute(text(f"""
            SELECT {pk_col}, game_id, player_name, team_code
            FROM {table_name}
            WHERE player_id IS NULL
              AND game_id LIKE :prefix
        """), {"prefix": f"{year}%"}).fetchall()

        if not rows:
            continue

        print(f"  📋 {table_name}: {len(rows)} NULL rows")
        resolved = 0
        updates = []

        for row_id, game_id, player_name, team_code in rows:
            if not player_name:
                stats["skipped"] += 1
                continue

            pid = resolver.resolve_id(player_name, team_code, year)
            if pid:
                updates.append((pid, row_id))
                resolved += 1
            else:
                if stats["failed"] < 5:  # Limit debug output
                    print(f"       ❌ Failed to resolve: {player_name} ({team_code}) {year}")
                stats["failed"] += 1

        if updates and not dry_run:
            # Batch UPDATE in chunks of 500
            for i in range(0, len(updates), 500):
                chunk = updates[i:i+500]
                for pid, rid in chunk:
                    session.execute(text(f"""
                        UPDATE {table_name} SET player_id = :pid WHERE {pk_col} = :rid
                    """), {"pid": pid, "rid": rid})
            session.commit()

        stats["resolved"] += resolved
        pct = (resolved / len(rows) * 100) if rows else 0
        print(f"     ✅ Resolved: {resolved}/{len(rows)} ({pct:.1f}%)")

    return dict(stats)


def main():
    parser = argparse.ArgumentParser(description="Backfill NULL player_ids")
    parser.add_argument("--year", type=int, help="Single year")
    parser.add_argument("--start", type=int, default=2001, help="Start year")
    parser.add_argument("--end", type=int, default=2025, help="End year")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    args = parser.parse_args()

    years = [args.year] if args.year else list(range(args.start, args.end + 1))

    print(f"🚀 Backfilling player_ids for years: {years[0]}-{years[-1]}")
    if args.dry_run:
        print("   ⚠️  DRY RUN — no DB writes")

    grand = defaultdict(int)

    with SessionLocal() as session:
        resolver = PlayerIdResolver(session)

        for year in years:
            # Quick check: any NULLs for this year?
            cnt = session.execute(text(
                "SELECT COUNT(*) FROM game_batting_stats WHERE player_id IS NULL AND game_id LIKE :p"
            ), {"p": f"{year}%"}).scalar()
            if cnt == 0:
                continue

            print(f"\n📅 {year} ({cnt} NULL batting rows)")
            result = backfill_year(session, resolver, year, args.dry_run)
            for k, v in result.items():
                grand[k] += v

    print("\n" + "=" * 50)
    print(f"🏁 Summary: Resolved {grand['resolved']:,} | Failed {grand['failed']:,} | Skipped {grand['skipped']:,}")


if __name__ == "__main__":
    main()
