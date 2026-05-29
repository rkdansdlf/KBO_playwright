#!/usr/bin/env python3
"""
KBO Smart Data Remediation CLI.
Scrapes and repairs logically inconsistent or empty game details chronologically from 2025 backward.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from sqlalchemy import bindparam, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.verification.audit_game_logic import audit_game_logic
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.db.engine import SessionLocal
from src.services.game_collection_service import crawl_and_save_game_details
from src.services.player_id_resolver import PlayerIdResolver


def get_invalid_games_for_year(year: int) -> list[dict[str, str]]:
    """
    Identifies all completed games for a year that are logically inconsistent
    or have missing child record data in the database.
    """
    # 1. Fetch game logic violations
    print(f"🕵️  Checking game logic violations for year {year}...")
    violations = audit_game_logic(year=year)
    invalid_ids = {v["game_id"] for v in violations}

    # 2. Check for games with completely empty batting stats
    print(f"🕵️  Checking for games with empty batting stats for year {year}...")
    with SessionLocal() as session:
        empty_games = (
            session.execute(
                text("""
                SELECT game_id, game_date
                FROM game
                WHERE game_status IN ('COMPLETED', 'DRAW')
                  AND game_date LIKE :year_pattern
                  AND NOT EXISTS (SELECT 1 FROM game_batting_stats WHERE game_id = game.game_id)
            """),
                {"year_pattern": f"{year}%"},
            )
            .mappings()
            .all()
        )

        for row in empty_games:
            invalid_ids.add(row["game_id"])

        if not invalid_ids:
            return []

        targets = (
            session.execute(
                text("""
                SELECT game_id, game_date
                FROM game
                WHERE game_id IN :game_ids
                ORDER BY game_date DESC, game_id DESC
            """).bindparams(bindparam("game_ids", expanding=True)),
                {"game_ids": list(invalid_ids)},
            )
            .mappings()
            .all()
        )

    return [
        {
            "game_id": r["game_id"],
            "game_date": r["game_date"].strftime("%Y%m%d")
            if hasattr(r["game_date"], "strftime")
            else str(r["game_date"]).replace("-", ""),
        }
        for r in targets
    ]


async def remediate_year(year: int, limit: int | None = None, request_delay: float = 1.0) -> bool:
    """
    Finds and repairs invalid games for a single year.
    """
    print(f"\n📂 Processing Year: {year}")
    print("-" * 40)

    targets = get_invalid_games_for_year(year)
    if not targets:
        print(f"✅ Year {year}: No inconsistent or empty game details found.")
        return True

    print(f"❌ Year {year}: Found {len(targets)} game(s) requiring remediation.")
    if limit:
        targets = targets[:limit]
        print(f"⚠️ Limit applied: Restricting to first {limit} game(s).")

    # Setup resolver and crawlers
    with SessionLocal() as session:
        resolver = PlayerIdResolver(
            session,
            strict_game_resolution=True,
            allow_auto_register=False,
        )
        resolver.preload_season_index(year)

        detail_crawler = GameDetailCrawler(request_delay=request_delay, resolver=resolver)

        print(f"🚀 Starting remediation crawl for {len(targets)} game(s)...")
        result = await crawl_and_save_game_details(
            targets,
            detail_crawler=detail_crawler,
            force=True,  # Overwrite bad DB records
            pause_every=10,
            pause_seconds=2.0,
            log=print,
        )

        print(f"\n🎉 Remediation completed for {year}:")
        print(f"   Saved={result.detail_saved} Failed={result.detail_failed}")

    return result.detail_failed == 0


async def main():
    parser = argparse.ArgumentParser(
        description="Scrapes and repairs logically inconsistent or empty KBO game details from 2025 backward."
    )
    parser.add_argument("--start-year", type=int, default=2025, help="Year to start backward remediation from")
    parser.add_argument("--end-year", type=int, default=1982, help="Year to stop remediation at")
    parser.add_argument("--limit", type=int, help="Max number of games to remediate per year (useful for testing)")
    parser.add_argument("--delay", type=float, default=1.0, help="Base delay between requests in seconds")
    parser.add_argument("--game-id", help="Specific game ID to remediate")
    args = parser.parse_args()

    print("🛠️  KBO Data Remediation Tool initialized.")

    if args.game_id:
        print(f"🎯 Target: Specific game ID {args.game_id}")
        with SessionLocal() as session:
            game = (
                session.execute(
                    text("SELECT game_id, game_date FROM game WHERE game_id = :game_id"), {"game_id": args.game_id}
                )
                .mappings()
                .first()
            )
            if not game:
                print(f"❌ Game {args.game_id} not found in database.")
                return
            game_date = (
                game["game_date"].strftime("%Y%m%d")
                if hasattr(game["game_date"], "strftime")
                else str(game["game_date"]).replace("-", "")
            )
            targets = [{"game_id": game["game_id"], "game_date": game_date}]
            year = int(game_date[:4])

            resolver = PlayerIdResolver(
                session,
                strict_game_resolution=True,
                allow_auto_register=False,
            )
            resolver.preload_season_index(year)
            detail_crawler = GameDetailCrawler(request_delay=args.delay, resolver=resolver)

            print(f"🚀 Starting remediation crawl for {args.game_id}...")
            result = await crawl_and_save_game_details(
                targets,
                detail_crawler=detail_crawler,
                force=True,
                pause_every=10,
                pause_seconds=2.0,
                log=print,
            )
            print(f"\n🎉 Remediation completed: Saved={result.detail_saved} Failed={result.detail_failed}")
        return

    print(f"   Range: {args.start_year} down to {args.end_year}")
    if args.limit:
        print(f"   Per-year limit: {args.limit} game(s)")
    print("-" * 50)

    for year in range(args.start_year, args.end_year - 1, -1):
        success = await remediate_year(year, limit=args.limit, request_delay=args.delay)
        if not success:
            print(f"⚠️ Warning: Remediation encountered failures in season {year}.")

    print("\n🏁 All specified seasons processed!")


if __name__ == "__main__":
    asyncio.run(main())
