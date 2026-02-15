"""
KBO Daily Data Update Orchestrator.
Processes schedule, box score details, and cumulative season stats for one date.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, datetime, timedelta
from typing import Optional, Sequence
from zoneinfo import ZoneInfo

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.cli.sync_oci import main as sync_main
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.player_batting_all_series_crawler import crawl_series_batting_stats
from src.crawlers.player_pitching_all_series_crawler import crawl_pitcher_series
from src.crawlers.schedule_crawler import ScheduleCrawler
from src.db.engine import SessionLocal
from src.repositories.game_repository import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_UNRESOLVED,
    refresh_game_status_for_date,
    save_game_detail,
    save_schedule_game,
    update_game_status,
)
from src.services.player_id_resolver import PlayerIdResolver
from src.utils.safe_print import safe_print as print

KST = ZoneInfo("Asia/Seoul")


def _today_kst() -> date:
    return datetime.now(KST).date()


def _failure_status(target_date: str, failure_reason: Optional[str], today: date) -> Optional[str]:
    if failure_reason == "cancelled":
        return GAME_STATUS_CANCELLED
    try:
        target_day = datetime.strptime(target_date, "%Y%m%d").date()
    except ValueError:
        return None
    if target_day < today:
        return GAME_STATUS_UNRESOLVED
    return None


async def run_update(target_date: str, sync: bool = False, headless: bool = True, limit: int = None):
    """
    Main orchestration logic for daily updates.
    """
    print(f"\n{'='*60}")
    print(f"🚀 KBO Daily Update Started for Date: {target_date}")
    print(f"{'='*60}")

    year = int(target_date[:4])
    month = int(target_date[4:6])

    # 1. Schedule crawler and DB upsert
    print("\n📅 Step 1: Crawling + saving monthly schedule...")
    s_crawler = ScheduleCrawler()
    schedule_games = await s_crawler.crawl_schedule(year, month)
    schedule_saved = 0
    schedule_failed = 0
    for game in schedule_games:
        if save_schedule_game(game):
            schedule_saved += 1
        else:
            schedule_failed += 1
    print(
        f"   ✅ Schedule discovered={len(schedule_games)} "
        f"saved={schedule_saved} failed={schedule_failed}"
    )

    # Filter for target date
    daily_games = [g for g in schedule_games if str(g.get("game_date", "")).replace("-", "") == target_date]
    if limit and len(daily_games) > limit:
        daily_games = daily_games[:limit]
        print(f"   [LIMIT] Restricted to first {limit} games")
    print(f"   ✅ Found {len(daily_games)} games for {target_date}")

    if not daily_games:
        print(f"   ℹ️ No games scheduled for {target_date}. Continuing with status/stats update...")

    # 2. Game detail crawler with shared resolver
    print("\n🎮 Step 2: Crawling game details (BoxScore)...")
    today_kst = _today_kst()
    resolver_session = SessionLocal()
    try:
        resolver = PlayerIdResolver(resolver_session)
        resolver.preload_season_index(year)
        g_crawler = GameDetailCrawler(resolver=resolver)

        success_count = 0
        failed_count = 0
        for game in daily_games:
            game_id = game["game_id"]
            print(f"   📡 Processing Game: {game_id}")
            try:
                detail = await g_crawler.crawl_game(game_id, target_date)
                if detail:
                    save_success = save_game_detail(detail)
                    if save_success:
                        print(f"   ✅ Successfully saved {game_id}")
                        success_count += 1
                    else:
                        print(f"   ❌ Failed to save {game_id} to local DB")
                        failed_count += 1
                        fallback = _failure_status(target_date, "save_failed", today_kst)
                        if fallback:
                            update_game_status(game_id, fallback)
                else:
                    failed_count += 1
                    reason = g_crawler.get_last_failure_reason(game_id)
                    print(f"   ⚠️ Could not fetch details for {game_id} (reason={reason or 'unknown'})")
                    fallback = _failure_status(target_date, reason, today_kst)
                    if fallback:
                        update_game_status(game_id, fallback)
            except Exception as exc:
                failed_count += 1
                print(f"   ❌ Error processing {game_id}: {exc}")
                fallback = _failure_status(target_date, "exception", today_kst)
                if fallback:
                    update_game_status(game_id, fallback)
        print(f"   ✅ Detail result success={success_count} failed={failed_count}")
    finally:
        resolver_session.close()

    # 3. Game status post-process (target date only)
    print("\n🧭 Step 3: Refreshing game status for target date...")
    status_result = refresh_game_status_for_date(target_date, today=today_kst)
    print(
        "   ✅ "
        f"total={status_result.get('total', 0)} "
        f"updated={status_result.get('updated', 0)} "
        f"counts={status_result.get('status_counts', {})}"
    )

    # 4. Cumulative Stats Update (Standard Seasonal Stats)
    print("\n📈 Step 4: Updating cumulative player stats (Current Season)...")
    try:
        print("   🏏 Updating Batting Stats...")
        await asyncio.to_thread(
            crawl_series_batting_stats,
            year=year,
            series_key='regular',
            save_to_db=True,
            headless=headless,
            limit=limit,
        )

        print("\n   ⚾ Updating Pitching Stats...")
        await asyncio.to_thread(
            crawl_pitcher_series,
            year=year,
            series_key='regular',
            save_to_db=True,
            headless=headless,
            limit=limit,
        )

        print(f"\n   ✅ Local cumulative stats for {year} regular season updated")
    except Exception as exc:
        print(f"   ❌ Error during stats update: {exc}")

    print("\n✨ Local data update sequence finished.")

    # 5. Sync to Supabase
    if sync:
        print("\n☁️ Step 5: Synchronizing to Supabase...")
        try:
            print("   🔗 Syncing Game Details...")
            sync_main(["--game-details"])

            print("   🔗 Syncing Player Season Stats...")
            sync_main([])

            print("   ✅ Supabase synchronization completed")
        except Exception as exc:
            print(f"   ❌ Error during Supabase sync: {exc}")

    print(f"\n{'='*60}")
    print(f"🏁 Daily Update Finished for {target_date}")
    print(f"{'='*60}\n")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="KBO Daily Data Update Orchestrator")
    parser.add_argument(
        "--date",
        type=str,
        help="Target date in YYYYMMDD format. Defaults to yesterday in KST.",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Whether to sync data to Supabase after local update.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        default=True,
        help="Run crawlers with browser headless",
    )
    parser.add_argument(
        "--no-headless",
        action="store_false",
        dest="headless",
        help="Run crawlers with browser UI visible",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of games and players (for testing/debugging)",
    )
    return parser


def main(argv: Sequence[str] | None = None):
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    target_date = args.date
    if not target_date:
        target_date = (datetime.now(KST) - timedelta(days=1)).strftime("%Y%m%d")
    elif len(target_date) != 8 or not target_date.isdigit():
        print(f"❌ Invalid date format: {target_date}. Please use YYYYMMDD.")
        sys.exit(1)

    asyncio.run(run_update(target_date, sync=args.sync, headless=args.headless, limit=args.limit))


if __name__ == "__main__":
    main()
