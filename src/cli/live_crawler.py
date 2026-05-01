"""
Real-time KBO live crawler.

Polls today's schedule, captures relay events plus a lightweight scoreboard snapshot,
then explicitly syncs changed games to OCI.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import time
from datetime import datetime
from typing import Sequence

import pytz

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.naver_relay_crawler import NaverRelayCrawler
from src.crawlers.schedule_crawler import ScheduleCrawler
from src.db.engine import SessionLocal
from src.repositories.game_repository import GAME_STATUS_LIVE, save_game_snapshot, save_relay_data
from src.sync.oci_sync import OCISync
from src.utils.refresh_manifest import write_refresh_manifest
from src.utils.safe_print import safe_print as print


async def run_live_crawler_cycle(*, sync_to_oci: bool | None = None) -> bool:
    """Run one live polling cycle. Returns True when at least one game was updated."""
    seoul_tz = pytz.timezone("Asia/Seoul")
    now = datetime.now(seoul_tz)
    today_str = now.strftime("%Y%m%d")

    print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] 🚨 Live Crawl Cycle Started")

    sched_crawler = ScheduleCrawler()
    games = await sched_crawler.crawl_schedule(now.year, now.month)
    today_games = [g for g in games if g["game_date"].replace("-", "") == today_str]

    if not today_games:
        print(f"[INFO] No games scheduled for today ({today_str}).")
        return False

    # Optimization: Fetch latest game statuses from Naver to skip unnecessary crawls
    relay_crawler = NaverRelayCrawler()
    active_game_ids: set[str] = set()
    try:
        async with httpx.AsyncClient() as client:
            # We use RelayCrawler's internal methods to get the schedule context
            query = relay_crawler._schedule_query_context(today_str + "XXXXX") 
            response = await client.get(
                relay_crawler.schedule_api_base_url,
                params=query,
                headers=relay_crawler.headers,
                timeout=10.0,
            )
            if response.status_code == 200:
                payload = response.json()
                naver_games = list((payload.get("result") or {}).get("games") or [])
                for ng in naver_games:
                    status = str(ng.get("status") or "").upper()
                    # Statuses: 'BEFORE', 'RUNNING', 'RESULT', 'CANCEL'
                    if status == "RUNNING":
                        # Match Naver game to KBO game ID using team codes if possible
                        # but for simplicity, we'll just trust Naver's 'RUNNING' status
                        # to filter our today_games list in the loop below.
                        pass
                    
                # Store naver status mapping for quick lookup
                naver_status_map = {
                    (ng.get("awayTeamCode"), ng.get("homeTeamCode")): status
                    for ng in naver_games
                    if (status := ng.get("status"))
                }
            else:
                naver_status_map = {}
    except Exception as e:
        print(f"[WARN] Failed to fetch Naver live statuses: {e}")
        naver_status_map = {}

    detail_crawler = GameDetailCrawler(request_delay=0.1)
    touched_game_ids: set[str] = set()

    for game in today_games:
        game_id = game["game_id"]
        
        # Heuristic matching for Naver status
        away_nav = relay_crawler._naver_team_code(game["away_team_code"])
        home_nav = relay_crawler._naver_team_code(game["home_team_code"])
        nav_status = str(naver_status_map.get((away_nav, home_nav)) or "").upper()

        if nav_status == "CANCEL":
            print(f"[SKIP] {game_id} is CANCELLED.")
            continue
        if nav_status == "BEFORE":
            print(f"[SKIP] {game_id} has not started yet.")
            continue
        if nav_status == "RESULT":
            # If it's already COMPLETED in our DB, skip it.
            # We can check this via a quick repository call if needed, 
            # but for a simple 'live' loop, skipping 'RESULT' is generally safe
            # as run_daily_update will finalize it later.
            print(f"[SKIP] {game_id} is already finished (RESULT).")
            continue
        
        # Default to crawl if status is RUNNING or unknown
        print(f"[LIVE] 🔍 Crawling active game: {game_id} (Status: {nav_status or 'UNKNOWN'})")
        
        relay_data = await relay_crawler.crawl_game_events(game_id)
        flat_events = list((relay_data or {}).get("events") or [])
        raw_pbp_rows = list((relay_data or {}).get("raw_pbp_rows") or [])
        
        if flat_events or raw_pbp_rows:
            saved_rows = save_relay_data(
                game_id,
                flat_events,
                raw_pbp_rows=raw_pbp_rows,
                source_name="naver_live",
            )
            if saved_rows:
                touched_game_ids.add(game_id)
                print(f"[LIVE] 📝 Synced {saved_rows} relay rows for {game_id}")

            detail = await detail_crawler.crawl_game(game_id, today_str, lightweight=True)
            if detail and save_game_snapshot(detail, status=GAME_STATUS_LIVE):
                touched_game_ids.add(game_id)
                print(f"[LIVE] 📊 Updated scoreboard snapshot for {game_id}")

    manifest_path = write_refresh_manifest(
        phase="live",
        target_date=today_str,
        game_ids=touched_game_ids,
        datasets=["game", "game_metadata", "game_inning_scores", "game_events", "game_play_by_play"],
    )

    if not touched_game_ids:
        print(f"[INFO] No live games currently active right now. manifest={manifest_path}")
        return False

    should_sync = sync_to_oci if sync_to_oci is not None else bool(os.getenv("OCI_DB_URL"))
    if should_sync:
        oci_url = os.getenv("OCI_DB_URL")
        if oci_url:
            with SessionLocal() as sync_session:
                sync_engine = OCISync(oci_url, sync_session)
                try:
                    for game_id in sorted(touched_game_ids):
                        sync_engine.sync_specific_game(game_id)
                        print(f"[SYNC] ✅ Synced {game_id} to OCI.")
                finally:
                    sync_engine.close()

    print(f"[INFO] Live cycle finished. updated={len(touched_game_ids)} manifest={manifest_path}")
    return True


async def main_loop(interval_minutes: int, *, sync_to_oci: bool | None = None):
    while True:
        try:
            active = await run_live_crawler_cycle(sync_to_oci=sync_to_oci)
            if not active:
                seoul_tz = pytz.timezone("Asia/Seoul")
                now = datetime.now(seoul_tz)
                if now.hour < 12 or now.hour >= 23:
                    print("[SLEEP] Outside primary live window. Sleeping for 30 minutes.")
                    await asyncio.sleep(1800)
                else:
                    print("[WAIT] No live games. Next check in 5 minutes.")
                    await asyncio.sleep(300)
            else:
                print(f"[WAIT] Waiting {interval_minutes} minutes for next live cycle...")
                await asyncio.sleep(interval_minutes * 60)
        except Exception as exc:
            print(f"[CRITICAL ERROR] Live loop crashed: {exc}")
            await asyncio.sleep(60)


def main(argv: Sequence[str] | None = None):
    parser = argparse.ArgumentParser(description="KBO Live Score & PBP Daemon")
    parser.add_argument("--interval", type=int, default=2, help="Crawling polling interval in minutes")
    parser.add_argument("--run-once", action="store_true", help="Run precisely one cycle and exit")
    parser.add_argument("--no-sync", action="store_true", help="Skip explicit OCI sync after local writes")
    args = parser.parse_args(argv)

    if args.run_once:
        asyncio.run(run_live_crawler_cycle(sync_to_oci=not args.no_sync))
    else:
        print(f"🚀 Starting Real-time Daemon... Polling every {args.interval}m.")
        asyncio.run(main_loop(args.interval, sync_to_oci=not args.no_sync))


if __name__ == "__main__":
    main()
