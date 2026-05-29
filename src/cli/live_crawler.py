"""
Real-time KBO live crawler.

Polls today's schedule, captures relay events plus a lightweight scoreboard snapshot,
then explicitly syncs changed games to OCI.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import datetime
from typing import Sequence
from zoneinfo import ZoneInfo

import httpx

logger = logging.getLogger(__name__)

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.naver_relay_crawler import NaverRelayCrawler
from src.crawlers.schedule_crawler import ScheduleCrawler
from src.db.engine import SessionLocal
from src.repositories.game_repository import GAME_STATUS_LIVE, save_game_snapshot, save_relay_data
from src.sync.oci_sync import OCISync
from src.utils.refresh_manifest import write_refresh_manifest
from src.utils.safe_print import safe_print as print


async def run_live_crawler_cycle(*, sync_to_oci: bool | None = None) -> tuple[bool, bool]:
    """Run one live polling cycle. Returns (active, active_playing)."""
    seoul_tz = ZoneInfo("Asia/Seoul")
    now = datetime.now(seoul_tz)
    today_str = now.strftime("%Y%m%d")

    print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] 🚨 Live Crawl Cycle Started")

    sched_crawler = ScheduleCrawler()
    games = await sched_crawler.crawl_schedule(now.year, now.month)
    today_games = [g for g in games if g["game_date"].replace("-", "") == today_str]

    if not today_games:
        print(f"[INFO] No games scheduled for today ({today_str}).")
        return False, False

    # Optimization: Fetch latest game statuses from Naver to skip unnecessary crawls
    relay_crawler = NaverRelayCrawler()
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
    except Exception:
        logger.exception("[WARN] Failed to fetch Naver live statuses")
        naver_status_map = {}

    detail_crawler = GameDetailCrawler(request_delay=0.1)
    touched_game_ids: set[str] = set()
    active_playing_flag = False

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

        # Determine if actively playing (not inning change)
        game_is_playing = True
        if flat_events:
            # If the last event is 3 outs, it's an inning change
            if flat_events[-1].get("outs") == 3:
                game_is_playing = False
        elif raw_pbp_rows and "종료" in str(raw_pbp_rows[-1].get("play_description", "")):
            game_is_playing = False

        if game_is_playing:
            active_playing_flag = True

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
        return False, False

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
    return True, active_playing_flag


async def main_loop(base_interval_minutes: int, *, sync_to_oci: bool | None = None, dynamic: bool = False):
    while True:
        try:
            # Use seoul time for window logic
            seoul_tz = ZoneInfo("Asia/Seoul")
            now = datetime.now(seoul_tz)

            # 1. Run the cycle
            active, active_playing = await run_live_crawler_cycle(sync_to_oci=sync_to_oci)

            # 2. Determine next sleep interval
            if not dynamic:
                # Traditional fixed interval
                sleep_seconds = base_interval_minutes * 60 if active else 300
            else:
                # Dynamic Interval Logic
                if active:
                    # Active games running: poll frequently (5-10s playing, 30s inning change)
                    sleep_seconds = 5 if active_playing else 30
                elif 12 <= now.hour < 23:
                    # During game hours but no active games right now (e.g., between games or pre-game)
                    sleep_seconds = 120  # 2 minutes
                else:
                    # Outside game hours
                    sleep_seconds = 1800  # 30 minutes

            print(f"[WAIT] Next check in {sleep_seconds} seconds...")
            await asyncio.sleep(sleep_seconds)

        except Exception:
            logger.exception("[CRITICAL ERROR] Live loop crashed")
            await asyncio.sleep(60)


def main(argv: Sequence[str] | None = None):
    parser = argparse.ArgumentParser(description="KBO Live Score & PBP Daemon")
    parser.add_argument(
        "--interval", type=int, default=2, help="Crawling polling interval in minutes (default for fixed mode)"
    )
    parser.add_argument(
        "--dynamic", action="store_true", help="Enable dynamic polling (20s when active, 2m pre-game, 30m off-hours)"
    )
    parser.add_argument("--run-once", action="store_true", help="Run precisely one cycle and exit")
    parser.add_argument("--no-sync", action="store_true", help="Skip explicit OCI sync after local writes")
    args = parser.parse_args(argv)

    if args.run_once:
        asyncio.run(run_live_crawler_cycle(sync_to_oci=not args.no_sync))
    else:
        mode = "DYNAMIC" if args.dynamic else f"FIXED ({args.interval}m)"
        print(f"🚀 Starting Real-time Daemon... Mode: {mode}")
        asyncio.run(main_loop(args.interval, sync_to_oci=not args.no_sync, dynamic=args.dynamic))


if __name__ == "__main__":
    main()
