"""
KBO Daily Data Update Orchestrator.

This entrypoint is the postgame finalize + daily reconciliation job.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, datetime, timedelta
from typing import Callable, Optional, Sequence
from zoneinfo import ZoneInfo

from src.cli.auto_healer import run_healer_async
from src.cli.sync_oci import main as sync_main
from src.crawlers.daily_roster_crawler import DailyRosterCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.player_batting_all_series_crawler import crawl_series_batting_stats
from src.crawlers.player_movement_crawler import PlayerMovementCrawler
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
from src.repositories.player_repository import PlayerRepository
from src.repositories.team_repository import TeamRepository
from src.services.player_id_resolver import PlayerIdResolver
from src.sync.oci_sync import OCISync
from src.utils.refresh_manifest import write_refresh_manifest
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


def _run_python_step(argv: Sequence[str]) -> None:
    import subprocess

    subprocess.run([sys.executable, *argv], check=True)


async def run_update(
    target_date: str,
    sync: bool = False,
    headless: bool = True,
    limit: int | None = None,
    *,
    step_runner: Optional[Callable[[Sequence[str]], None]] = None,
    seed_tomorrow_preview: bool = False,
    run_auto_healer: bool = True,
):
    """Main orchestration logic for postgame finalize and daily reconciliation."""
    runner = step_runner or _run_python_step

    print(f"\n{'=' * 60}")
    print(f"🚀 KBO Daily Finalize Started for Date: {target_date}")
    print(f"{'=' * 60}")

    year = int(target_date[:4])
    month = int(target_date[4:6])

    if run_auto_healer:
        print("\n🩺 Step 0: Running Auto-Healer...")
        try:
            await run_healer_async(dry_run=False)
        except Exception as exc:
            print(f"   ⚠️ Auto-Healer encountered an error (continuing anyway): {exc}")
    else:
        print("\n🩺 Step 0: Auto-Healer skipped for scoped backfill run.")

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

    daily_games = [g for g in schedule_games if str(g.get("game_date", "")).replace("-", "") == target_date]
    if limit and len(daily_games) > limit:
        daily_games = daily_games[:limit]
        print(f"   [LIMIT] Restricted to first {limit} games")
    print(f"   ✅ Found {len(daily_games)} games for {target_date}")

    print("\n🎮 Step 2: Crawling full postgame details...")
    today_kst = _today_kst()
    resolver_session = SessionLocal()
    processed_game_ids: list[str] = []
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
                    if save_game_detail(detail):
                        print(f"   ✅ Successfully saved {game_id}")
                        processed_game_ids.append(game_id)
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

    print("\n🧭 Step 3: Refreshing game status for target date...")
    status_result = refresh_game_status_for_date(target_date, today=today_kst)
    print(
        "   ✅ "
        f"total={status_result.get('total', 0)} "
        f"updated={status_result.get('updated', 0)} "
        f"counts={status_result.get('status_counts', {})}"
    )

    print("\n📝 Step 4: Relay recovery (events / PBP)...")
    try:
        runner(["scripts/fetch_kbo_pbp.py", "--date", target_date])
        print("   ✅ Relay recovery complete")
    except Exception as exc:
        print(f"   ❌ Error generating relay events: {exc}")

    print("\n📝 Step 5: Post-game review/WPA generation...")
    try:
        review_args = ["-m", "src.cli.daily_review_batch", "--date", target_date]
        review_args.append("--no-sync")
        runner(review_args)
        print("   ✅ Review context generation complete")
    except Exception as exc:
        print(f"   ❌ Error generating review context: {exc}")

    print("\n📈 Step 6: Updating cumulative player stats...")
    try:
        print("   🏏 Updating Batting Stats...")
        await asyncio.to_thread(
            crawl_series_batting_stats,
            year=year,
            series_key="regular",
            save_to_db=True,
            headless=headless,
            limit=limit,
        )
        print("   ⚾ Updating Pitching Stats...")
        await asyncio.to_thread(
            crawl_pitcher_series,
            year=year,
            series_key="regular",
            save_to_db=True,
            headless=headless,
            limit=limit,
        )
        print(f"   ✅ Local cumulative stats for {year} regular season updated")
    except Exception as exc:
        print(f"   ❌ Error during stats update: {exc}")

    print("\n🔄 Step 7: Updating player movements and daily rosters...")
    try:
        m_crawler = PlayerMovementCrawler()
        movements = await m_crawler.crawl_years(year, year)
        if movements:
            m_repo = PlayerRepository()
            m_count = m_repo.save_player_movements(movements)
            print(f"   ✅ Saved {m_count} player movements for {year}")

        r_target_date = datetime.strptime(target_date, "%Y%m%d").strftime("%Y-%m-%d")
        r_crawler = DailyRosterCrawler()
        rosters = await r_crawler.crawl_date_range(r_target_date, r_target_date)
        if rosters:
            with SessionLocal() as session:
                r_repo = TeamRepository(session)
                r_count = r_repo.save_daily_rosters(rosters)
                print(f"   ✅ Saved {r_count} daily roster records for {r_target_date}")
    except Exception as exc:
        print(f"   ❌ Error updating player movements/rosters: {exc}")

    derived_refresh: list[str] = []

    print("\n📊 Step 8: Rebuilding derived standings...")
    try:
        runner(["-m", "src.cli.calculate_standings", "--year", str(year)])
        derived_refresh.append("standings")
    except Exception as exc:
        print(f"   ❌ Error calculating standings: {exc}")

    print("\n🧮 Step 9: Recalculating matchup splits...")
    try:
        runner(["-m", "src.cli.calculate_matchups", "--year", str(year)])
        derived_refresh.append("matchups")
        print("   ✅ Matchup splits recalculated successfully")
    except Exception as exc:
        print(f"   ❌ Error recalculating matchups: {exc}")

    print("\n🏷️ Step 10: Recalculating stat rankings...")
    try:
        runner(["-m", "src.cli.calculate_rankings", "--year", str(year)])
        derived_refresh.append("stat_rankings")
        print("   ✅ Stat rankings recalculated successfully")
    except Exception as exc:
        print(f"   ❌ Error recalculating stat rankings: {exc}")

    candidate_sync_game_ids = sorted({game["game_id"] for game in daily_games} | set(processed_game_ids))

    if sync:
        print("\n🧪 Step 11: Freshness gate before OCI publish...")
        runner(["-m", "src.cli.freshness_gate", "--date", target_date])
        print("   ✅ Freshness gate passed")

        print("\n☁️ Step 12: Synchronizing to OCI...")
        oci_url = os.getenv("OCI_DB_URL")
        if not oci_url:
            raise RuntimeError("OCI_DB_URL is required when --sync is enabled")

        with SessionLocal() as sync_session:
            syncer = OCISync(oci_url, sync_session)
            try:
                for game_id in candidate_sync_game_ids:
                    syncer.sync_specific_game(game_id)
                syncer.sync_standings(year=year)
                syncer.sync_matchups(year=year)
                syncer.sync_stat_rankings(year=year)
                syncer.sync_player_season_batting()
                syncer.sync_player_season_pitching()
                syncer.sync_player_movements()
                syncer.sync_daily_rosters()
                syncer.sync_players()
                print("   ✅ OCI synchronization completed")
            finally:
                syncer.close()

    if seed_tomorrow_preview:
        tomorrow_date = (datetime.strptime(target_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
        print(f"\n🔮 Step 13: Seeding tomorrow preview contexts ({tomorrow_date})...")
        try:
            preview_args = ["-m", "src.cli.daily_preview_batch", "--date", tomorrow_date]
            if not sync:
                preview_args.append("--no-sync")
            runner(preview_args)
            print("   ✅ Tomorrow preview seed complete")
        except Exception as exc:
            print(f"   ❌ Error generating tomorrow preview seed: {exc}")

    manifest_path = write_refresh_manifest(
        phase="postgame_finalize",
        target_date=target_date,
        game_ids=processed_game_ids or [game["game_id"] for game in daily_games],
        datasets=[
            "game",
            "game_metadata",
            "game_inning_scores",
            "game_lineups",
            "game_events",
            "game_summary",
            "game_play_by_play",
        ],
        derived_refresh=derived_refresh,
    )

    print(f"\n{'=' * 60}")
    print(f"🏁 Daily Finalize Finished for {target_date}")
    print(f"📄 Refresh Manifest: {manifest_path}")
    print(f"{'=' * 60}\n")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="KBO Daily Data Finalize Orchestrator")
    parser.add_argument(
        "--date",
        type=str,
        help="Target date in YYYYMMDD format. Defaults to yesterday in KST.",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Whether to sync data to OCI after local update.",
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
    parser.add_argument(
        "--seed-tomorrow-preview",
        action="store_true",
        help="Optionally seed tomorrow preview data after finalize.",
    )
    parser.add_argument(
        "--skip-auto-healer",
        action="store_true",
        help="Skip global past-game auto-healing for scoped backfill runs.",
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

    asyncio.run(
        run_update(
            target_date,
            sync=args.sync,
            headless=args.headless,
            limit=args.limit,
            seed_tomorrow_preview=args.seed_tomorrow_preview,
            run_auto_healer=not args.skip_auto_healer,
        )
    )


if __name__ == "__main__":
    main()
