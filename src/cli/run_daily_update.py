"""
KBO Daily Data Update Orchestrator.

This entrypoint is the postgame finalize + daily reconciliation job.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
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
from src.models.game import Game
from src.repositories.game_repository import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_UNRESOLVED,
    refresh_game_status_for_date,
    update_game_status,
)
from src.repositories.player_repository import PlayerRepository
from src.repositories.team_repository import TeamRepository
from src.services.game_collection_service import crawl_and_save_game_details
from src.services.player_id_resolver import PlayerIdResolver
from src.services.schedule_collection_service import save_schedule_games
from src.sync.oci_sync import OCISync
from src.utils.refresh_manifest import write_refresh_manifest
from src.utils.safe_print import safe_print as print
from src.utils.team_codes import normalize_kbo_game_id

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


def _collect_past_scheduled_recovery_targets(today: date) -> list[dict[str, str]]:
    """Capture auto-healer candidates so repaired past games can be finalized and synced."""
    yesterday = today - timedelta(days=1)
    try:
        with SessionLocal() as session:
            rows = (
                session.query(Game.game_id, Game.game_date)
                .filter(
                    Game.game_status == GAME_STATUS_SCHEDULED,
                    Game.game_date <= yesterday,
                )
                .order_by(Game.game_date.asc(), Game.game_id.asc())
                .all()
            )
    except Exception as exc:
        print(f"   ⚠️ Could not inspect auto-healer recovery candidates: {exc}")
        return []

    return [
        {
            "game_id": normalize_kbo_game_id(game_id),
            "game_date": _format_target_date(game_date, fallback_game_id=game_id),
        }
        for game_id, game_date in rows
        if game_id
    ]


def _format_target_date(value: object, *, fallback_game_id: str) -> str:
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    text = str(value or "").replace("-", "")
    if len(text) == 8 and text.isdigit():
        return text
    return fallback_game_id[:8]


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
    today_kst = _today_kst()
    healer_recovery_targets: list[dict[str, str]] = []

    if run_auto_healer:
        print("\n🩺 Step 0: Running Auto-Healer...")
        healer_recovery_targets = _collect_past_scheduled_recovery_targets(today_kst)
        try:
            await run_healer_async(dry_run=False)
        except Exception as exc:
            print(f"   ⚠️ Auto-Healer encountered an error (continuing anyway): {exc}")
            healer_recovery_targets = []
        if healer_recovery_targets:
            print(f"   ✅ Auto-Healer recovery candidates tracked: {len(healer_recovery_targets)}")
    else:
        print("\n🩺 Step 0: Auto-Healer skipped for scoped backfill run.")

    print("\n📅 Step 1: Crawling + saving monthly schedule...")
    s_crawler = ScheduleCrawler()
    schedule_games = await s_crawler.crawl_schedule(year, month)
    schedule_result = save_schedule_games(schedule_games, log=print)
    print(
        f"   ✅ Schedule discovered={schedule_result.discovered} "
        f"saved={schedule_result.saved} failed={schedule_result.failed}"
    )

    daily_games = [g for g in schedule_games if str(g.get("game_date", "")).replace("-", "") == target_date]
    if limit and len(daily_games) > limit:
        daily_games = daily_games[:limit]
        print(f"   [LIMIT] Restricted to first {limit} games")
    print(f"   ✅ Found {len(daily_games)} games for {target_date}")

    print("\n🎮 Step 2: Crawling full postgame details...")
    resolver_session = SessionLocal()
    processed_game_ids: list[str] = []
    try:
        resolver = PlayerIdResolver(resolver_session)
        resolver.preload_season_index(year)
        g_crawler = GameDetailCrawler(resolver=resolver)

        collection_result = await crawl_and_save_game_details(
            daily_games,
            detail_crawler=g_crawler,
            force=True,
            concurrency=1,
            log=print,
        )
        processed_game_ids = list(collection_result.processed_game_ids)

        for game in daily_games:
            game_id = game["game_id"]
            item = collection_result.items.get(normalize_kbo_game_id(game_id))
            if item and item.detail_saved:
                print(f"   ✅ Successfully saved {game_id}")
                continue

            reason = item.failure_reason if item else "exception"
            if item and item.detail_status == "save_failed":
                print(f"   ❌ Failed to save {game_id} to local DB")
            else:
                print(f"   ⚠️ Could not fetch details for {game_id} (reason={reason or 'unknown'})")
            fallback = _failure_status(target_date, reason, today_kst)
            if fallback:
                update_game_status(game_id, fallback)
        print(
            f"   ✅ Detail result success={collection_result.detail_saved} "
            f"failed={collection_result.detail_failed}"
        )
    except Exception as exc:
        print(f"   ❌ Error processing daily details: {exc}")
        for game in daily_games:
            game_id = game["game_id"]
            fallback = _failure_status(target_date, "exception", today_kst)
            if fallback:
                update_game_status(game_id, fallback)
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
        for recovery_date in sorted({item["game_date"] for item in healer_recovery_targets} - {target_date}):
            runner(["scripts/fetch_kbo_pbp.py", "--date", recovery_date])
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

    print("\n🩹 Step 6.5: Backfilling starting pitchers from stats...")
    try:
        backfill_args = [
            "-m",
            "src.cli.backfill_starting_pitchers_from_stats",
            "--start-date",
            target_date,
            "--end-date",
            target_date,
        ]
        if sync:
            backfill_args.append("--sync")
        runner(backfill_args)
        print("   ✅ Starting pitcher backfill complete")
    except Exception as exc:
        print(f"   ❌ Error during pitcher backfill: {exc}")

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

    candidate_sync_game_ids = sorted(
        {game["game_id"] for game in daily_games}
        | set(processed_game_ids)
        | {item["game_id"] for item in healer_recovery_targets}
    )
    freshness_dates = sorted({target_date} | {item["game_date"] for item in healer_recovery_targets})

    if sync:
        print("\n🧪 Step 11: Freshness gate before OCI publish...")
        for freshness_date in freshness_dates:
            runner(["-m", "src.cli.freshness_gate", "--date", freshness_date])
        print("   ✅ Freshness gate passed")

        print("\n⚖️ Step 12: Statistical quality gate check...")
        try:
            runner(["-m", "src.cli.quality_gate_check", "--year", str(year)])
            print("   ✅ Statistical quality gate passed")
        except subprocess.CalledProcessError as exc:
            print(f"   ⚠️ Statistical quality gate failed (continuing OCI game publish): {exc}")

        print("\n☁️ Step 13: Synchronizing to OCI...")
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
        print(f"\n🔮 Step 14: Seeding tomorrow preview contexts ({tomorrow_date})...")
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
