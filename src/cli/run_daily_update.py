"""
KBO Daily Data Update Orchestrator.

This entrypoint is the postgame finalize + daily reconciliation job.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence
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
from src.services.postgame_reconciliation_service import (
    format_reconciliation_report,
    reconcile_postgame_range,
)
from src.services.schedule_collection_service import save_schedule_games
from src.services.game_write_contract import GameWriteContract
from src.sync.oci_sync import OCISync
from src.utils.refresh_manifest import write_refresh_manifest
from src.utils.safe_print import safe_print as print
from src.utils.schedule_validation import is_detail_candidate_game
from src.utils.team_codes import normalize_kbo_game_id

KST = ZoneInfo("Asia/Seoul")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DAILY_SUMMARY_DIR = PROJECT_ROOT / "logs" / "daily_update_summary"
OCI_SKIP_KEYS = (
    "skipped_schedule_only",
    "skipped_incomplete_detail",
    "skipped_empty_relay",
    "skipped_cancelled",
)


def _today_kst() -> date:
    return datetime.now(KST).date()


def _format_counts(counts: dict[str, int]) -> str:
    parts = [f"{key}={value}" for key, value in sorted(counts.items()) if value]
    return ", ".join(parts) if parts else "none"


def _failure_reason_summary(items: Mapping[str, object]) -> tuple[dict[str, int], dict[str, list[str]]]:
    counter = Counter()
    game_ids_by_reason: dict[str, list[str]] = {}
    for game_id, item in items.items():
        reason = getattr(item, "failure_reason", None)
        if reason:
            reason_text = str(reason)
            counter[reason_text] += 1
            if game_id:
                game_ids_by_reason.setdefault(reason_text, []).append(str(game_id))
    return (
        dict(counter),
        {reason: sorted(set(game_ids)) for reason, game_ids in sorted(game_ids_by_reason.items())},
    )


def _merge_oci_skip_summary(
    counter: dict[str, int],
    game_ids_by_reason: dict[str, list[str]],
    result: object,
    game_id: str,
) -> None:
    if not isinstance(result, dict):
        return

    for key in OCI_SKIP_KEYS:
        raw_value = result.get(key)
        if not raw_value:
            continue
        if isinstance(raw_value, Sequence) and not isinstance(raw_value, (str, bytes, bytearray)):
            value = len(raw_value)
            skipped_ids = [str(item) for item in raw_value if item]
        else:
            try:
                value = int(raw_value)
            except (TypeError, ValueError):
                continue
            skipped_ids = [game_id] if value else []
        if value:
            counter[key] = counter.get(key, 0) + value
            game_ids_by_reason.setdefault(key, []).extend(skipped_ids)


def _daily_summary_path(target_date: str, summary_dir: str | Path | None = None) -> Path:
    output_dir = Path(summary_dir) if summary_dir is not None else DEFAULT_DAILY_SUMMARY_DIR
    return output_dir / f"{target_date}.json"


def _build_stability_summary(
    *,
    detail_failure_counts: Mapping[str, int],
    detail_failure_game_ids: Mapping[str, list[str]],
    relay_recovery_target_ids: Sequence[str],
    oci_skip_counts: Mapping[str, int],
    oci_skip_game_ids: Mapping[str, list[str]],
    summary_path: Path,
) -> dict[str, Any]:
    detail_retry_candidates = sorted(
        set(detail_failure_game_ids.get("incomplete_detail", []))
        | set(detail_failure_game_ids.get("detail_payload_filtered", []))
    )
    relay_retry_candidates = sorted(set(oci_skip_game_ids.get("skipped_empty_relay", [])))
    affected_game_ids = sorted(
        {
            game_id
            for ids in [*detail_failure_game_ids.values(), *oci_skip_game_ids.values()]
            for game_id in ids
            if game_id
        }
    )
    return {
        "summary_path": str(summary_path),
        "detail": {
            "failure_counts": dict(sorted(detail_failure_counts.items())),
            "failure_game_ids": {
                reason: sorted(set(game_ids))
                for reason, game_ids in sorted(detail_failure_game_ids.items())
            },
        },
        "relay": {
            "target_count": len(set(relay_recovery_target_ids)),
            "target_game_ids": sorted(set(relay_recovery_target_ids)),
        },
        "oci": {
            "skip_counts": dict(sorted(oci_skip_counts.items())),
            "skip_game_ids": {
                reason: sorted(set(game_ids))
                for reason, game_ids in sorted(oci_skip_game_ids.items())
            },
        },
        "retry_candidates": {
            "detail": detail_retry_candidates,
            "relay": relay_retry_candidates,
        },
        "affected_game_ids": affected_game_ids,
    }


def _write_daily_update_summary(
    *,
    target_date: str,
    stability: Mapping[str, Any],
    manifest_path: Path | str,
    summary_path: Path,
) -> Path:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "phase": "postgame_finalize",
        "target_date": target_date,
        "generated_at": datetime.now(KST).isoformat(),
        "manifest_path": str(manifest_path),
        "stability": dict(stability),
    }
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary_path


def format_stability_alert_summary(result: object) -> str | None:
    if not isinstance(result, dict):
        return None
    stability = result.get("stability")
    if not isinstance(stability, dict):
        return None

    detail = stability.get("detail") if isinstance(stability.get("detail"), dict) else {}
    relay = stability.get("relay") if isinstance(stability.get("relay"), dict) else {}
    oci = stability.get("oci") if isinstance(stability.get("oci"), dict) else {}
    detail_counts = detail.get("failure_counts") if isinstance(detail, dict) else {}
    oci_counts = oci.get("skip_counts") if isinstance(oci, dict) else {}
    relay_targets = relay.get("target_count", 0) if isinstance(relay, dict) else 0

    return (
        f"target_date={result.get('target_date', 'unknown')} "
        f"detail_failures={_format_counts(detail_counts if isinstance(detail_counts, dict) else {})} "
        f"relay_targets={relay_targets} "
        f"oci_skips={_format_counts(oci_counts if isinstance(oci_counts, dict) else {})}"
    )


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
    summary_dir: str | Path | None = None,
    seed_tomorrow_preview: bool = False,
    run_auto_healer: bool = True,
    run_postgame_reconciliation: bool = True,
    postgame_reconcile_lookback_days: int = 3,
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
    reconciliation_changed_ids: list[str] = []
    reconciliation_dates: list[str] = []
    detail_failure_counts: dict[str, int] = {}
    detail_failure_game_ids: dict[str, list[str]] = {}
    relay_recovery_target_ids: set[str] = set()
    oci_skip_counts: dict[str, int] = {}
    oci_skip_game_ids: dict[str, list[str]] = {}
    write_contract = GameWriteContract(run_label=f"daily_update:{target_date}", log=print)

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
    schedule_result = save_schedule_games(
        schedule_games,
        log=print,
        write_contract=write_contract,
        source_reason=f"monthly_schedule_refresh:{year}-{month:02d}",
    )
    print(
        f"   ✅ Schedule discovered={schedule_result.discovered} "
        f"saved={schedule_result.saved} failed={schedule_result.failed}"
    )

    daily_games = [g for g in schedule_games if str(g.get("game_date", "")).replace("-", "") == target_date]
    detail_games = [g for g in daily_games if is_detail_candidate_game(g, today=today_kst)]
    skipped_detail_games = len(daily_games) - len(detail_games)
    if skipped_detail_games:
        print(f"   ℹ️ Skipping {skipped_detail_games} non-detail schedule games")
    if limit and len(detail_games) > limit:
        detail_games = detail_games[:limit]
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
            detail_games,
            detail_crawler=g_crawler,
            force=True,
            concurrency=1,
            log=print,
            write_contract=write_contract,
            source_reason=f"postgame_finalize:{target_date}",
        )
        processed_game_ids = list(collection_result.processed_game_ids)
        detail_failure_counts, detail_failure_game_ids = _failure_reason_summary(collection_result.items)

        for game in detail_games:
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
        if detail_failure_counts:
            print(f"   ℹ️ Detail failure reasons: {_format_counts(detail_failure_counts)}")

        if run_postgame_reconciliation:
            reconcile_start = (
                datetime.strptime(target_date, "%Y%m%d")
                - timedelta(days=max(0, postgame_reconcile_lookback_days))
            ).strftime("%Y%m%d")
            print(
                "\n🧩 Step 2.5: Reconciling recently started games "
                f"({reconcile_start}~{target_date})..."
            )
            reconciliation_result = await reconcile_postgame_range(
                reconcile_start,
                target_date,
                detail_crawler=g_crawler,
                concurrency=1,
                log=print,
                write_contract=write_contract,
                source_reason=f"postgame_reconciliation:{reconcile_start}-{target_date}",
            )
            reconciliation_changed_ids = reconciliation_result.changed_game_ids
            reconciliation_dates = sorted({change.game_date for change in reconciliation_result.changes})
            print(
                "   ✅ "
                f"candidates={reconciliation_result.candidates} "
                f"changed={len(reconciliation_result.changes)}"
            )
            if reconciliation_result.changes:
                for line in format_reconciliation_report(reconciliation_result.changes).splitlines():
                    print(f"   {line}")
        else:
            print("\n🧩 Step 2.5: Postgame reconciliation skipped.")
    except Exception as exc:
        print(f"   ❌ Error processing daily details: {exc}")
        if detail_games:
            detail_failure_counts["exception"] = detail_failure_counts.get("exception", 0) + len(detail_games)
            detail_failure_game_ids.setdefault("exception", []).extend(
                str(game["game_id"]) for game in detail_games if game.get("game_id")
            )
        for game in detail_games:
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
        relay_game_ids = sorted(set(processed_game_ids) | set(reconciliation_changed_ids))
        if relay_game_ids:
            relay_recovery_target_ids.update(relay_game_ids)
            print(f"   ℹ️ Relay candidates={len(relay_game_ids)}")
            runner(["scripts/fetch_kbo_pbp.py", "--game-ids", ",".join(relay_game_ids)])
        else:
            print("   ℹ️ No detail-success relay candidates for target date")

        healer_ids_by_date: dict[str, set[str]] = {}
        for item in healer_recovery_targets:
            game_id = item["game_id"]
            if game_id in relay_game_ids:
                continue
            healer_ids_by_date.setdefault(item["game_date"], set()).add(game_id)
        for recovery_date in sorted(healer_ids_by_date):
            healer_ids = sorted(healer_ids_by_date[recovery_date])
            relay_recovery_target_ids.update(healer_ids)
            runner(["scripts/fetch_kbo_pbp.py", "--game-ids", ",".join(healer_ids)])
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
    # Identify unique season types from today's games
    active_series = sorted({g.get("season_type", "regular") for g in daily_games if g.get("season_type")})
    if not active_series:
        active_series = ["regular"] # Fallback

    print(f"   🔍 Active series detected: {active_series}")

    try:
        for series_key in active_series:
            print(f"   [{series_key}] Updating Batting Stats...")
            await asyncio.to_thread(
                crawl_series_batting_stats,
                year=year,
                series_key=series_key,
                save_to_db=True,
                headless=headless,
                limit=limit,
            )
            print(f"   [{series_key}] Updating Pitching Stats...")
            await asyncio.to_thread(
                crawl_pitcher_series,
                year=year,
                series_key=series_key,
                save_to_db=True,
                headless=headless,
                limit=limit,
            )
        print(f"   ✅ Local cumulative stats for {year} {active_series} series updated")
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

    print("\n📈 Step 10.6: Calculating advanced Sabermetrics (wOBA, wRC+, WAR)...")
    try:
        runner(["-m", "src.cli.calculate_sabermetrics", "--years", str(year)])
        print("   ✅ Sabermetrics engine completed successfully")
    except Exception as exc:
        print(f"   ❌ Error calculating Sabermetrics: {exc}")

    print("\n🕵️  Step 10.5: Auditing season stats vs transactional details (Auto-fix enabled)...")
    try:
        runner(["scripts/verification/audit_fallback_stats.py", "--year", str(year), "--type", "all", "--fix"])
        print("   ✅ Statistical audit and auto-remediation complete")
    except Exception as exc:
        print(f"   ⚠️ Statistical audit/fix found issues (see logs): {exc}")

    candidate_sync_game_ids = sorted(
        {game["game_id"] for game in daily_games}
        | set(processed_game_ids)
        | set(reconciliation_changed_ids)
        | {item["game_id"] for item in healer_recovery_targets}
    )
    freshness_dates = sorted(
        {target_date}
        | set(reconciliation_dates)
        | {item["game_date"] for item in healer_recovery_targets}
    )

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
                # 1. Sync specific targeted games (full detail)
                for game_id in candidate_sync_game_ids:
                    sync_result = syncer.sync_specific_game(game_id)
                    _merge_oci_skip_summary(oci_skip_counts, oci_skip_game_ids, sync_result, game_id)

                # 2. Sync all parent games for the year (to ensure future schedules are pushed)
                # This fixes the issue where D+1 games are present locally but missing in OCI.
                syncer.sync_games(filters=[Game.game_id.like(f"{year}%")])

                syncer.sync_standings(year=year)
                syncer.sync_matchups(year=year)
                syncer.sync_stat_rankings(year=year)
                syncer.sync_player_season_batting(year=year)
                syncer.sync_player_season_pitching(year=year)
                syncer.sync_player_movements()
                syncer.sync_daily_rosters()
                syncer.sync_players()
                print("   ✅ OCI synchronization completed")
                if oci_skip_counts:
                    print(f"   ℹ️ OCI skip summary: {_format_counts(oci_skip_counts)}")
            finally:
                syncer.close()

        print("\n🧪 Step 13.5: Freshness gate after OCI publish...")
        for freshness_date in freshness_dates:
            runner(["-m", "src.cli.freshness_gate", "--date", freshness_date, "--source-url-env", "OCI_DB_URL"])
        print("   ✅ OCI freshness gate passed")

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

    summary_path = _daily_summary_path(target_date, summary_dir)
    stability_summary = _build_stability_summary(
        detail_failure_counts=detail_failure_counts,
        detail_failure_game_ids=detail_failure_game_ids,
        relay_recovery_target_ids=sorted(relay_recovery_target_ids),
        oci_skip_counts=oci_skip_counts,
        oci_skip_game_ids=oci_skip_game_ids,
        summary_path=summary_path,
    )

    manifest_path = write_refresh_manifest(
        phase="postgame_finalize",
        target_date=target_date,
        game_ids=sorted(set(processed_game_ids) | set(reconciliation_changed_ids))
        or [game["game_id"] for game in daily_games],
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
        stability=stability_summary,
    )
    _write_daily_update_summary(
        target_date=target_date,
        stability=stability_summary,
        manifest_path=manifest_path,
        summary_path=summary_path,
    )

    print(write_contract.summary())
    print(
        "🔎 Stability summary: "
        f"detail_failures={_format_counts(detail_failure_counts)} "
        f"relay_targets={len(relay_recovery_target_ids)} "
        f"oci_skips={_format_counts(oci_skip_counts)}"
    )

    print(f"\n{'=' * 60}")
    print(f"🏁 Daily Finalize Finished for {target_date}")
    print(f"📄 Refresh Manifest: {manifest_path}")
    print(f"📄 Daily Summary: {summary_path}")
    print(f"{'=' * 60}\n")

    return {
        "phase": "postgame_finalize",
        "target_date": target_date,
        "manifest_path": str(manifest_path),
        "summary_path": str(summary_path),
        "stability": stability_summary,
    }


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
        "--summary-dir",
        type=str,
        help="Directory for daily stability summary JSON. Defaults to logs/daily_update_summary.",
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
    parser.add_argument(
        "--skip-postgame-reconciliation",
        action="store_true",
        help="Skip the recent started-game reconciliation pass.",
    )
    parser.add_argument(
        "--postgame-reconcile-lookback-days",
        type=int,
        default=3,
        help="Number of days before --date to revisit for started-game reconciliation.",
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

    return asyncio.run(
        run_update(
            target_date,
            sync=args.sync,
            headless=args.headless,
            limit=args.limit,
            summary_dir=args.summary_dir,
            seed_tomorrow_preview=args.seed_tomorrow_preview,
            run_auto_healer=not args.skip_auto_healer,
            run_postgame_reconciliation=not args.skip_postgame_reconciliation,
            postgame_reconcile_lookback_days=args.postgame_reconcile_lookback_days,
        )
    )


if __name__ == "__main__":
    main()
