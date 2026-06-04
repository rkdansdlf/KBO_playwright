"""
KBO Pipeline Auto-Healer

Two healing modes:

1. Default mode (stuck/inconsistent games):
   Detects past games stuck in SCHEDULED state (no scores) or with logic inconsistencies
   and attempts to self-correct by re-crawling from the KBO GameCenter.

   Resolution logic per game:
     - shared detail collection saved data → COMPLETED
     - failure_reason=cancelled            → update status → CANCELLED
     - failure_reason=missing              → update status → UNRESOLVED_MISSING

2. PBP mode (--pbp flag):
   Scans game_metadata for games with pbp_validation_status='unverified' in COMPLETED/DRAW state,
   re-crawls PBP data from the KBO official website, re-validates, and sends Telegram notifications.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Sequence

from sqlalchemy import select, text

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.db.engine import SessionLocal
from src.models.game import Game
from src.repositories.game_repository import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_UNRESOLVED,
    update_game_status,
)
from src.services.game_collection_service import crawl_and_save_game_details
from src.services.game_write_contract import GameWriteContract
from src.services.player_id_resolver import PlayerIdResolver
from src.services.recovery_manager import RecoveryManager
from src.utils.alerting import SlackWebhookClient, TelegramBotClient
from src.utils.safe_print import safe_print as print

logger = logging.getLogger(__name__)


def _find_stuck_games() -> list[Game]:
    """Return all past games whose status is still SCHEDULED (no scores yet)."""
    yesterday = datetime.now().date() - timedelta(days=1)
    with SessionLocal() as session:
        stmt = select(Game).where(
            Game.game_status == GAME_STATUS_SCHEDULED,
            Game.game_date <= yesterday,
        )
        return list(session.execute(stmt).scalars().all())


def _find_inconsistent_games() -> list[Game]:
    """Return games where total score does not match sum of inning scores."""
    from sqlalchemy import text

    # We check all games that are in terminal COMPLETED/DRAW state
    # but have score mismatches. This usually happens due to crawler bugs.
    query = text(
        """
        SELECT g.game_id FROM game g
        JOIN (
            SELECT g.game_id, g.away_score, g.home_score,
                   COALESCE((SELECT SUM(runs) FROM game_inning_scores i WHERE i.game_id = g.game_id AND i.team_side = 'away'), 0) as away_sum,
                   COALESCE((SELECT SUM(runs) FROM game_inning_scores i WHERE i.game_id = g.game_id AND i.team_side = 'home'), 0) as home_sum
            FROM game g
            WHERE g.game_status IN ('COMPLETED', 'DRAW')
        ) sub ON g.game_id = sub.game_id
        WHERE (sub.away_score != sub.away_sum OR sub.home_score != sub.home_sum)
    """
    )
    with SessionLocal() as session:
        game_ids = session.execute(query).scalars().all()
        if not game_ids:
            return []
        stmt = select(Game).where(Game.game_id.in_(list(game_ids)))
        return list(session.execute(stmt).scalars().all())


def _apply_heal_outcome(game_id: str, item) -> str:
    """
    Apply status repair based on one shared collection result item.

    Returns one of: 'completed', 'cancelled', 'unresolved'
    """
    if item and item.detail_saved:
        print(f"  ✅ {game_id} → COMPLETED (score saved)")
        return "completed"

    failure_reason = item.failure_reason if item else None
    if failure_reason == "cancelled":
        update_game_status(game_id, GAME_STATUS_CANCELLED)
        print(f"  🚫 {game_id} → CANCELLED")
        return "cancelled"

    update_game_status(game_id, GAME_STATUS_UNRESOLVED)
    print(f"  ❓ {game_id} → UNRESOLVED_MISSING (reason={failure_reason})")
    return "unresolved"


async def run_healer_async(
    dry_run: bool = False,
    reset_checkpoint: bool = False,
    target_game_ids: list[str] | None = None,
) -> int:
    print("\n🩺 Running KBO Pipeline Auto-Healer...")

    recovery_mgr = RecoveryManager()
    if reset_checkpoint:
        recovery_mgr.clear()

    all_found = []
    stuck_games = []
    inconsistent_games = []
    if target_game_ids:
        # Targeted recovery mode
        with SessionLocal() as session:
            stmt = select(Game).where(Game.game_id.in_(target_game_ids))
            all_found = list(session.execute(stmt).scalars().all())
            print(f"🎯 Target recovery requested for {len(all_found)} specific game(s).")
    else:
        # Standard anomaly detection mode
        stuck_games = _find_stuck_games()
        inconsistent_games = _find_inconsistent_games()

        if not stuck_games and not inconsistent_games:
            print("✅ No anomalies detected. Pipeline is healthy.")
            recovery_mgr.clear()
            return 0

        all_found = sorted(
            list({g.game_id: g for g in (stuck_games + inconsistent_games)}.values()),
            key=lambda x: x.game_id,
        )

    if not all_found:
        print("✅ No games found for recovery.")
        return 0

    # Initialize or resume checkpoint
    recovery_mgr.initialize_run("default_healer_run", [g.game_id for g in all_found])

    pending_ids = set(recovery_mgr.get_pending_targets())
    recovery_candidates = [g for g in all_found if g.game_id in pending_ids]

    if not recovery_candidates:
        print("✅ All detected anomalies were already processed in current checkpoint.")
        return 0

    total = len(recovery_candidates)
    anomaly_dates = sorted({g.game_date for g in recovery_candidates})

    # Initialize variables for summary check
    stuck_games_filtered = [g for g in all_found if g.game_status == GAME_STATUS_SCHEDULED]

    if stuck_games_filtered:
        stuck_count = len([g for g in stuck_games_filtered if g.game_id in pending_ids])
        if stuck_count:
            print(f"⚠️  Anomaly Detected: {stuck_count} past game(s) stuck in SCHEDULED state!")
    if inconsistent_games:
        incon_count = len([g for g in inconsistent_games if g.game_id in pending_ids])
        if incon_count:
            print(f"⚠️  Anomaly Detected: {incon_count} game(s) with score inconsistencies!")

    for d in anomaly_dates:
        print(f"  - {d}")

    # Slack alert
    if not dry_run:
        summary_parts = []
        if stuck_games:
            summary_parts.append(f"*{len(stuck_games)}* stuck games")
        if inconsistent_games:
            summary_parts.append(f"*{len(inconsistent_games)}* inconsistent games")

        date_range = (
            f"`{anomaly_dates[0]}`" if len(anomaly_dates) == 1 else f"`{anomaly_dates[0]}` ~ `{anomaly_dates[-1]}`"
        )
        SlackWebhookClient.send_alert(
            f"Pipeline Anomaly: {total} games detected for auto-healing.",
            blocks=[
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": "⚠️ KBO Pipeline Anomaly"},
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"Found {' and '.join(summary_parts)} for auto-healing.\n"
                            f"Affected dates: {date_range}\n\n"
                            "*Auto-Healing initiated.*"
                        ),
                    },
                },
            ],
        )

    print(f"\n🚀 Initiating self-recovery for {total} game(s)...")

    with SessionLocal() as db_session:
        resolver = PlayerIdResolver(
            db_session,
            strict_game_resolution=True,
            allow_auto_register=False,
        )
        # Preload for potentially multiple years found in anomalies
        years = {d.year for d in anomaly_dates}
        for y in years:
            resolver.preload_season_index(y)

        crawler = GameDetailCrawler(request_delay=1.0, resolver=resolver)
        write_contract = GameWriteContract(
            run_label=f"auto_healer:{datetime.now():%Y%m%dT%H%M%S}",
            log=print,
        )

        results = {"completed": 0, "cancelled": 0, "unresolved": 0, "dry_run": 0}
        if dry_run:
            for game in recovery_candidates:
                print(f"  [DRY-RUN] Would re-crawl {game.game_id}")
                results["dry_run"] += 1
        else:
            collection_result = await crawl_and_save_game_details(
                [
                    {
                        "game_id": game.game_id,
                        "game_date": game.game_date.strftime("%Y%m%d"),
                    }
                    for game in recovery_candidates
                ],
                detail_crawler=crawler,
                force=True,
                concurrency=1,
                log=print,
                write_contract=write_contract,
                source_reason="auto_healing_recovery",
            )
            for game in recovery_candidates:
                item = collection_result.items.get(game.game_id)
                outcome = _apply_heal_outcome(game.game_id, item)
                results[outcome] = results.get(outcome, 0) + 1

                if outcome == "completed":
                    recovery_mgr.mark_completed(game.game_id)
                elif outcome == "unresolved":
                    recovery_mgr.mark_failed(game.game_id, item.failure_reason if item else "unknown")

            print(write_contract.summary())

    # Final Summary
    print("\n📊 Auto-Healer Summary:")
    for outcome, count in results.items():
        if count > 0:
            print(f"  {outcome}: {count}")

    if not dry_run:
        unresolved_count = results.get("unresolved", 0)
        if unresolved_count == 0:
            SlackWebhookClient.send_alert(f"✅ Auto-healing complete. {results['completed']} games recovered.")
        else:
            SlackWebhookClient.send_alert(
                f"⚠️ Auto-healing complete. {results['completed']} recovered, {unresolved_count} failed."
            )

    return results["unresolved"]


# ---------------------------------------------------------------------------
# PBP Healing Mode
# ---------------------------------------------------------------------------


def _find_unverified_pbp_games(lookback_days: int = 3) -> list[dict]:
    """
    Scan game_metadata for finished games whose PBP is still 'unverified'.

    Returns a list of dicts: {game_id, game_date, away_team, home_team, error_reason}
    """
    cutoff = (datetime.now().date() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    query = text(
        """
        SELECT
            g.game_id,
            g.game_date,
            g.away_team,
            g.home_team,
            m.source_payload
        FROM game g
        JOIN game_metadata m ON g.game_id = m.game_id
        WHERE g.game_status IN ('COMPLETED', 'DRAW')
          AND g.game_date >= :cutoff
          AND json_extract(m.source_payload, '$.pbp_validation_status') = 'unverified'
        ORDER BY g.game_date, g.game_id
        """
    )

    results = []
    with SessionLocal() as session:
        rows = session.execute(query, {"cutoff": cutoff}).fetchall()
        for row in rows:
            import json

            payload = row.source_payload
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = {}
            error_reason = payload.get("pbp_validation_error", "unknown") if isinstance(payload, dict) else "unknown"
            results.append(
                {
                    "game_id": row.game_id,
                    "game_date": str(row.game_date),
                    "away_team": row.away_team or "?",
                    "home_team": row.home_team or "?",
                    "error_reason": error_reason,
                }
            )
    return results


async def run_pbp_healer_async(
    dry_run: bool = False,
    lookback_days: int = 3,
    target_game_ids: list[str] | None = None,
) -> dict:
    """
    PBP Auto-Healer:
      1. Scan DB for unverified PBP games.
      2. Send Telegram notification with the list.
      3. Re-crawl from KBO official website (PBPCrawler).
      4. Re-validate and re-save.
      5. Send Telegram notification with the final result.

    Returns a summary dict: {found, recovered, failed, skipped}
    """
    print("\n🩺 [PBP Healer] 검증 실패 PBP 게임 스캔 중...")

    if target_game_ids:
        # Targeted mode: load metadata for specific games
        query = text(
            """
            SELECT g.game_id, g.game_date, g.away_team, g.home_team, m.source_payload
            FROM game g
            LEFT JOIN game_metadata m ON g.game_id = m.game_id
            WHERE g.game_id IN :ids
            """
        )
        results = []
        import json

        with SessionLocal() as session:
            rows = session.execute(query, {"ids": tuple(target_game_ids)}).fetchall()
            for row in rows:
                payload = row.source_payload
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except Exception:
                        payload = {}
                error_reason = (
                    payload.get("pbp_validation_error", "targeted_mode")
                    if isinstance(payload, dict)
                    else "targeted_mode"
                )
                results.append(
                    {
                        "game_id": row.game_id,
                        "game_date": str(row.game_date),
                        "away_team": row.away_team or "?",
                        "home_team": row.home_team or "?",
                        "error_reason": error_reason,
                    }
                )
    else:
        results = _find_unverified_pbp_games(lookback_days=lookback_days)

    if not results:
        print("✅ [PBP Healer] 검증 실패 PBP 게임 없음. 파이프라인 정상.")
        return {"found": 0, "recovered": 0, "failed": 0, "skipped": 0}

    found = len(results)
    print(f"⚠️  [PBP Healer] 검증 실패 게임 {found}건 발견")
    for item in results:
        print(f"   • {item['game_id']} ({item['away_team']} vs {item['home_team']}) - {item['error_reason']}")

    # --- Telegram: 발견 알림 ---
    if not dry_run:
        date_vals = sorted({item["game_date"] for item in results})
        date_range = f"{date_vals[0]}" if len(date_vals) == 1 else f"{date_vals[0]} ~ {date_vals[-1]}"
        game_lines = "\n".join(
            f"• {item['game_id']} ({item['away_team']} vs {item['home_team']}) - {item['error_reason']}"
            for item in results
        )
        discovery_msg = (
            f"⚠️ <b>PBP 검증 실패 게임 발견</b>\n\n"
            f"총 <b>{found}</b>건 PBP 이상 감지\n"
            f"대상 날짜: {date_range}\n\n"
            f"<pre>{game_lines}</pre>\n\n"
            f"🔧 자동 재크롤 시작..."
        )
        TelegramBotClient.send_message(discovery_msg)

    if dry_run:
        print("[DRY-RUN] 재크롤 생략. 실제 복구는 --pbp 없이 실행하거나 dry-run 플래그 제거.")
        return {"found": found, "recovered": 0, "failed": 0, "skipped": found}

    # --- Re-crawl through the relay source orchestrator ---
    from src.services.relay_recovery_service import RelayRecoveryTarget, recover_relay_data
    from src.sources.relay import derive_bucket_id

    targets = [
        RelayRecoveryTarget(
            game_id=item["game_id"],
            bucket_id=derive_bucket_id(item["game_id"]),
            needs_event_recovery=True,
            needs_pbp_recovery=True,
        )
        for item in results
    ]
    recovery_result = await recover_relay_data(
        targets,
        source_order_override=["kbo", "naver", "import", "manual"],
        allow_derived_pbp=False,
        sleep_seconds=0,
        log=print,
    )
    recovered_ids = {
        str(row.get("game_id"))
        for row in recovery_result.report_rows
        if str(row.get("status") or "") in {"saved", "partial_relay"}
    }
    recovered = recovery_result.saved_games
    failed = max(0, found - recovered)
    failed_games = [
        {**item, "heal_error": "orchestrator_recovery_failed"}
        for item in results
        if item["game_id"] not in recovered_ids
    ]

    # --- Telegram: 결과 알림 ---
    if recovered == found:
        result_msg = f"✅ <b>PBP 자동 치유 완료</b>\n\n복구 성공: <b>{recovered}</b>건 (전체 {found}건 모두 복구)"
    else:
        failed_lines = "\n".join(
            f"• {g['game_id']} ({g['away_team']} vs {g['home_team']}) - {g.get('heal_error', '?')}"
            for g in failed_games
        )
        result_msg = (
            f"⚠️ <b>PBP 자동 치유 완료</b>\n\n"
            f"복구 성공: <b>{recovered}</b>건\n"
            f"복구 실패: <b>{failed}</b>건\n\n"
            f"<b>실패 게임:</b>\n<pre>{failed_lines}</pre>"
        )

    TelegramBotClient.send_message(result_msg)
    print(f"\n📊 [PBP Healer] 완료 — 발견 {found}, 복구 {recovered}, 실패 {failed}")

    return {"found": found, "recovered": recovered, "failed": failed, "skipped": 0}


def run_pbp_healer(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for PBP-specific auto-healing."""
    parser = argparse.ArgumentParser(description="KBO PBP 자동 치유 도구")
    parser.add_argument("--dry-run", action="store_true", help="발견만 하고 재크롤 생략")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=3,
        help="스캔할 과거 일수 (기본값: 3)",
    )
    parser.add_argument(
        "--game-id",
        nargs="+",
        metavar="GAME_ID",
        help="특정 game_id 강제 치유 (여러 개 가능)",
    )
    args = parser.parse_args(argv)
    result = asyncio.run(
        run_pbp_healer_async(
            dry_run=args.dry_run,
            lookback_days=args.lookback_days,
            target_game_ids=args.game_id,
        )
    )
    # Exit code: 0 if all healed, 1 if some failed
    return 1 if result.get("failed", 0) > 0 else 0


# ---------------------------------------------------------------------------
# Default Healer (Stuck / Inconsistent Games) CLI
# ---------------------------------------------------------------------------


def run_healer(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KBO Data Auto-Healer daemon")
    parser.add_argument(
        "--pbp",
        action="store_true",
        help="PBP 검증 실패 게임 재크롤 모드 실행",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report anomalies without fixing",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clear existing checkpoint and start fresh (default mode only)",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=3,
        help="PBP 모드: 스캔할 과거 일수 (기본값: 3)",
    )
    parser.add_argument(
        "--game-id",
        nargs="+",
        metavar="GAME_ID",
        help="PBP 모드: 특정 game_id 강제 치유",
    )
    args = parser.parse_args(argv)

    if args.pbp:
        return run_pbp_healer(
            [
                *(["--dry-run"] if args.dry_run else []),
                "--lookback-days",
                str(args.lookback_days),
                *(["--game-id"] + args.game_id if args.game_id else []),
            ]
        )

    return asyncio.run(run_healer_async(dry_run=args.dry_run, reset_checkpoint=args.reset))


if __name__ == "__main__":
    import sys

    sys.exit(run_healer())
