"""KBO Pipeline Auto-Healer.

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
import json
import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.db.engine import SessionLocal
from src.models.game import Game
from src.repositories.game_repository import update_game_status
from src.services.game_collection_service import (
    GameCollectionConfig,
    GameCollectionItemResult,
    crawl_and_save_game_details,
)
from src.services.game_write_contract import GameWriteContract
from src.services.player_id_resolver import PlayerIdResolver
from src.services.recovery_manager import RecoveryManager
from src.utils.alerting import SlackWebhookClient, TelegramBotClient
from src.utils.game_status import GAME_STATUS_CANCELLED, GAME_STATUS_SCHEDULED, GAME_STATUS_UNRESOLVED

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def _find_stuck_games() -> list[Game]:
    """Return all past games whose status is still SCHEDULED (no scores yet)."""
    yesterday = datetime.now(KST).date() - timedelta(days=1)
    with SessionLocal() as session:
        stmt = select(Game).where(
            Game.game_status.in_([GAME_STATUS_SCHEDULED, GAME_STATUS_UNRESOLVED]),
            Game.game_date <= yesterday,
        )
        return list(session.execute(stmt).scalars().all())


def _find_inconsistent_games() -> list[Game]:
    """Return games where total score does not match sum of inning scores."""
    # We check all games that are in terminal COMPLETED/DRAW state
    # but have score mismatches. This usually happens due to crawler bugs.
    query = text(
        """
        SELECT g.game_id FROM game g

        JOIN (
            SELECT g.game_id, g.away_score, g.home_score,
                   COALESCE((SELECT SUM(runs) FROM game_inning_scores i
                             WHERE i.game_id = g.game_id AND i.team_side = 'away'), 0) as away_sum,
                   COALESCE((SELECT SUM(runs) FROM game_inning_scores i
                             WHERE i.game_id = g.game_id AND i.team_side = 'home'), 0) as home_sum
            FROM game g
            WHERE g.game_status IN ('COMPLETED', 'DRAW')
        ) sub ON g.game_id = sub.game_id
        WHERE (sub.away_score != sub.away_sum OR sub.home_score != sub.home_sum)
    """,
    )
    with SessionLocal() as session:
        game_ids = session.execute(query).scalars().all()
        if not game_ids:
            return []
        stmt = select(Game).where(Game.game_id.in_(list(game_ids)))
        return list(session.execute(stmt).scalars().all())


def _find_pa_formula_inconsistent_games() -> list[Game]:
    """Return completed/draw games where PA != AB + BB + HBP + SH + SF in game_batting_stats."""
    query = text(
        """
        SELECT DISTINCT g.game_id
        FROM game g
        JOIN game_batting_stats b ON b.game_id = g.game_id
        WHERE g.game_status IN ('COMPLETED', 'DRAW')
          AND b.plate_appearances IS NOT NULL
          AND b.plate_appearances != (
              COALESCE(b.at_bats, 0) + COALESCE(b.walks, 0) + COALESCE(b.hbp, 0)
              + COALESCE(b.sacrifice_hits, 0) + COALESCE(b.sacrifice_flies, 0)
          )
    """,
    )
    with SessionLocal() as session:
        game_ids = session.execute(query).scalars().all()
        if not game_ids:
            return []
        stmt = select(Game).where(Game.game_id.in_(list(game_ids)))
        return list(session.execute(stmt).scalars().all())


def _apply_heal_outcome(game_id: str, item: GameCollectionItemResult | None) -> str:
    """Apply status repair based on one shared collection result item.

    Return one of: 'completed', 'cancelled', 'unresolved'

    Args:
        game_id: Game ID.
        item: Item.

    """
    if item and item.detail_saved:
        logger.info("  ✅ %s → COMPLETED (score saved)", game_id)
        return "completed"

    failure_reason = item.failure_reason if item else None
    if failure_reason == "cancelled":
        update_game_status(game_id, GAME_STATUS_CANCELLED)
        logger.info("  🚫 %s → CANCELLED", game_id)
        return "cancelled"

    update_game_status(game_id, GAME_STATUS_UNRESOLVED)
    logger.info("  ❓ %s → UNRESOLVED_MISSING (reason=%s)", game_id, failure_reason)
    return "unresolved"


def _find_recovery_targets(
    target_game_ids: list[str] | None,
) -> tuple[list[Game], list[Game], list[Game], list[Game]]:
    if target_game_ids:
        with SessionLocal() as session:
            stmt = select(Game).where(Game.game_id.in_(target_game_ids))
            all_found = list(session.execute(stmt).scalars().all())
            logger.info("🎯 Target recovery requested for %s specific game(s).", len(all_found))
            return all_found, [], [], []

    stuck_games = _find_stuck_games()
    inconsistent_games = _find_inconsistent_games()
    pa_formula_games = _find_pa_formula_inconsistent_games()
    all_found = sorted(
        {game.game_id: game for game in (stuck_games + inconsistent_games + pa_formula_games)}.values(),
        key=lambda game: game.game_id,
    )
    return all_found, stuck_games, inconsistent_games, pa_formula_games


def _pending_recovery_candidates(recovery_mgr: RecoveryManager, all_found: list[Game]) -> tuple[set[str], list[Game]]:
    recovery_mgr.initialize_run("default_healer_run", [game.game_id for game in all_found])  # type: ignore[misc]
    pending_ids = set(recovery_mgr.get_pending_targets())
    return pending_ids, [game for game in all_found if game.game_id in pending_ids]


def _log_anomaly_summary(
    all_found: list[Game],
    inconsistent_games: list[Game],
    pending_ids: set[str],
    anomaly_dates: list[Any],
    pa_formula_games: list[Game] | None = None,
) -> None:
    stuck_games_filtered = [game for game in all_found if game.game_status == GAME_STATUS_SCHEDULED]
    if stuck_games_filtered:
        stuck_count = len([game for game in stuck_games_filtered if game.game_id in pending_ids])
        if stuck_count:
            logger.warning("⚠️  Anomaly Detected: %s past game(s) stuck in SCHEDULED state!", stuck_count)
    if inconsistent_games:
        incon_count = len([game for game in inconsistent_games if game.game_id in pending_ids])
        if incon_count:
            logger.warning("⚠️  Anomaly Detected: %s game(s) with score inconsistencies!", incon_count)
    if pa_formula_games:
        pa_count = len([game for game in pa_formula_games if game.game_id in pending_ids])
        if pa_count:
            logger.warning("⚠️  Anomaly Detected: %s game(s) with PA formula violations!", pa_count)
    for anomaly_date in anomaly_dates:
        logger.info("  - %s", anomaly_date)


def _send_healer_start_alert(
    total: int,
    stuck_games: list[Game],
    inconsistent_games: list[Game],
    anomaly_dates: list[Any],
    pa_formula_games: list[Game] | None = None,
) -> None:
    summary_parts = []
    if stuck_games:
        summary_parts.append(f"*{len(stuck_games)}* stuck games")
    if inconsistent_games:
        summary_parts.append(f"*{len(inconsistent_games)}* inconsistent games")
    if pa_formula_games:
        summary_parts.append(f"*{len(pa_formula_games)}* PA formula violation games")

    date_range = f"`{anomaly_dates[0]}`" if len(anomaly_dates) == 1 else f"`{anomaly_dates[0]}` ~ `{anomaly_dates[-1]}`"
    SlackWebhookClient.send_alert(
        f"Pipeline Anomaly: {total} games detected for auto-healing.",
        blocks=[
            {"type": "header", "text": {"type": "plain_text", "text": "⚠️ KBO Pipeline Anomaly"}},
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


AUTO_HEALER_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)


def _apply_pa_formula_backfill(recovery_candidates: list[Game]) -> int:
    from src.cli.recalc_player_game_stats import run_recalc as recalc_game_stats
    from src.services.pbp_sh_sf_derivation import apply_sh_sf_to_batting_stats

    pa_fixed_count = 0
    with SessionLocal() as session:
        for game in recovery_candidates:
            try:
                updated_rows = apply_sh_sf_to_batting_stats(session, game.game_id)
                if updated_rows > 0:
                    session.commit()
                    pa_fixed_count += 1
                    recalc_game_stats(game_id=game.game_id, dry_run=False)
                    logger.info("  ✅ PA formula backfilled & game stats recalculated for %s", game.game_id)
            except (SQLAlchemyError, ValueError, RuntimeError) as e:
                session.rollback()
                logger.warning("  ⚠️ Error during PA formula backfill for %s: %s", game.game_id, e)
    return pa_fixed_count


async def _run_recovery(
    recovery_candidates: list[Game],
    anomaly_dates: list[Any],
    recovery_mgr: RecoveryManager,
    *,
    dry_run: bool,
) -> dict[str, int]:
    with SessionLocal() as db_session:
        resolver = PlayerIdResolver(db_session, strict_game_resolution=True, allow_auto_register=False)
        for year in {anomaly_date.year for anomaly_date in anomaly_dates}:
            resolver.preload_season_index(year)

        crawler = GameDetailCrawler(request_delay=1.0, resolver=resolver)
        write_contract = GameWriteContract(run_label=f"auto_healer:{datetime.now(KST):%Y%m%dT%H%M%S}", log=logger.info)
        results = {"completed": 0, "cancelled": 0, "unresolved": 0, "dry_run": 0}
        if dry_run:
            for game in recovery_candidates:
                logger.info("  [DRY-RUN] Would re-crawl %s", game.game_id)
                results["dry_run"] += 1
            return results

        collection_result = await crawl_and_save_game_details(
            [{"game_id": game.game_id, "game_date": game.game_date.strftime("%Y%m%d")} for game in recovery_candidates],
            detail_crawler=crawler,
            config=GameCollectionConfig(
                force=True,
                concurrency=1,
                log=logger.info,
                write_contract=write_contract,
                source_reason="auto_healing_recovery",
            ),
        )
        for game in recovery_candidates:
            item = collection_result.items.get(game.game_id)  # type: ignore[call-overload]
            outcome = _apply_heal_outcome(game.game_id, item)  # type: ignore[arg-type]
            results[outcome] = results.get(outcome, 0) + 1
            if outcome == "completed":
                recovery_mgr.mark_completed(game.game_id)  # type: ignore[arg-type]
            elif outcome == "unresolved":
                recovery_mgr.mark_failed(game.game_id, item.failure_reason if item else "unknown")  # type: ignore[arg-type]

        pa_fixed_count = _apply_pa_formula_backfill(recovery_candidates)
        if pa_fixed_count > 0:
            results["pa_formula_fixed"] = pa_fixed_count

        logger.info(write_contract.summary())
        return results


def _log_healer_summary(results: dict[str, int], *, dry_run: bool) -> None:
    logger.info("\n📊 Auto-Healer Summary:")
    for outcome, count in results.items():
        if count > 0:
            logger.info("  %s: %s", outcome, count)

    if dry_run:
        return

    from src.utils.metrics import KBO_AUTO_HEALER_RECOVERED_TOTAL, KBO_AUTO_HEALER_UNRESOLVED_TOTAL

    completed = results.get("completed", 0)
    unresolved = results.get("unresolved", 0)
    if completed > 0:
        KBO_AUTO_HEALER_RECOVERED_TOTAL.labels(type="stuck").inc(completed)
    if unresolved > 0:
        KBO_AUTO_HEALER_UNRESOLVED_TOTAL.labels(type="stuck").inc(unresolved)

    unresolved_count = results.get("unresolved", 0)
    if unresolved_count == 0:
        SlackWebhookClient.send_alert(f"✅ Auto-healing complete. {results['completed']} games recovered.")
    else:
        SlackWebhookClient.send_alert(
            f"⚠️ Auto-healing complete. {results['completed']} recovered, {unresolved_count} failed.",
        )


async def run_healer_async(
    *,
    dry_run: bool = False,
    reset_checkpoint: bool = False,
    target_game_ids: list[str] | None = None,
) -> int:
    """Run healer async.

    Args:
        dry_run: If True, performs a dry run without persisting changes.
        reset_checkpoint: Reset Checkpoint.
        target_game_ids: Target Game Ids.

    Returns:
        Integer result.

    """
    logger.info("\n🩺 Running KBO Pipeline Auto-Healer...")

    recovery_mgr = RecoveryManager()
    if reset_checkpoint:
        recovery_mgr.clear()

    all_found, stuck_games, inconsistent_games, pa_formula_games = _find_recovery_targets(target_game_ids)
    if not target_game_ids and not stuck_games and not inconsistent_games and not pa_formula_games:
        logger.info("✅ No anomalies detected. Pipeline is healthy.")
        recovery_mgr.clear()
        return 0

    if not all_found:
        logger.info("✅ No games found for recovery.")
        return 0

    pending_ids, recovery_candidates = _pending_recovery_candidates(recovery_mgr, all_found)
    if not recovery_candidates:
        logger.info("✅ All detected anomalies were already processed in current checkpoint.")
        return 0

    total = len(recovery_candidates)
    anomaly_dates = sorted({g.game_date for g in recovery_candidates})
    _log_anomaly_summary(all_found, inconsistent_games, pending_ids, anomaly_dates, pa_formula_games=pa_formula_games)
    if not dry_run:
        _send_healer_start_alert(
            total, stuck_games, inconsistent_games, anomaly_dates, pa_formula_games=pa_formula_games
        )

    logger.info("\n🚀 Initiating self-recovery for %s game(s)...", total)
    results = await _run_recovery(recovery_candidates, anomaly_dates, recovery_mgr, dry_run=dry_run)
    _log_healer_summary(results, dry_run=dry_run)
    return results["unresolved"]


# ---------------------------------------------------------------------------
# PBP Healing Mode
# ---------------------------------------------------------------------------


def _find_unverified_pbp_games(lookback_days: int = 3) -> list[dict]:
    """Scan game_metadata for finished games whose PBP is still 'unverified'.

    Return a list of dicts: {game_id, game_date, away_team, home_team, error_reason}

    Args:
        lookback_days: Lookback Days.

    """
    cutoff = (datetime.now(KST).date() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

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
        """,
    )

    results = []
    with SessionLocal() as session:
        rows = session.execute(query, {"cutoff": cutoff}).fetchall()
        for row in rows:
            payload = row.source_payload
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except (json.JSONDecodeError, TypeError):
                    payload = {}
            error_reason = payload.get("pbp_validation_error", "unknown") if isinstance(payload, dict) else "unknown"
            results.append(
                {
                    "game_id": row.game_id,
                    "game_date": str(row.game_date),
                    "away_team": row.away_team or "?",
                    "home_team": row.home_team or "?",
                    "error_reason": error_reason,
                },
            )
    return results


def _record_pbp_healer_metrics(recovered: int, failed: int) -> None:
    from src.utils.metrics import KBO_AUTO_HEALER_RECOVERED_TOTAL, KBO_AUTO_HEALER_UNRESOLVED_TOTAL

    if recovered > 0:
        KBO_AUTO_HEALER_RECOVERED_TOTAL.labels(type="pbp").inc(recovered)
    if failed > 0:
        KBO_AUTO_HEALER_UNRESOLVED_TOTAL.labels(type="pbp").inc(failed)


async def run_pbp_healer_async(
    *,
    dry_run: bool = False,
    lookback_days: int = 3,
    target_game_ids: list[str] | None = None,
) -> dict[str, Any]:
    """PBP Auto-Healer.

      1. Scan DB for unverified PBP games.

      2. Send Telegram notification with the list.
      3. Re-crawl from KBO official website (PBPCrawler).
      4. Re-validate and re-save.
      5. Send Telegram notification with the final result.

    Returns a summary dict: {found, recovered, failed, skipped}

    Args:
        dry_run: If True, performs a dry run without persisting changes.
        lookback_days: Lookback Days.
        target_game_ids: Target Game Ids.

    """
    logger.info("\n🩺 [PBP Healer] 검증 실패 PBP 게임 스캔 중...")

    if target_game_ids:
        # Targeted mode: load metadata for specific games
        query = text(
            """
            SELECT g.game_id, g.game_date, g.away_team, g.home_team, m.source_payload

            FROM game g
            LEFT JOIN game_metadata m ON g.game_id = m.game_id
            WHERE g.game_id IN :ids
            """,
        )
        results = []

        with SessionLocal() as session:
            rows = session.execute(query, {"ids": tuple(target_game_ids)}).fetchall()
            for row in rows:
                payload = row.source_payload
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except (json.JSONDecodeError, TypeError):
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
                    },
                )
    else:
        results = _find_unverified_pbp_games(lookback_days=lookback_days)

    if not results:
        logger.info("✅ [PBP Healer] 검증 실패 PBP 게임 없음. 파이프라인 정상.")
        return {"found": 0, "recovered": 0, "failed": 0, "skipped": 0}

    found = len(results)
    logger.warning("⚠️  [PBP Healer] 검증 실패 게임 %s건 발견", found)
    for item in results:
        logger.info(
            "   • %s (%s vs %s) - %s",
            item["game_id"],
            item["away_team"],
            item["home_team"],
            item["error_reason"],
        )

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
        logger.info("[DRY-RUN] 재크롤 생략. 실제 복구는 --pbp 없이 실행하거나 dry-run 플래그 제거.")
        return {"found": found, "recovered": 0, "failed": 0, "skipped": found}

    # --- Re-crawl through the relay source orchestrator ---
    from src.services.relay_recovery_service import (
        RelayRecoveryConfig,
        RelayRecoveryTarget,
        recover_relay_data,
    )
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
        RelayRecoveryConfig(
            source_order_override=["kbo", "naver", "import", "manual"],
            allow_derived_pbp=False,
            sleep_seconds=0,
            log=logger.info,
        ),
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
    logger.info("\n📊 [PBP Healer] 완료 — 발견 %s, 복구 %s, 실패 %s", found, recovered, failed)

    _record_pbp_healer_metrics(recovered, failed)
    return {"found": found, "recovered": recovered, "failed": failed, "skipped": 0}


def run_pbp_healer(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for PBP-specific auto-healing.

    Args:
        argv: Argv.

    """
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
        ),
    )
    # Exit code: 0 if all healed, 1 if some failed
    return 1 if result.get("failed", 0) > 0 else 0


# ---------------------------------------------------------------------------
# Default Healer (Stuck / Inconsistent Games) CLI
# ---------------------------------------------------------------------------


def run_healer(argv: Sequence[str] | None = None) -> int:
    """Run healer.

    Args:
        argv: Argv.
        argv: Argv.

    Returns:
        Integer result.

    """
    parser = argparse.ArgumentParser(description="KBO Data Auto-Healer daemon")

    parser.add_argument(
        "--pbp",
        action="store_true",
        help="PBP 검증 실패 게임 재크롤 모드 실행",
    )
    parser.add_argument(
        "--pa-formula",
        action="store_true",
        help="PA 공식(PA = AB+BB+HBP+SH+SF) 검증 실패 게임 복구 모드 실행",
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
        help="PBP/PA-Formula 모드: 특정 game_id 강제 치유",
    )
    args = parser.parse_args(argv)

    if args.pbp:
        return run_pbp_healer(
            [
                *(["--dry-run"] if args.dry_run else []),
                "--lookback-days",
                str(args.lookback_days),
                *(["--game-id", *args.game_id] if args.game_id else []),
            ],
        )

    kwargs: dict[str, Any] = {"dry_run": args.dry_run, "reset_checkpoint": args.reset}
    if args.pa_formula and args.game_id:
        kwargs["target_game_ids"] = args.game_id
    return asyncio.run(run_healer_async(**kwargs))


def main(argv: Sequence[str] | None = None) -> int:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    return run_healer(argv)


if __name__ == "__main__":
    import sys

    sys.exit(main())
