"""
KBO Daily Data Update Orchestrator.

This entrypoint is the postgame finalize + daily reconciliation job.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import subprocess
import sys
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from playwright.async_api import Error as PlaywrightError
from sqlalchemy import or_, select
from sqlalchemy.exc import SQLAlchemyError

from scripts.maintenance.audit_game_status_integrity import audit_game_status
from scripts.maintenance.quality_gate import run_quality_gate as run_legacy_quality_gate
from src.cli.auto_healer import run_healer_async
from src.crawlers.daily_roster_crawler import DailyRosterCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.player_batting_all_series_crawler import crawl_series_batting_stats
from src.crawlers.player_movement_crawler import PlayerMovementCrawler
from src.crawlers.player_pitching_all_series_crawler import crawl_pitcher_series
from src.crawlers.roster_transaction_crawler import RosterTransactionCrawler
from src.crawlers.schedule_crawler import ScheduleCrawler
from src.crawlers.team_event_crawler import TeamEventCrawler
from src.crawlers.ticket_crawler import TicketCrawler
from src.db.engine import SessionLocal
from src.models.game import Game, GameEvent, GamePlayByPlay
from src.repositories.game_repository import (
    refresh_game_status_for_date,
    update_game_status,
)
from src.repositories.player_repository import PlayerRepository
from src.repositories.team_repository import TeamRepository
from src.services.game_collection_service import GameCollectionItemResult, crawl_and_save_game_details
from src.services.game_write_contract import GameWriteContract
from src.services.p0_readiness import build_p0_readiness, format_p0_readiness_summary
from src.services.player_id_resolver import PlayerIdResolver
from src.services.postgame_reconciliation_service import (
    format_reconciliation_report,
    reconcile_postgame_range,
)
from src.services.recovery_manager import RecoveryManager
from src.services.schedule_collection_service import save_schedule_games
from src.sync.oci_sync import OCISync
from src.utils.alerting import SlackWebhookClient
from src.utils.game_status import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_POSTPONED,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_UNRESOLVED,
)
from src.utils.refresh_manifest import write_refresh_manifest
from src.utils.schedule_validation import is_detail_candidate_game
from src.utils.team_codes import normalize_kbo_game_id


@dataclass
class _RunContext:
    target_date: str
    year: int
    month: int
    today_kst: date
    runner: Callable
    write_contract: GameWriteContract
    step_runner: Callable | None = None
    summary_dir: str | Path | None = None
    seed_tomorrow_preview: bool = False
    run_auto_healer: bool = True
    run_postgame_reconciliation: bool = True
    postgame_reconcile_lookback_days: int = 3
    fix: bool = False
    skip_season_stats: bool = False
    skip_oci_supporting_sync: bool = False
    run_p0_non_game: bool = True
    headless: bool = True
    limit: int | None = None
    sync: bool = False
    daily_games: list[dict] = field(default_factory=list)
    detail_games: list[dict] = field(default_factory=list)
    freshness_dates: list[str] = field(default_factory=list)
    r_target_date: str = ""
    candidate_sync_game_ids: list[str] = field(default_factory=list)
    derived_refresh: list[str] = field(default_factory=list)
    healer_recovery_targets: list[dict[str, str]] = field(default_factory=list)
    reconciliation_changed_ids: list[str] = field(default_factory=list)
    reconciliation_dates: list[str] = field(default_factory=list)
    detail_failure_counts: dict[str, int] = field(default_factory=dict)
    detail_failure_game_ids: dict[str, list[str]] = field(default_factory=dict)
    relay_recovery_target_ids: set[str] = field(default_factory=set)
    oci_skip_counts: dict[str, int] = field(default_factory=dict)
    oci_skip_game_ids: dict[str, list[str]] = field(default_factory=dict)
    non_p0_quality_gate_counts: dict[str, int] = field(default_factory=dict)
    non_p0_quality_gate_ids: dict[str, list[str]] = field(default_factory=dict)
    p0_non_game_counts: dict[str, int] = field(default_factory=dict)
    p0_non_game_errors: dict[str, str] = field(default_factory=dict)
    status_refresh_game_ids: list[str] = field(default_factory=list)
    detail_recovery_passes: int = 0
    detail_recovered_after_retry: int = 0
    detail_retry_escalation_game_ids: list[str] = field(default_factory=list)
    detail_recovery_attempts: dict[str, int] = field(default_factory=dict)
    detail_still_missing: set[str] = field(default_factory=set)
    detail_recovery_queue: RecoveryManager | None = None
    queued_recovery_game_ids: set[str] = field(default_factory=set)
    processed_game_ids: list[str] = field(default_factory=list)
    detail_games_by_id: dict[str, dict[str, str]] = field(default_factory=dict)


logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DAILY_SUMMARY_DIR = PROJECT_ROOT / "logs" / "daily_update_summary"
OCI_SKIP_KEYS = (
    "skipped_schedule_only",
    "skipped_incomplete_detail",
    "skipped_empty_relay",
    "skipped_cancelled",
)
DETAIL_RECOVERY_MAX_ROUNDS = int(os.getenv("DETAIL_RECOVERY_MAX_ROUNDS", "3"))
DETAIL_RECOVERY_RETRY_ALERT_THRESHOLD = int(os.getenv("DETAIL_RECOVERY_RETRY_ALERT_THRESHOLD", "2"))
DETAIL_RECOVERY_COOLDOWN_MINUTES = int(os.getenv("DETAIL_RECOVERY_COOLDOWN_MINUTES", "360"))
DETAIL_RECOVERY_QUEUE_PATH = os.getenv("DETAIL_RECOVERY_QUEUE_PATH", "data/recovery/detail_recovery_queue.json")
DETAIL_RECOVERY_ALLOWED_REASONS = {
    "no_detail_payload",
    "incomplete_detail",
    "timeout",
    "navigation_error",
    "exception",
    "missing",
}
RUNNER_EXCEPTIONS = (subprocess.CalledProcessError, OSError, RuntimeError)
DB_STEP_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError)
CRAWLER_STEP_EXCEPTIONS = (
    PlaywrightError,
    TimeoutError,
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
    SQLAlchemyError,
)
DAILY_STEP_EXCEPTIONS = (*CRAWLER_STEP_EXCEPTIONS, subprocess.CalledProcessError)
ALERT_EXCEPTIONS = (RuntimeError, ValueError, TypeError, OSError)
FILE_READ_EXCEPTIONS = (OSError, UnicodeError, csv.Error)


def _is_recoverable_detail_reason(reason: str | None) -> bool:
    normalized = (reason or "").strip().lower()
    return normalized in DETAIL_RECOVERY_ALLOWED_REASONS


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
    non_p0_quality_gate_counts: Mapping[str, int],
    non_p0_quality_gate_ids: Mapping[str, list[str]],
    p0_non_game_counts: Mapping[str, int],
    p0_non_game_errors: Mapping[str, str],
    detail_recovery_passes: int,
    detail_recovered_after_retry: int,
    detail_still_missing: Sequence[str],
    detail_recovery_attempts: Mapping[str, int],
    detail_recovery_escalation_game_ids: Sequence[str],
    summary_path: Path,
) -> dict[str, Any]:
    recoverable_reasons = {
        "no_detail_payload",
        "incomplete_detail",
        "timeout",
        "navigation_error",
        "exception",
        "missing",
    }
    detail_retry_candidates = sorted(
        {
            game_id
            for reason, game_ids in detail_failure_game_ids.items()
            if reason in recoverable_reasons
            for game_id in game_ids
        },
    )
    relay_retry_candidates = sorted(set(oci_skip_game_ids.get("skipped_empty_relay", [])))
    affected_game_ids = sorted(
        {
            game_id
            for ids in [*detail_failure_game_ids.values(), *oci_skip_game_ids.values()]
            for game_id in ids
            if game_id
        },
    )
    return {
        "summary_path": str(summary_path),
        "detail": {
            "failure_counts": dict(sorted(detail_failure_counts.items())),
            "failure_game_ids": {
                reason: sorted(set(game_ids)) for reason, game_ids in sorted(detail_failure_game_ids.items())
            },
        },
        "relay": {
            "target_count": len(set(relay_recovery_target_ids)),
            "target_game_ids": sorted(set(relay_recovery_target_ids)),
        },
        "oci": {
            "skip_counts": dict(sorted(oci_skip_counts.items())),
            "skip_game_ids": {reason: sorted(set(game_ids)) for reason, game_ids in sorted(oci_skip_game_ids.items())},
        },
        "quality_gates": {
            "non_p0_failure_counts": dict(sorted(non_p0_quality_gate_counts.items())),
            "non_p0_failure_ids": {reason: sorted(set(ids)) for reason, ids in sorted(non_p0_quality_gate_ids.items())},
        },
        "p0_non_game": {
            "counts": dict(sorted(p0_non_game_counts.items())),
            "errors": dict(sorted(p0_non_game_errors.items())),
        },
        "retry_candidates": {
            "detail": detail_retry_candidates,
            "relay": relay_retry_candidates,
        },
        "detail_recovery": {
            "passes": int(detail_recovery_passes),
            "recovered_after_retry": int(detail_recovered_after_retry),
            "still_missing_count": len({str(game_id) for game_id in detail_still_missing}),
            "still_missing": sorted({str(game_id) for game_id in detail_still_missing}),
            "attempts_by_game": {str(game_id): int(attempts) for game_id, attempts in detail_recovery_attempts.items()},
            "escalation_threshold": DETAIL_RECOVERY_RETRY_ALERT_THRESHOLD,
            "escalation_game_ids": sorted({str(game_id) for game_id in detail_recovery_escalation_game_ids}),
        },
        "affected_game_ids": affected_game_ids,
    }


def _write_daily_update_summary(
    *,
    target_date: str,
    stability: Mapping[str, Any],
    p0_readiness: Mapping[str, Any],
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
        "p0_readiness": dict(p0_readiness),
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
    quality_gates = stability.get("quality_gates") if isinstance(stability.get("quality_gates"), dict) else {}
    detail_recovery = stability.get("detail_recovery") if isinstance(stability.get("detail_recovery"), dict) else {}
    detail_counts = detail.get("failure_counts") if isinstance(detail, dict) else {}
    oci_counts = oci.get("skip_counts") if isinstance(oci, dict) else {}
    non_p0_counts = quality_gates.get("non_p0_failure_counts") if isinstance(quality_gates, dict) else {}
    relay_targets = relay.get("target_count", 0) if isinstance(relay, dict) else 0
    recovery_passes = detail_recovery.get("passes", 0) if isinstance(detail_recovery, dict) else 0
    recovered_after_retry = detail_recovery.get("recovered_after_retry", 0) if isinstance(detail_recovery, dict) else 0
    still_missing_count = detail_recovery.get("still_missing_count", 0) if isinstance(detail_recovery, dict) else 0

    return (
        f"target_date={result.get('target_date', 'unknown')} "
        f"detail_failures={_format_counts(detail_counts if isinstance(detail_counts, dict) else {})} "
        f"detail_recovery_passes={recovery_passes} "
        f"detail_recovered_after_retry={recovered_after_retry} "
        f"detail_still_missing={still_missing_count} "
        f"relay_targets={relay_targets} "
        f"oci_skips={_format_counts(oci_counts if isinstance(oci_counts, dict) else {})} "
        f"non_p0_quality_gates={_format_counts(non_p0_counts if isinstance(non_p0_counts, dict) else {})} "
        f"{format_p0_readiness_summary(result.get('p0_readiness'))}"
    )


def _failure_status(target_date: str, failure_reason: str | None, today: date) -> str | None:
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


def _run_game_status_integrity_audit() -> None:
    violations = audit_game_status()
    if not violations:
        return

    sample = "; ".join(
        f"{item.get('game_id')} {item.get('game_date')} {item.get('status')}: {item.get('reason')}"
        for item in violations[:5]
    )
    if len(violations) > 5:
        sample = f"{sample}; ... and {len(violations) - 5} more"
    msg = f"{len(violations)} game status integrity violations found: {sample}"
    raise RuntimeError(msg)


def _run_oci_parity_quality_gate() -> dict[str, Any]:
    result = run_legacy_quality_gate(
        baseline_path=PROJECT_ROOT / "Docs" / "quality_gate_baseline.json",
        output_dir=PROJECT_ROOT / "data",
        oci_url=os.getenv("OCI_DB_URL"),
        skip_oci=False,
        oci_only=False,
        write_artifacts=True,
        strict_zero=False,
    )
    if not result.get("ok"):
        failures = result.get("failures") or []
        detail = "; ".join(str(item) for item in failures[:5]) if failures else "unknown failure"
        if len(failures) > 5:
            detail = f"{detail}; ... and {len(failures) - 5} more"
        raise RuntimeError(detail)
    return result


def _collect_past_scheduled_recovery_targets(today: date) -> list[dict[str, str]]:
    """Capture auto-healer candidates so repaired past games can be finalized and synced."""
    yesterday = today - timedelta(days=1)
    try:
        with SessionLocal() as session:
            rows = (
                session.query(Game.game_id, Game.game_date)
                .filter(
                    Game.game_status.in_([GAME_STATUS_SCHEDULED, GAME_STATUS_UNRESOLVED]),
                    Game.game_date <= yesterday,
                )
                .order_by(Game.game_date.asc(), Game.game_id.asc())
                .all()
            )
    except SQLAlchemyError:
        logger.exception("   ⚠️ Could not inspect auto-healer recovery candidates")
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


async def _step_0_auto_healer(ctx: _RunContext) -> None:
    if ctx.run_auto_healer:
        logger.info("\n\U0001fa7a Step 0: Running Auto-Healer...")
        ctx.healer_recovery_targets = _collect_past_scheduled_recovery_targets(ctx.today_kst)
        try:
            await run_healer_async(dry_run=False)
        except DAILY_STEP_EXCEPTIONS:
            logger.exception("   \u26a0\ufe0f Auto-Healer encountered an error (continuing anyway)")
            ctx.healer_recovery_targets = []
        if ctx.healer_recovery_targets:
            logger.info("   \u2705 Auto-Healer recovery candidates tracked: %s", len(ctx.healer_recovery_targets))
    else:
        logger.info("\n\U0001fa7a Step 0: Auto-Healer skipped for scoped backfill run.")


async def _step_1_schedule(ctx: _RunContext) -> None:
    logger.info("\n\U0001f4c5 Step 1: Crawling + saving monthly schedule...")
    s_crawler = ScheduleCrawler()
    schedule_games = await s_crawler.crawl_schedule(ctx.year, ctx.month)
    schedule_result = save_schedule_games(
        schedule_games,
        log=logger.info,
        write_contract=ctx.write_contract,
        source_reason=f"monthly_schedule_refresh:{ctx.year}-{ctx.month:02d}",
    )
    logger.info(
        "   ✅ Schedule discovered=%s saved=%s failed=%s",
        schedule_result.discovered,
        schedule_result.saved,
        schedule_result.failed,
    )

    daily_games = [g for g in schedule_games if str(g.get("game_date", "")).replace("-", "") == ctx.target_date]
    detail_games = [g for g in daily_games if is_detail_candidate_game(g, today=ctx.today_kst)]
    skipped_detail_games = len(daily_games) - len(detail_games)
    if skipped_detail_games:
        logger.info("   \u2139\ufe0f Skipping %s non-detail schedule games", skipped_detail_games)
    if ctx.limit and len(detail_games) > ctx.limit:
        detail_games = detail_games[: ctx.limit]
        logger.info("   [LIMIT] Restricted to first %s games", ctx.limit)
    logger.info("   \u2705 Found %s games for %s", len(daily_games), ctx.target_date)
    ctx.daily_games = daily_games
    ctx.detail_games = detail_games


async def _run_detail_recovery_passes(
    ctx: _RunContext,
    g_crawler: GameDetailCrawler,
    detail_results_by_game: dict,
    unrecovered_game_ids: set[str],
    recoverable_failure_ids: set[str],
    max_recovery_rounds: int,
) -> None:
    for _ in range(max_recovery_rounds - 1):
        retry_game_ids = sorted(
            game_id
            for game_id in recoverable_failure_ids
            if ctx.detail_recovery_attempts.get(game_id, 0) < max_recovery_rounds
        )
        if not retry_game_ids:
            break

        ctx.detail_recovery_passes += 1
        retry_targets = [
            ctx.detail_games_by_id[game_id] for game_id in retry_game_ids if game_id in ctx.detail_games_by_id
        ]
        for game_id in retry_game_ids:
            ctx.detail_recovery_attempts[game_id] = ctx.detail_recovery_attempts.get(game_id, 0) + 1

        logger.info(
            "   \U0001f501 Detail recovery pass #%s (%s game(s))", ctx.detail_recovery_passes, len(retry_targets)
        )
        retry_result = await crawl_and_save_game_details(
            retry_targets,
            detail_crawler=g_crawler,
            force=True,
            concurrency=1,
            log=logger.info,
            write_contract=ctx.write_contract,
            source_reason=f"postgame_finalize:{ctx.target_date}:recovery",
        )

        for game_id, item in retry_result.items.items():
            normalized_game_id = normalize_kbo_game_id(game_id)
            detail_results_by_game[normalized_game_id] = item

            if item.detail_saved:
                if normalized_game_id in unrecovered_game_ids:
                    unrecovered_game_ids.remove(normalized_game_id)
                    ctx.detail_recovered_after_retry += 1
                recoverable_failure_ids.discard(normalized_game_id)
            elif not _is_recoverable_detail_reason(item.failure_reason):
                unrecovered_game_ids.discard(normalized_game_id)
                recoverable_failure_ids.discard(normalized_game_id)


def _process_detail_results(
    ctx: _RunContext, detail_results_by_game: dict[str, Any], processed_game_ids_set: set[str]
) -> None:
    for game_id, item in detail_results_by_game.items():
        reason = item.failure_reason if item else None
        if item.detail_saved:
            processed_game_ids_set.add(game_id)
            ctx.detail_recovery_queue.mark_detail_recovery_success(ctx.target_date, game_id)
        elif _is_recoverable_detail_reason(reason):
            ctx.detail_recovery_queue.mark_detail_recovery_failure(
                ctx.target_date,
                game_id,
                failure_reason=reason,
            )
        else:
            ctx.detail_recovery_queue.mark_detail_recovery_success(ctx.target_date, game_id)


def _prepare_detail_targets(ctx: _RunContext) -> None:
    for game in ctx.detail_games:
        game_id = normalize_kbo_game_id(game.get("game_id"))
        if not game_id:
            continue
        ctx.detail_games_by_id[game_id] = {
            "game_id": game_id,
            "game_date": str(game.get("game_date") or ctx.target_date),
        }
        ctx.detail_recovery_attempts[game_id] = 0

    queued_recovery_game_count = 0
    for queued_game_id in sorted(ctx.queued_recovery_game_ids):
        if queued_game_id in ctx.detail_games_by_id:
            continue
        if ctx.limit is not None and len(ctx.detail_games_by_id) >= ctx.limit:
            continue
        ctx.detail_games_by_id[queued_game_id] = {
            "game_id": queued_game_id,
            "game_date": ctx.target_date,
        }
        ctx.detail_recovery_attempts[queued_game_id] = 0
        queued_recovery_game_count += 1
    if queued_recovery_game_count > 0:
        logger.info(
            "   ♻️ Re-prioritizing %s queued detail-recovery game(s)",
            queued_recovery_game_count,
        )


async def _collect_detail_results(ctx: _RunContext, g_crawler: GameDetailCrawler) -> dict[str, Any]:
    collection_result = await crawl_and_save_game_details(
        list(ctx.detail_games_by_id.values()),
        detail_crawler=g_crawler,
        force=True,
        concurrency=1,
        log=logger.info,
        write_contract=ctx.write_contract,
        source_reason=f"postgame_finalize:{ctx.target_date}",
    )
    detail_results_by_game = dict(collection_result.items)
    for game_id in ctx.detail_games_by_id:
        ctx.detail_recovery_attempts[game_id] = ctx.detail_recovery_attempts.get(game_id, 0) + 1

    unrecovered_game_ids = {
        normalize_kbo_game_id(game_id)
        for game_id, item in detail_results_by_game.items()
        if item and not item.detail_saved
    }
    recoverable_failure_ids = {
        game_id
        for game_id in unrecovered_game_ids
        if _is_recoverable_detail_reason(detail_results_by_game[game_id].failure_reason)
    }
    await _run_detail_recovery_passes(
        ctx,
        g_crawler,
        detail_results_by_game,
        unrecovered_game_ids,
        recoverable_failure_ids,
        max(1, int(DETAIL_RECOVERY_MAX_ROUNDS)),
    )
    return detail_results_by_game


def _apply_detail_failure_fallback(ctx: _RunContext, game_id: str, reason: str | None) -> None:
    fallback = _failure_status(ctx.target_date, reason, ctx.today_kst)
    if not fallback:
        return
    with SessionLocal() as status_check_session:
        current_game = (
            status_check_session.query(Game).filter(Game.game_id == normalize_kbo_game_id(game_id)).one_or_none()
        )
        if (
            current_game
            and current_game.game_status in {GAME_STATUS_CANCELLED, GAME_STATUS_POSTPONED}
            and fallback != GAME_STATUS_CANCELLED
        ):
            logger.info(
                "   ℹ️ Preservation: Keeping terminal status '%s' for %s",
                current_game.game_status,
                game_id,
            )
        else:
            update_game_status(game_id, fallback)


def _record_detail_result_status(ctx: _RunContext, game_id: str, item: GameCollectionItemResult | None) -> None:
    if item and item.detail_saved:
        logger.info("   ✅ Successfully saved %s", game_id)
        return

    reason = item.failure_reason if item else "exception"
    if item and item.detail_status == "save_failed":
        logger.error("   ❌ Failed to save details for %s to local DB", game_id)
    else:
        logger.warning("   ⚠️ Could not fetch details for %s (reason=%s)", game_id, reason or "unknown")

    ctx.detail_still_missing.add(game_id)
    if ctx.detail_recovery_attempts.get(game_id, 0) >= DETAIL_RECOVERY_RETRY_ALERT_THRESHOLD + 1:
        ctx.detail_retry_escalation_game_ids.append(game_id)
    _apply_detail_failure_fallback(ctx, game_id, reason)


def _send_detail_recovery_escalation_alert(ctx: _RunContext) -> None:
    if not ctx.detail_retry_escalation_game_ids:
        return
    try:
        SlackWebhookClient.send_alert(
            "⚠️ Detail recovery repeated failures: "
            f"target_date={ctx.target_date} threshold={DETAIL_RECOVERY_RETRY_ALERT_THRESHOLD} "
            f"game_ids={','.join(sorted(set(ctx.detail_retry_escalation_game_ids)))}",
        )
    except ALERT_EXCEPTIONS:
        logger.exception("   ❌ Failed to send detail recovery escalation alert")


def _finalize_detail_results(
    ctx: _RunContext,
    detail_results_by_game: dict[str, Any],
    processed_game_ids_set: set[str],
) -> None:
    _process_detail_results(ctx, detail_results_by_game, processed_game_ids_set)
    ctx.processed_game_ids = sorted(processed_game_ids_set)
    ctx.detail_failure_counts, ctx.detail_failure_game_ids = _failure_reason_summary(detail_results_by_game)

    for game_id in sorted(ctx.detail_games_by_id):
        _record_detail_result_status(ctx, game_id, detail_results_by_game.get(game_id))

    logger.info(
        "   ✅ Detail result success=%s failed=%s recovery_passes=%s",
        len(ctx.processed_game_ids),
        len(ctx.detail_still_missing),
        ctx.detail_recovery_passes,
    )
    if ctx.detail_failure_counts:
        logger.info("   ℹ️ Detail failure reasons: %s", _format_counts(ctx.detail_failure_counts))
    if ctx.detail_recovery_passes:
        logger.info(
            "   ℹ️ Detail recovery recovered_after_retry=%s, still_missing=%s, escalated=%s",
            ctx.detail_recovered_after_retry,
            len(ctx.detail_still_missing),
            len(ctx.detail_retry_escalation_game_ids),
        )
    _send_detail_recovery_escalation_alert(ctx)


async def _run_postgame_reconciliation(ctx: _RunContext, g_crawler: GameDetailCrawler) -> None:
    if not ctx.run_postgame_reconciliation:
        logger.info("\n🧩 Step 2.5: Postgame reconciliation skipped.")
        return

    reconcile_start = (
        datetime.strptime(ctx.target_date, "%Y%m%d") - timedelta(days=max(0, ctx.postgame_reconcile_lookback_days))
    ).strftime("%Y%m%d")
    logger.info("\n🧩 Step 2.5: Reconciling recently started games (%s~%s)...", reconcile_start, ctx.target_date)
    reconciliation_result = await reconcile_postgame_range(
        reconcile_start,
        ctx.target_date,
        detail_crawler=g_crawler,
        concurrency=1,
        log=logger.info,
        write_contract=ctx.write_contract,
        source_reason=f"postgame_reconciliation:{reconcile_start}-{ctx.target_date}",
    )
    ctx.reconciliation_changed_ids = reconciliation_result.changed_game_ids
    ctx.reconciliation_dates = sorted({change.game_date for change in reconciliation_result.changes})
    logger.info(
        "   ✅ candidates=%s changed=%s",
        reconciliation_result.candidates,
        len(reconciliation_result.changes),
    )
    if reconciliation_result.changes:
        for line in format_reconciliation_report(reconciliation_result.changes).splitlines():
            logger.info("   %s", line)


def _handle_detail_step_exception(ctx: _RunContext) -> None:
    target_game_ids = sorted(ctx.detail_games_by_id)
    if not target_game_ids:
        target_game_ids = sorted(
            {normalized for game in ctx.detail_games if (normalized := normalize_kbo_game_id(game.get("game_id")))},
        )
    if not target_game_ids:
        return

    ctx.detail_failure_counts["exception"] = ctx.detail_failure_counts.get("exception", 0) + len(target_game_ids)
    ctx.detail_failure_game_ids.setdefault("exception", []).extend(target_game_ids)
    ctx.detail_still_missing.update(target_game_ids)
    for game_id in target_game_ids:
        _apply_detail_failure_fallback(ctx, game_id, "exception")


async def _step_2_detail_crawl(ctx: _RunContext) -> None:
    logger.info("\n\U0001f3ae Step 2: Crawling full postgame details...")
    resolver_session = SessionLocal()
    processed_game_ids_set: set[str] = set()
    _prepare_detail_targets(ctx)

    try:
        resolver = PlayerIdResolver(
            resolver_session,
            strict_game_resolution=True,
            allow_auto_register=False,
        )
        resolver.preload_season_index(ctx.year)
        g_crawler = GameDetailCrawler(resolver=resolver)

        detail_results_by_game = await _collect_detail_results(ctx, g_crawler)
        _finalize_detail_results(ctx, detail_results_by_game, processed_game_ids_set)
        await _run_postgame_reconciliation(ctx, g_crawler)
    except DAILY_STEP_EXCEPTIONS:
        logger.exception("   ❌ Error processing daily details")
        _handle_detail_step_exception(ctx)
    finally:
        resolver_session.close()


async def _step_3_refresh_status(ctx: _RunContext) -> None:
    logger.info("\n\U0001f9ed Step 3: Refreshing game status for target date...")
    status_result = refresh_game_status_for_date(ctx.target_date, today=ctx.today_kst)
    ctx.status_refresh_game_ids = [
        normalized for game_id in status_result.get("game_ids", []) if (normalized := normalize_kbo_game_id(game_id))
    ]
    logger.info(
        "   ✅ total=%s updated=%s counts=%s",
        status_result.get("total", 0),
        status_result.get("updated", 0),
        status_result.get("status_counts", {}),
    )


async def _step_4_relay_recovery(ctx: _RunContext) -> None:
    logger.info("\n\U0001f4dd Step 4: Relay recovery (events / PBP)...")
    try:
        relay_game_ids = sorted(set(ctx.processed_game_ids) | set(ctx.reconciliation_changed_ids))
        if relay_game_ids:
            ctx.relay_recovery_target_ids.update(relay_game_ids)
            logger.info("   \u2139\ufe0f Relay candidates=%s", len(relay_game_ids))
            ctx.runner(
                [
                    "scripts/fetch_kbo_pbp.py",
                    "--game-ids",
                    ",".join(relay_game_ids),
                    "--include-incomplete",
                    "--report-out",
                    f"logs/daily_update_summary/pbp_report_daily_{ctx.target_date}.csv",
                ],
            )
        else:
            logger.info("   \u2139\ufe0f No detail-success relay candidates for target date")

        healer_ids_by_date: dict[str, set[str]] = {}
        for item in ctx.healer_recovery_targets:
            game_id = item["game_id"]
            if game_id in relay_game_ids:
                continue
            healer_ids_by_date.setdefault(item["game_date"], set()).add(game_id)
        for recovery_date in sorted(healer_ids_by_date):
            healer_ids = sorted(healer_ids_by_date[recovery_date])
            ctx.relay_recovery_target_ids.update(healer_ids)
            ctx.runner(
                [
                    "scripts/fetch_kbo_pbp.py",
                    "--game-ids",
                    ",".join(healer_ids),
                    "--include-incomplete",
                    "--report-out",
                    f"logs/daily_update_summary/pbp_report_healer_{ctx.target_date}.csv",
                ],
            )
        logger.info("   \u2705 Relay recovery complete")
    except DAILY_STEP_EXCEPTIONS:
        logger.exception("   \u274c Error generating relay events")


async def _step_4_5_proactive_relay(ctx: _RunContext) -> None:
    logger.info("\n\U0001f50d Step 4.5: Proactive Relay Recovery (Last 30 days)...")
    try:
        with SessionLocal() as session:
            thirty_days_ago = datetime.now(KST).date() - timedelta(days=30)

            valid_wpa_event_ids = (
                select(GameEvent.game_id)
                .where(
                    GameEvent.wpa.isnot(None),
                    GameEvent.win_expectancy_before.isnot(None),
                    GameEvent.win_expectancy_after.isnot(None),
                    GameEvent.home_score.isnot(None),
                    GameEvent.away_score.isnot(None),
                    GameEvent.outs.isnot(None),
                    or_(GameEvent.base_state.isnot(None), GameEvent.bases_after.isnot(None)),
                )
                .distinct()
            )

            stmt = (
                select(Game.game_id)
                .where(
                    Game.game_date >= thirty_days_ago,
                    Game.game_date <= datetime.now(KST).date(),
                    Game.game_status.in_(["COMPLETED", "DRAW", GAME_STATUS_UNRESOLVED, GAME_STATUS_SCHEDULED]),
                )
                .where(
                    or_(
                        ~Game.game_id.in_(select(GamePlayByPlay.game_id).distinct()),
                        ~Game.game_id.in_(select(GameEvent.game_id).distinct()),
                        ~Game.game_id.in_(valid_wpa_event_ids),
                    ),
                )
            )
            missing_relay_game_ids = session.execute(stmt).scalars().all()

            if missing_relay_game_ids:
                logger.info(
                    "   ⚠️ Found %s games missing PBP/event/WPA data. Attempting recovery...",
                    len(missing_relay_game_ids),
                )
                to_recover = [gid for gid in missing_relay_game_ids if gid not in ctx.relay_recovery_target_ids]
                if to_recover:
                    ctx.runner(
                        [
                            "scripts/fetch_kbo_pbp.py",
                            "--game-ids",
                            ",".join(to_recover),
                            "--include-incomplete",
                            "--report-out",
                            f"logs/daily_update_summary/pbp_report_proactive_{ctx.target_date}.csv",
                        ],
                    )
                    ctx.relay_recovery_target_ids.update(to_recover)
                    logger.info("   \u2705 Proactive recovery initiated for %s games", len(to_recover))
                else:
                    logger.info("   \u2139\ufe0f Missing games already covered in Step 4")
            else:
                logger.info("   \u2705 No missing PBP/event/WPA data detected in recent games")
    except RUNNER_EXCEPTIONS:
        logger.exception("   \u274c Error in proactive relay recovery")


async def _step_5_content_generation(ctx: _RunContext) -> None:
    ctx.freshness_dates = sorted(
        {ctx.target_date} | set(ctx.reconciliation_dates) | {item["game_date"] for item in ctx.healer_recovery_targets},
    )

    logger.info("\n\U0001f4dd Step 5: Post-game review/WPA generation...")
    try:
        for f_date in ctx.freshness_dates:
            review_args = ["-m", "src.cli.daily_review_batch", "--date", f_date, "--no-sync"]
            ctx.runner(review_args)
        logger.info("   \u2705 Review context generation complete")
    except RUNNER_EXCEPTIONS:
        logger.exception("   \u274c Error generating review context")

    logger.info("\n\U0001f3ac Step 5.2: Daily highlight generation...")
    try:
        for f_date in ctx.freshness_dates:
            highlight_args = ["-m", "src.cli.daily_highlight_batch", "--date", f_date, "--no-sync"]
            ctx.runner(highlight_args)
        logger.info("   \u2705 Daily highlight generation complete")
    except RUNNER_EXCEPTIONS:
        logger.exception("   \u274c Error generating daily highlights")

    logger.info("\n\U0001f4da Step 5.5: LLM-ready game story generation...")
    try:
        for f_date in ctx.freshness_dates:
            story_args = ["-m", "src.cli.daily_story_batch", "--date", f_date, "--no-sync"]
            ctx.runner(story_args)
        logger.info("   \u2705 Game story generation complete")
    except CRAWLER_STEP_EXCEPTIONS:
        logger.exception("   \u274c Error generating game stories")


async def _step_6_player_stats(ctx: _RunContext) -> None:
    logger.info("\n\U0001f4c8 Step 6: Updating cumulative player stats...")
    if ctx.skip_season_stats:
        logger.info("   \u23ed\ufe0f Season stats update skipped by operator flag")
        return

    active_series = sorted({g.get("season_type", "regular") for g in ctx.daily_games if g.get("season_type")})
    if not active_series:
        active_series = ["regular"]

    logger.info("   \U0001f50d Active series detected: %s", active_series)

    try:
        for series_key in active_series:
            logger.info("   [%s] Updating Batting Stats...", series_key)
            await asyncio.to_thread(
                crawl_series_batting_stats,
                year=ctx.year,
                series_key=series_key,
                save_to_db=True,
                headless=ctx.headless,
                limit=ctx.limit,
            )
            logger.info("   [%s] Updating Pitching Stats...", series_key)
            await asyncio.to_thread(
                crawl_pitcher_series,
                year=ctx.year,
                series_key=series_key,
                save_to_db=True,
                headless=ctx.headless,
                limit=ctx.limit,
            )
        logger.info("   \u2705 Local cumulative stats for %s %s series updated", ctx.year, active_series)
    except RUNNER_EXCEPTIONS:
        logger.exception("   \u274c Error during stats update")


async def _step_6_5_maintenance(ctx: _RunContext) -> None:
    logger.info("\n\U0001fa79 Step 6.5: Backfilling starting pitchers from stats...")
    try:
        backfill_args = [
            "-m",
            "src.cli.backfill_starting_pitchers_from_stats",
            "--start-date",
            ctx.target_date,
            "--end-date",
            ctx.target_date,
        ]
        if ctx.sync:
            backfill_args.append("--sync")
        ctx.runner(backfill_args)
        logger.info("   \u2705 Starting pitcher backfill complete")
    except RUNNER_EXCEPTIONS:
        logger.exception("   \u274c Error during pitcher backfill")

    logger.info("\n\U0001f575\ufe0f  Step 6.6: Auditing season stats vs transactional details (Auto-remediation)...")
    try:
        audit_cmd = ["scripts/verification/audit_fallback_stats.py", "--year", str(ctx.year), "--type", "all"]
        if ctx.fix:
            audit_cmd.append("--fix")
        ctx.runner(audit_cmd)
        logger.info("   \u2705 Statistical audit and auto-remediation complete")
    except CRAWLER_STEP_EXCEPTIONS:
        logger.exception("   \u26a0\ufe0f Statistical audit/fix found issues (see logs)")


async def _step_7_rosters(ctx: _RunContext) -> None:
    ctx.r_target_date = datetime.strptime(ctx.target_date, "%Y%m%d").strftime("%Y-%m-%d")

    logger.info("\n\U0001f504 Step 7: Updating player movements and daily rosters...")
    try:
        m_crawler = PlayerMovementCrawler()
        movements = await m_crawler.crawl_years(ctx.year, ctx.year, save_snapshots=True)
        if movements:
            m_repo = PlayerRepository()
            m_count = m_repo.save_player_movements(movements)
            logger.info("   \u2705 Saved %s player movements for %s", m_count, ctx.year)

        r_crawler = DailyRosterCrawler()
        rosters = await r_crawler.crawl_date_range(ctx.r_target_date, ctx.r_target_date)
        if rosters:
            with SessionLocal() as session:
                r_repo = TeamRepository(session)
                r_count = r_repo.save_daily_rosters(rosters)
                logger.info("   \u2705 Saved %s daily roster records for %s", r_count, ctx.r_target_date)

        rt_crawler = RosterTransactionCrawler()
        roster_transactions = await rt_crawler.run(save=True, target_date=ctx.r_target_date)
        ctx.p0_non_game_counts["roster_transactions"] = len(roster_transactions)
        logger.info(
            "   \u2705 Roster transactions checked for %s: %s rows", ctx.r_target_date, len(roster_transactions)
        )
    except CRAWLER_STEP_EXCEPTIONS:
        logger.exception("   \u274c Error updating player movements/rosters")
        ctx.p0_non_game_errors["roster_transactions"] = "roster_pipeline_failed"


async def _step_7_5_p0_non_game(ctx: _RunContext) -> None:
    logger.info("\n\U0001f39f\ufe0f Step 7.5: Updating P0 non-game events and tickets...")
    if ctx.run_p0_non_game:
        try:
            event_crawler = TeamEventCrawler(days_back=3)
            team_events = await event_crawler.run(save=True)
            ctx.p0_non_game_counts["team_events"] = len(team_events)
            logger.info("   \u2705 Team events checked: %s rows", len(team_events))
        except CRAWLER_STEP_EXCEPTIONS as exc:
            logger.exception("   \u26a0\ufe0f Team event crawler failed")
            ctx.p0_non_game_errors["team_events"] = str(exc) or exc.__class__.__name__

        try:
            ticket_crawler = TicketCrawler()
            ticket_prices = await ticket_crawler.run(save=True, season=ctx.year)
            ctx.p0_non_game_counts["ticket_prices"] = len(ticket_prices)
            logger.info("   \u2705 Ticket prices checked for %s: %s rows", ctx.year, len(ticket_prices))
        except CRAWLER_STEP_EXCEPTIONS as exc:
            logger.exception("   \u26a0\ufe0f Ticket crawler failed")
            ctx.p0_non_game_errors["ticket_prices"] = str(exc) or exc.__class__.__name__
    else:
        logger.info("   \u23ed\ufe0f P0 non-game event/ticket crawlers skipped by operator flag")
        ctx.p0_non_game_counts["skipped"] = 1


async def _step_8_derived_stats(ctx: _RunContext) -> None:
    logger.info("\n\U0001f4ca Step 8: Rebuilding derived standings...")
    try:
        ctx.runner(["-m", "src.cli.calculate_standings", "--year", str(ctx.year)])
        ctx.derived_refresh.append("standings")
    except RUNNER_EXCEPTIONS:
        logger.exception("   \u274c Error calculating standings")

    logger.info("\n\U0001f9ee Step 9: Recalculating matchup splits...")
    try:
        ctx.runner(["-m", "src.cli.calculate_matchups", "--year", str(ctx.year)])
        ctx.derived_refresh.append("matchups")
        logger.info("   \u2705 Matchup splits recalculated successfully")
    except RUNNER_EXCEPTIONS:
        logger.exception("   \u274c Error recalculating matchups")

    logger.info("\n\U0001f3f7\ufe0f Step 10: Recalculating stat rankings...")
    try:
        ctx.runner(["-m", "src.cli.calculate_rankings", "--year", str(ctx.year)])
        ctx.derived_refresh.append("stat_rankings")
        logger.info("   \u2705 Stat rankings recalculated successfully")
    except RUNNER_EXCEPTIONS:
        logger.exception("   \u274c Error recalculating stat rankings")

    logger.info("\n\U0001f4c8 Step 10.6: Calculating advanced Sabermetrics (wOBA, wRC+, WAR)...")
    try:
        ctx.runner(["-m", "src.cli.calculate_sabermetrics", "--years", str(ctx.year)])
        logger.info("   \u2705 Sabermetrics engine completed successfully")
    except RUNNER_EXCEPTIONS:
        logger.exception("   \u274c Error calculating Sabermetrics")


async def _step_10_7_enrichment(ctx: _RunContext) -> None:
    logger.info("\n\U0001f3ad Step 10.7: Enriching new player profiles (fetching missing photos/details)...")
    try:
        ctx.runner(["scripts/backfill_player_profiles.py", "--limit", "0", "--delay", "1.0"])
        logger.info("   \u2705 Player profile enrichment complete")
    except DAILY_STEP_EXCEPTIONS:
        logger.exception("   \u26a0\ufe0f Profile enrichment found issues (continning)")

    logger.info("\n\U0001f575\ufe0f  Step 10.8: Deep statistical logic audit (cross-table invariants)...")
    try:
        from scripts.verification.audit_game_logic import audit_game_logic

        violations = audit_game_logic(year=ctx.year)

        if violations:
            inconsistent_ids = sorted({v["game_id"] for v in violations})
            logger.warning(
                "   \u26a0\ufe0f  Audit found %s inconsistencies in %s games.", len(violations), len(inconsistent_ids)
            )
            logger.info("   🚀 Triggering targeted self-healing for: %s...", ", ".join(inconsistent_ids[:5]))

            await run_healer_async(target_game_ids=inconsistent_ids)

            logger.info("   \U0001f50d Re-auditing after repair...")
            violations_after = audit_game_logic(year=ctx.year)
            if not violations_after:
                logger.info("   \u2705 All inconsistencies resolved automatically.")
            else:
                remaining_ids = sorted({v["game_id"] for v in violations_after})
                logger.error(
                    "   ❌ %s inconsistencies still remain in %s games.",
                    len(violations_after),
                    len(remaining_ids),
                )
        else:
            logger.info("   \u2705 Deep statistical logic audit complete (No issues found)")
    except DAILY_STEP_EXCEPTIONS:
        logger.exception("   \u26a0\ufe0f  Deep statistical audit/heal process failed")


def _set_candidate_sync_game_ids(ctx: _RunContext) -> None:
    ctx.candidate_sync_game_ids = sorted(
        {game["game_id"] for game in ctx.daily_games}
        | set(ctx.status_refresh_game_ids)
        | set(ctx.processed_game_ids)
        | set(ctx.reconciliation_changed_ids)
        | {item["game_id"] for item in ctx.healer_recovery_targets}
        | ctx.relay_recovery_target_ids,
    )


def _run_pre_oci_freshness_gate(ctx: _RunContext) -> None:
    logger.info("\n\U0001f9ea Step 11: Freshness gate before OCI publish...")
    freshness_ok = True
    for freshness_date in ctx.freshness_dates:
        try:
            ctx.runner(["-m", "src.cli.freshness_gate", "--date", freshness_date])
        except subprocess.CalledProcessError:
            freshness_ok = False
            logger.exception("   \u26a0\ufe0f Freshness gate found issues for %s (continuing)", freshness_date)
    if freshness_ok:
        logger.info("   \u2705 Freshness gate passed")


def _run_local_integrity_gate() -> None:
    logger.info("\n\U0001f575\ufe0f  Step 11.5: Local game status integrity audit...")
    try:
        _run_game_status_integrity_audit()
        logger.info("   \u2705 Local integrity audit passed")
    except RuntimeError as exc:
        logger.exception("   \u274c Local integrity audit FAILED")
        msg = "Aborting OCI sync due to local data integrity violations."
        raise RuntimeError(msg) from exc


def _run_statistical_quality_gate_for_sync(ctx: _RunContext) -> None:
    logger.info("\n\u2696\ufe0f Step 12: Statistical quality gate check...")
    try:
        ctx.runner(["-m", "src.cli.quality_gate_check", "--year", str(ctx.year)])
        logger.info("   \u2705 Statistical quality gate passed")
    except subprocess.CalledProcessError:
        reason = "non_p0_statistical_quality_gate_failed"
        ctx.non_p0_quality_gate_counts[reason] = ctx.non_p0_quality_gate_counts.get(reason, 0) + 1
        ctx.non_p0_quality_gate_ids.setdefault(reason, []).append(f"season:{ctx.year}")
        logger.exception("   \u26a0\ufe0f Non-P0 statistical quality gate failed (continuing OCI game publish)")


def _recalculate_season_aggregates_for_quality_gate(ctx: _RunContext) -> None:
    logger.info("\n\U0001f9ee Step 11.75: Refreshing season aggregates before statistical quality gate...")
    try:
        ctx.runner(["-m", "src.cli.recalc_player_stats", "--season", str(ctx.year)])
        ctx.runner(["-m", "src.cli.recalc_team_stats", "--season", str(ctx.year)])
        logger.info("   \u2705 Season aggregates refreshed")
    except subprocess.CalledProcessError:
        reason = "non_p0_season_aggregate_recalc_failed"
        ctx.non_p0_quality_gate_counts[reason] = ctx.non_p0_quality_gate_counts.get(reason, 0) + 1
        ctx.non_p0_quality_gate_ids.setdefault(reason, []).append(f"season:{ctx.year}")
        logger.exception("   \u26a0\ufe0f Non-P0 season aggregate recalculation failed (continuing OCI game publish)")


def _resolve_null_player_ids_before_quality_gate(ctx: _RunContext) -> None:
    logger.info("\n🧩 Step 11.9: Resolving NULL player_ids before OCI publish...")
    try:
        ctx.runner(
            [
                "-m",
                "scripts.maintenance.resolve_null_player_ids_conservative",
                "--years",
                str(ctx.year),
                "--apply",
                "--no-backup",
                "--delete-duplicates",
            ]
        )
        logger.info("   ✅ NULL player_id resolver complete")
    except subprocess.CalledProcessError:
        reason = "non_p0_null_player_id_resolution_failed"
        ctx.non_p0_quality_gate_counts[reason] = ctx.non_p0_quality_gate_counts.get(reason, 0) + 1
        ctx.non_p0_quality_gate_ids.setdefault(reason, []).append(f"season:{ctx.year}")
        logger.exception("   ⚠️ NULL player_id resolver failed (continuing OCI game publish)")


def _sync_oci_supporting_datasets(ctx: _RunContext, syncer: OCISync) -> None:
    if ctx.skip_oci_supporting_sync:
        ctx.oci_skip_counts["oci_supporting_sync_skipped"] = (
            ctx.oci_skip_counts.get("oci_supporting_sync_skipped", 0) + 1
        )
        ctx.oci_skip_game_ids.setdefault("oci_supporting_sync_skipped", []).append(f"season:{ctx.year}")
        logger.info("   \u23ed\ufe0f Non-P0 OCI supporting dataset sync skipped by operator flag")
        return

    try:
        syncer.sync_games(filters=[Game.game_id.like(f"{ctx.year}%")])
        syncer.sync_standings(year=ctx.year)
        syncer.sync_matchups(year=ctx.year)
        syncer.sync_stat_rankings(year=ctx.year)
        syncer.sync_player_season_batting(year=ctx.year)
        syncer.sync_player_season_pitching(year=ctx.year)
        syncer.sync_player_movements()
        syncer.sync_daily_rosters(start_date=ctx.r_target_date, end_date=ctx.r_target_date)
    except DB_STEP_EXCEPTIONS:
        logger.exception("   \u26a0\ufe0f Non-P0 OCI supporting dataset sync failed")
        ctx.oci_skip_counts["non_p0_supporting_sync_failed"] = (
            ctx.oci_skip_counts.get(
                "non_p0_supporting_sync_failed",
                0,
            )
            + 1
        )
        ctx.oci_skip_game_ids.setdefault("non_p0_supporting_sync_failed", []).append(f"season:{ctx.year}")


def _publish_to_oci(ctx: _RunContext) -> None:
    logger.info("\n\u2601\ufe0f Step 13: Synchronizing to OCI...")
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        msg = "OCI_DB_URL is required when --sync is enabled"
        raise RuntimeError(msg)

    with SessionLocal() as sync_session:
        syncer = OCISync(oci_url, sync_session)
        try:
            logger.info("   \U0001f6e1\ufe0f Syncing players/basic first to satisfy FK constraints...")
            syncer.sync_player_basic()
            syncer.sync_players()

            for game_id in ctx.candidate_sync_game_ids:
                sync_result = syncer.sync_specific_game(game_id)
                _merge_oci_skip_summary(ctx.oci_skip_counts, ctx.oci_skip_game_ids, sync_result, game_id)

            _sync_oci_supporting_datasets(ctx, syncer)
            if ctx.oci_skip_counts:
                logger.info("   \u2139\ufe0f OCI skip summary: %s", _format_counts(ctx.oci_skip_counts))
        finally:
            syncer.close()


def _run_post_oci_freshness_gate(ctx: _RunContext) -> None:
    logger.info("\n\U0001f9ea Step 13.5: Freshness gate after OCI publish...")
    for freshness_date in ctx.freshness_dates:
        try:
            ctx.runner(["-m", "src.cli.freshness_gate", "--date", freshness_date, "--source-url-env", "OCI_DB_URL"])
        except subprocess.CalledProcessError:
            logger.exception("   \u26a0\ufe0f OCI freshness gate found issues for %s (continuing)", freshness_date)


def _run_oci_parity_gate(ctx: _RunContext) -> None:
    logger.info("\n\u2696\ufe0f Step 13.6: OCI parity quality gate check...")
    try:
        _run_oci_parity_quality_gate()
        logger.info("   \u2705 OCI parity check complete")
    except RuntimeError:
        logger.exception("OCI parity quality gate failed")
        reason = "non_p0_oci_parity_quality_gate_failed"
        ctx.non_p0_quality_gate_counts[reason] = ctx.non_p0_quality_gate_counts.get(reason, 0) + 1
        ctx.non_p0_quality_gate_ids.setdefault(reason, []).append("oci")


async def _step_11_sync_pipeline(ctx: _RunContext) -> None:
    _set_candidate_sync_game_ids(ctx)
    if not ctx.sync:
        return

    _run_pre_oci_freshness_gate(ctx)
    _run_local_integrity_gate()
    _recalculate_season_aggregates_for_quality_gate(ctx)
    _resolve_null_player_ids_before_quality_gate(ctx)
    _run_statistical_quality_gate_for_sync(ctx)
    _publish_to_oci(ctx)
    _run_post_oci_freshness_gate(ctx)
    _run_oci_parity_gate(ctx)


async def _step_14_tomorrow_preview(ctx: _RunContext) -> None:
    if ctx.seed_tomorrow_preview:
        tomorrow_date = (datetime.strptime(ctx.target_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
        logger.info("\n\U0001f52e Step 14: Seeding tomorrow preview contexts (%s)...", tomorrow_date)
        try:
            preview_args = ["-m", "src.cli.daily_preview_batch", "--date", tomorrow_date]
            if not ctx.sync:
                preview_args.append("--no-sync")
            ctx.runner(preview_args)
            logger.info("   \u2705 Tomorrow preview seed complete")
        except RUNNER_EXCEPTIONS:
            logger.exception("   \u274c Error generating tomorrow preview seed")


def _build_stability_summary_for_context(ctx: _RunContext, summary_path: Path) -> dict[str, Any]:
    return _build_stability_summary(
        detail_failure_counts=ctx.detail_failure_counts,
        detail_failure_game_ids=ctx.detail_failure_game_ids,
        relay_recovery_target_ids=sorted(ctx.relay_recovery_target_ids),
        oci_skip_counts=ctx.oci_skip_counts,
        oci_skip_game_ids=ctx.oci_skip_game_ids,
        non_p0_quality_gate_counts=ctx.non_p0_quality_gate_counts,
        non_p0_quality_gate_ids=ctx.non_p0_quality_gate_ids,
        p0_non_game_counts=ctx.p0_non_game_counts,
        p0_non_game_errors=ctx.p0_non_game_errors,
        detail_recovery_passes=ctx.detail_recovery_passes,
        detail_recovered_after_retry=ctx.detail_recovered_after_retry,
        detail_still_missing=ctx.detail_still_missing,
        detail_recovery_attempts=ctx.detail_recovery_attempts,
        detail_recovery_escalation_game_ids=ctx.detail_retry_escalation_game_ids,
        summary_path=summary_path,
    )


def _build_p0_readiness_for_context(ctx: _RunContext) -> dict[str, Any]:
    try:
        with SessionLocal() as p0_session:
            return build_p0_readiness(
                p0_session,
                target_date=ctx.target_date,
                lookback_days=0,
                lookahead_days=0,
                oci_skip_counts=ctx.oci_skip_counts,
                oci_skip_game_ids=ctx.oci_skip_game_ids,
            )
    except DB_STEP_EXCEPTIONS:
        logger.exception("   Error building P0 readiness summary")
        return {
            "target_date": ctx.target_date,
            "schedule": {},
            "pregame": {},
            "live": {},
            "postgame": {},
            "relay": {},
            "roster": {},
            "broadcast": {},
            "oci": {"skip_counts": dict(ctx.oci_skip_counts), "skip_game_ids": dict(ctx.oci_skip_game_ids)},
            "failures": [
                {
                    "dataset": "p0_readiness",
                    "game_id": None,
                    "game_date": ctx.target_date,
                    "reason": "readiness_build_failed",
                    "severity": "critical",
                },
            ],
            "summary": {"ok": False, "failure_count": 1, "critical_failure_count": 1, "warning_count": 0},
        }


def _finalize_manifest_game_ids(ctx: _RunContext) -> list[str]:
    return (
        sorted(set(ctx.processed_game_ids) | set(ctx.reconciliation_changed_ids))
        or ctx.status_refresh_game_ids
        or [game["game_id"] for game in ctx.daily_games]
    )


def _write_finalize_outputs(
    ctx: _RunContext,
    stability_summary: dict[str, Any],
    p0_readiness: dict[str, Any],
    summary_path: Path,
) -> Path:
    manifest_path = write_refresh_manifest(
        phase="postgame_finalize",
        target_date=ctx.target_date,
        game_ids=_finalize_manifest_game_ids(ctx),
        datasets=[
            "game",
            "game_metadata",
            "game_inning_scores",
            "game_lineups",
            "game_events",
            "game_summary",
            "game_play_by_play",
            "team_events",
            "ticket_prices",
            "ticket_open_rules",
            "roster_transactions",
        ],
        derived_refresh=ctx.derived_refresh,
        stability=stability_summary,
    )
    _write_daily_update_summary(
        target_date=ctx.target_date,
        stability=stability_summary,
        p0_readiness=p0_readiness,
        manifest_path=manifest_path,
        summary_path=summary_path,
    )
    return manifest_path


def _log_finalize_summaries(ctx: _RunContext, p0_readiness: dict[str, Any]) -> None:
    logger.info(ctx.write_contract.summary())
    logger.info(
        "Stability summary: detail_failures=%s detail_recovery_passes=%s detail_recovered_after_retry=%s detail_still_missing=%s relay_targets=%s oci_skips=%s non_p0_quality_gates=%s p0_non_game=%s",
        _format_counts(ctx.detail_failure_counts),
        ctx.detail_recovery_passes,
        ctx.detail_recovered_after_retry,
        len(ctx.detail_still_missing),
        len(ctx.relay_recovery_target_ids),
        _format_counts(ctx.oci_skip_counts),
        _format_counts(ctx.non_p0_quality_gate_counts),
        _format_counts(ctx.p0_non_game_counts),
    )
    logger.info("P0 readiness: %s", format_p0_readiness_summary(p0_readiness))


def _load_pbp_attempts_by_game(target_date: str) -> dict[str, list[dict[str, str]]]:
    attempts_by_game: dict[str, list[dict[str, str]]] = {}
    for file_path in Path("logs/daily_update_summary").glob(f"pbp_report_*_{target_date}.csv"):
        try:
            with file_path.open(encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    gid = row.get("game_id")
                    if gid:
                        attempts_by_game.setdefault(gid, []).append(row)
        except FILE_READ_EXCEPTIONS:
            logger.exception("Failed to read PBP report file: %s", file_path)
    return attempts_by_game


def _normalize_pbp_attempt_notes(notes: str) -> str:
    if "final_score_mismatch" in notes:
        return "score_mismatch"
    if "missing_middle_inning" in notes:
        return "inning_gap"
    return notes


def _summarize_pbp_failed_game(gid: str, attempts: list[dict[str, str]]) -> str:
    if not attempts:
        return f"- `{gid}`: No logs found"

    attempt_summaries = []
    for att in attempts:
        source = att.get("source_name", "unknown")
        status = att.get("status", "unknown")
        notes = _normalize_pbp_attempt_notes(att.get("notes") or "")
        summary = f"*{source}*:{status}"
        if notes:
            summary += f" ({notes})"
        attempt_summaries.append(summary)
    return f"- `{gid}`: " + " -> ".join(attempt_summaries)


def _build_pbp_failed_details(failed_ids: set[str], attempts_by_game: dict[str, list[dict[str, str]]]) -> list[str]:
    return [_summarize_pbp_failed_game(gid, attempts_by_game.get(gid) or []) for gid in sorted(failed_ids)]


def _build_pbp_recovery_blocks(
    ctx: _RunContext,
    success_count: int,
    failed_count: int,
    failed_details: list[str],
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": "Daily PBP Recovery Report"}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Target Games:* {len(ctx.relay_recovery_target_ids)}"},
                {"type": "mrkdwn", "text": f"*Recovered:* {success_count}"},
                {"type": "mrkdwn", "text": f"*Failed:* {failed_count}"},
            ],
        },
    ]
    if failed_details:
        failed_text = "\n".join(failed_details)
        if len(failed_text) > 2900:
            failed_text = failed_text[:2800] + "\n... (truncated)"
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Detailed Failures:*\n{failed_text}",
                },
            },
        )
    return blocks


def _send_pbp_recovery_report(ctx: _RunContext) -> None:
    logger.info("\nStep 14: PBP Recovery Alerting...")
    if not ctx.relay_recovery_target_ids:
        logger.info("   No PBP recovery targets for today.")
        return

    try:
        with SessionLocal() as session:
            recovered_pbp_ids = (
                session.execute(
                    select(GamePlayByPlay.game_id)
                    .where(GamePlayByPlay.game_id.in_(list(ctx.relay_recovery_target_ids)))
                    .distinct(),
                )
                .scalars()
                .all()
            )

        failed_ids = set(ctx.relay_recovery_target_ids) - set(recovered_pbp_ids)
        success_count = len(recovered_pbp_ids)
        failed_count = len(failed_ids)
        attempts_by_game = _load_pbp_attempts_by_game(ctx.target_date)
        failed_details = _build_pbp_failed_details(failed_ids, attempts_by_game)
        blocks = _build_pbp_recovery_blocks(ctx, success_count, failed_count, failed_details)
        SlackWebhookClient.send_alert(f"*Daily PBP Recovery Report ({ctx.target_date})*", blocks=blocks)
        logger.info(
            "   Sent PBP recovery summary to Slack (Success: %s, Failed: %s)",
            success_count,
            failed_count,
        )
    except (*DB_STEP_EXCEPTIONS, *ALERT_EXCEPTIONS):
        logger.exception("   Error sending PBP recovery summary")


def _finalize_run_update(ctx: _RunContext) -> dict[str, Any]:
    summary_path = _daily_summary_path(ctx.target_date, ctx.summary_dir)
    stability_summary = _build_stability_summary_for_context(ctx, summary_path)
    p0_readiness = _build_p0_readiness_for_context(ctx)
    manifest_path = _write_finalize_outputs(ctx, stability_summary, p0_readiness, summary_path)
    _log_finalize_summaries(ctx, p0_readiness)
    _send_pbp_recovery_report(ctx)

    logger.info("\n%s", "=" * 60)
    logger.info("Daily Finalize Finished for %s", ctx.target_date)
    logger.info("Refresh Manifest: %s", manifest_path)
    logger.info("Daily Summary: %s", summary_path)
    logger.info("%s\n", "=" * 60)

    return {
        "phase": "postgame_finalize",
        "target_date": ctx.target_date,
        "manifest_path": str(manifest_path),
        "summary_path": str(summary_path),
        "stability": stability_summary,
        "p0_readiness": p0_readiness,
    }


async def run_update(
    target_date: str,
    *,
    sync: bool = False,
    headless: bool = True,
    limit: int | None = None,
    step_runner: Callable[[Sequence[str]], None] | None = None,
    summary_dir: str | Path | None = None,
    seed_tomorrow_preview: bool = False,
    run_auto_healer: bool = True,
    run_postgame_reconciliation: bool = True,
    postgame_reconcile_lookback_days: int = 3,
    fix: bool = False,
    skip_season_stats: bool = False,
    skip_oci_supporting_sync: bool = False,
    run_p0_non_game: bool = True,
) -> dict[str, Any]:
    """Main orchestration logic for postgame finalize and daily reconciliation."""
    ctx = _RunContext(
        target_date=target_date,
        sync=sync,
        year=int(target_date[:4]),
        month=int(target_date[4:6]),
        today_kst=_today_kst(),
        runner=step_runner or _run_python_step,
        write_contract=GameWriteContract(run_label=f"daily_update:{target_date}", log=logger.info),
        step_runner=step_runner,
        summary_dir=summary_dir,
        seed_tomorrow_preview=seed_tomorrow_preview,
        run_auto_healer=run_auto_healer,
        run_postgame_reconciliation=run_postgame_reconciliation,
        postgame_reconcile_lookback_days=postgame_reconcile_lookback_days,
        fix=fix,
        skip_season_stats=skip_season_stats,
        skip_oci_supporting_sync=skip_oci_supporting_sync,
        run_p0_non_game=run_p0_non_game,
        headless=headless,
        limit=limit,
        detail_recovery_queue=RecoveryManager(checkpoint_path=DETAIL_RECOVERY_QUEUE_PATH),
    )
    ctx.detail_recovery_queue.purge_detail_recovery_queue()
    ctx.queued_recovery_game_ids = set(
        ctx.detail_recovery_queue.get_due_detail_recovery_targets(
            target_date,
            cooldown_minutes=DETAIL_RECOVERY_COOLDOWN_MINUTES,
        ),
    )

    logger.info("\n%s", "=" * 60)
    logger.info("\U0001f680 KBO Daily Finalize Started for Date: %s", target_date)
    logger.info("%s", "=" * 60)

    await _step_0_auto_healer(ctx)
    await _step_1_schedule(ctx)
    await _step_2_detail_crawl(ctx)
    await _step_3_refresh_status(ctx)
    await _step_4_relay_recovery(ctx)
    await _step_4_5_proactive_relay(ctx)
    await _step_5_content_generation(ctx)
    await _step_6_player_stats(ctx)
    await _step_6_5_maintenance(ctx)
    await _step_7_rosters(ctx)
    await _step_7_5_p0_non_game(ctx)
    await _step_8_derived_stats(ctx)
    await _step_10_7_enrichment(ctx)
    await _step_11_sync_pipeline(ctx)
    await _step_14_tomorrow_preview(ctx)

    return _finalize_run_update(ctx)


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
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Enable auto-remediation (self-healing) during season stats audit",
    )
    parser.add_argument(
        "--skip-season-stats",
        action="store_true",
        help="Skip cumulative season stat crawling for scoped P0 recovery/finalize runs.",
    )
    parser.add_argument(
        "--skip-oci-supporting-sync",
        action="store_true",
        help="Skip year-level non-P0 OCI supporting dataset sync after targeted game publish.",
    )
    parser.add_argument(
        "--skip-p0-non-game",
        action="store_true",
        help="Skip P0 non-game event/ticket crawlers for scoped historical backfills.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    target_date = args.date
    if not target_date:
        target_date = (datetime.now(KST) - timedelta(days=1)).strftime("%Y%m%d")
    elif len(target_date) != 8 or not target_date.isdigit():
        logger.error("❌ Invalid date format: %s. Please use YYYYMMDD.", target_date)
        sys.exit(1)

    from src.utils.lock import ProcessLock

    lock = ProcessLock("daily_update", blocking=False)
    if not lock.acquire():
        logger.warning("⚠️ Another instance of run_daily_update is already running. Exiting.")
        return 1

    try:
        res = asyncio.run(
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
                fix=args.fix or os.getenv("DAILY_AUTO_REMEDIATION", "0") == "1",
                skip_season_stats=args.skip_season_stats,
                skip_oci_supporting_sync=args.skip_oci_supporting_sync,
                run_p0_non_game=not args.skip_p0_non_game,
            ),
        )
    finally:
        lock.release()
    return res


if __name__ == "__main__":
    main()
