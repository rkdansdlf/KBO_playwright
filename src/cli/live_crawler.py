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
from collections.abc import Sequence
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx

logger = logging.getLogger(__name__)

_LIVE_SHARD_CURSOR_BY_DATE: dict[str, int] = {}

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.naver_relay_crawler import NaverRelayCrawler
from src.crawlers.schedule_crawler import ScheduleCrawler
from src.db.engine import SessionLocal
from src.repositories.game_repository import GAME_STATUS_LIVE, save_game_snapshot, save_relay_data
from src.sync.oci_sync import OCISync
from src.utils.game_state import (
    TERMINAL_STATES,
    derive_lifecycle_from_naver_status,
)
from src.utils.refresh_manifest import write_refresh_manifest


def _has_ending_header(raw_pbp_rows: list[dict[str, Any]]) -> bool:
    """Check if the last inning_header contains game-ending keywords."""
    for row in reversed(raw_pbp_rows):
        if row.get("event_type") == "inning_header":
            return False
        desc = str(row.get("play_description") or "")
        if any(term in desc for term in ["경기 종료", "게임 종료", "경기종료"]):
            return True
    return False


def _select_live_shard(items: list[Any], *, shard_key: str, max_items: int | None) -> list[Any]:
    if max_items is None or max_items <= 0 or len(items) <= max_items:
        return list(items)

    cursor = _LIVE_SHARD_CURSOR_BY_DATE.get(shard_key, 0) % len(items)
    doubled = items + items
    selected = doubled[cursor : cursor + max_items]
    _LIVE_SHARD_CURSOR_BY_DATE[shard_key] = (cursor + max_items) % len(items)

    for key in list(_LIVE_SHARD_CURSOR_BY_DATE):
        if key != shard_key:
            _LIVE_SHARD_CURSOR_BY_DATE.pop(key, None)

    return selected


def _query_enriched_game_state(
    game_ids: list[str],
) -> dict[str, dict[str, int]]:
    """Query DB for event count and max inning per game_id.

    Returns {game_id: {"event_count": int, "max_inning": int}}.
    """
    if not game_ids:
        return {}
    try:
        from sqlalchemy import func

        from src.db.engine import SessionLocal
        from src.models.game import GameEvent, GamePlayByPlay

        state: dict[str, dict[str, int]] = {}
        with SessionLocal() as session:
            # Batch query event counts
            ec_rows = (
                session.query(
                    GameEvent.game_id,
                    func.count(GameEvent.id),
                )
                .filter(GameEvent.game_id.in_(game_ids))
                .group_by(GameEvent.game_id)
                .all()
            )
            ec_map = {r[0]: r[1] for r in ec_rows}

            # Batch query max innings
            mi_rows = (
                session.query(
                    GamePlayByPlay.game_id,
                    func.max(GamePlayByPlay.inning),
                )
                .filter(GamePlayByPlay.game_id.in_(game_ids))
                .group_by(GamePlayByPlay.game_id)
                .all()
            )
            mi_map = {r[0]: r[1] or 0 for r in mi_rows}

            for gid in game_ids:
                state[gid] = {
                    "event_count": ec_map.get(gid, 0),
                    "max_inning": mi_map.get(gid, 0),
                }
        return state
    except Exception:
        logger.exception("[WARN] Failed to query enriched game state")
        return {}


def _compute_enriched_interval(
    base_interval: int,
    game_ids_playing: list[str],
    last_event_counts: dict[str, int],
    enriched_state: dict[str, dict[str, int]] | None = None,
) -> tuple[int, str, dict[str, int]]:
    """Improve the base dynamic interval using at-bat/inning/event-density awareness.

    Returns (sleep_seconds, extra_note, updated_last_event_counts).
    """
    MIN_POLLING_INTERVAL = 5
    if not game_ids_playing or not enriched_state:
        return max(MIN_POLLING_INTERVAL, base_interval), "", dict(last_event_counts)

    updated_counts = dict(last_event_counts)
    multipliers: list[float] = []

    for gid in game_ids_playing:
        gs = enriched_state.get(gid)
        if gs is None:
            continue

        ec = gs["event_count"]
        prev = last_event_counts.get(gid)
        updated_counts[gid] = ec

        # Idle detection: no new events since last cycle
        if prev is not None and ec == prev and ec > 0:
            multipliers.append(1.8)
        elif prev is not None and ec > prev:
            multipliers.append(0.6)

        # Late-game acceleration: inning >= 7
        if gs["max_inning"] >= 7:
            multipliers.append(0.7)

    if not multipliers:
        return max(MIN_POLLING_INTERVAL, base_interval), "", updated_counts

    # Apply the most aggressive (lowest) multiplier among active games
    combined = min(multipliers)
    final = max(MIN_POLLING_INTERVAL, min(120, int(round(base_interval * combined))))

    note_parts = []
    if any(m < 1.0 for m in multipliers):
        note_parts.append("accelerated")
    if any(m > 1.0 for m in multipliers):
        note_parts.append("idle_backoff")
    extra_note = f"(enriched:{','.join(note_parts)} base={base_interval}s→{final}s)" if note_parts else ""

    return final, extra_note, updated_counts


_ACTIVE_HEALING_GAMES: set[str] = set()


async def _run_kbo_fallback_healing(game_id: str) -> None:
    """Run KBO official website re-crawl as a background fallback task when validation fails."""
    try:
        from src.crawlers.pbp_crawler import PBPCrawler
        from src.utils.alerting import SlackWebhookClient

        logger.info(
            f"[FALLBACK TRIGGER] PBP for {game_id} is unverified. Triggering KBO website re-crawl in background..."
        )
        kbo_crawler = PBPCrawler()
        kbo_data = None
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"[FALLBACK TRIGGER] Attempt {attempt} to crawl KBO PBP for {game_id}...")
                kbo_data = await kbo_crawler.crawl_game_events(game_id)
                if kbo_data and kbo_data.get("events"):
                    break
                else:
                    raise ValueError("KBO PBP crawl returned no events")
            except Exception as fallback_err:  # noqa: BLE001
                logger.warning(f"KBO fallback attempt {attempt} failed for {game_id}: {fallback_err}", exc_info=True)
                if attempt == max_attempts:
                    logger.error(f"[FALLBACK ERROR] KBO fallback failed all {max_attempts} attempts for {game_id}")
                    break
                else:
                    backoff = 2.0**attempt
                    await asyncio.sleep(backoff)

        if kbo_data and kbo_data.get("events"):
            try:
                saved = save_relay_data(
                    game_id,
                    events=kbo_data["events"],
                    source_name="kbo_fallback",
                    notes="Automatically re-crawled due to Naver validation failure.",
                )
                if saved:
                    msg = f"✅ KBO Fallback Success: Recovered {saved} unverified Naver PBP events from KBO for game {game_id}"
                    logger.info(f"[FALLBACK SUCCESS] {msg}")
                    SlackWebhookClient.send_alert(msg)
            except Exception as db_err:
                logger.error(f"Failed to save KBO fallback data for {game_id}: {db_err}", exc_info=True)
    except Exception as exc:
        logger.error(f"Unexpected exception in background KBO healing for {game_id}: {exc}", exc_info=True)
    finally:
        _ACTIVE_HEALING_GAMES.discard(game_id)


async def _process_single_live_game(
    game: dict[str, Any],
    lifecycle_state: str | None,
    nav_status_raw: str | None,
    relay_crawler: Any,
    detail_crawler: Any,
    today_str: str,
) -> tuple[str | None, str]:
    """Process a single live game by crawling its relay, updating DB, and triggering healing if needed.

    Returns (touched_game_id, resolved_lifecycle).
    """
    game_id = game["game_id"]

    logger.info(
        f"[LIVE] 🔍 Crawling active game: {game_id} (lifecycle={lifecycle_state}, nav_status={nav_status_raw or 'UNKNOWN'})"
    )

    relay_data = await relay_crawler.crawl_game_events(game_id)
    flat_events = list((relay_data or {}).get("events") or [])
    raw_pbp_rows = list((relay_data or {}).get("raw_pbp_rows") or [])

    # Determine lifecycle state from relay data + Naver status
    last_desc = ""
    if flat_events:
        last_desc = flat_events[-1].get("description", "")
    elif raw_pbp_rows:
        last_desc = raw_pbp_rows[-1].get("play_description", "")

    # Detect suspension from relay text
    detected_suspension = any(term in last_desc for term in ["중단", "지연", "우천", "서스펜디드"])

    # Detect game ending text
    detected_game_end = _has_ending_header(raw_pbp_rows) or any(
        term in last_desc for term in ["경기 종료", "게임 종료", "경기종료"]
    )

    # Resolve final lifecycle state:
    # Priority: Naver status > text detection > default
    if lifecycle_state == "result_pending_stabilization" or detected_game_end:
        resolved_lifecycle = "result_pending_stabilization"
    elif lifecycle_state == "suspended" or detected_suspension:
        resolved_lifecycle = "suspended"
    elif lifecycle_state == "delayed":
        resolved_lifecycle = "delayed"
    elif lifecycle_state == "running":
        resolved_lifecycle = "running"
    else:
        resolved_lifecycle = "running"

    touched = False
    if flat_events or raw_pbp_rows:
        saved_rows = save_relay_data(
            game_id,
            flat_events,
            raw_pbp_rows=raw_pbp_rows,
            source_name="naver_live",
            parser_version=(relay_data or {}).get("parser_version"),
            source_schema_version=(relay_data or {}).get("source_schema_version"),
            payload_hash=(relay_data or {}).get("payload_hash"),
            game_lifecycle_state=resolved_lifecycle,
        )
        if saved_rows:
            touched = True
            logger.info(f"[LIVE] 📝 Synced {saved_rows} relay rows for {game_id}")

        detail = await detail_crawler.crawl_game(game_id, today_str, lightweight=True)
        if detail and save_game_snapshot(detail, status=GAME_STATUS_LIVE):
            touched = True
            logger.info(f"[LIVE] 📊 Updated scoreboard snapshot for {game_id}")

        # Fallback auto-healing trigger: if the game is finished but validation failed
        if resolved_lifecycle == "result_pending_stabilization":
            from src.models.game import GameMetadata

            with SessionLocal() as session:
                meta = session.query(GameMetadata).filter(GameMetadata.game_id == game_id).first()
                val_status = (
                    meta.source_payload.get("pbp_validation_status")
                    if meta and isinstance(meta.source_payload, dict)
                    else None
                )
                if val_status == "unverified" and game_id not in _ACTIVE_HEALING_GAMES:
                    _ACTIVE_HEALING_GAMES.add(game_id)
                    asyncio.create_task(_run_kbo_fallback_healing(game_id))

    return (game_id if touched else None), resolved_lifecycle


async def run_live_crawler_cycle(
    *,
    sync_to_oci: bool | None = None,
    max_active_games: int | None = None,
) -> dict[str, Any]:
    """Run one live polling cycle. Returns status dictionary."""
    seoul_tz = ZoneInfo("Asia/Seoul")
    now = datetime.now(seoul_tz)
    today_str = now.strftime("%Y%m%d")

    logger.info(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] 🚨 Live Crawl Cycle Started")

    sched_crawler = ScheduleCrawler()
    games = await sched_crawler.crawl_schedule(now.year, now.month)
    today_games = [g for g in games if g["game_date"].replace("-", "") == today_str]

    if not today_games:
        logger.info(f"[INFO] No games scheduled for today ({today_str}).")
        return {
            "active": False,
            "active_playing": False,
            "active_suspended": False,
            "all_finished": True,
            "game_ids_playing": [],
        }

    # Optimization: Fetch latest game statuses from Naver to skip unnecessary crawls
    relay_crawler = NaverRelayCrawler()
    try:
        async with httpx.AsyncClient() as client:
            query = relay_crawler._schedule_query_context(query_date=today_str)
            response = await client.get(
                relay_crawler.schedule_api_base_url,
                params=query,
                headers=relay_crawler.headers,
                timeout=10.0,
            )
            if response.status_code == 200:
                payload = response.json()
                naver_games = list((payload.get("result") or {}).get("games") or [])
                # Store naver status mapping for quick lookup
                naver_status_map = {
                    (ng.get("awayTeamCode"), ng.get("homeTeamCode")): ng.get("status")
                    for ng in naver_games
                    if ng.get("status")
                }
            else:
                naver_status_map = {}
    except Exception:
        logger.exception("[WARN] Failed to fetch Naver live statuses")
        naver_status_map = {}

    detail_crawler = GameDetailCrawler(request_delay=0.1)
    touched_game_ids: set[str] = set()
    active_playing_flag = False
    active_suspended_flag = False
    all_finished = True
    active_candidates: list[tuple[dict[str, Any], str | None, str | None]] = []

    for game in today_games:
        game_id = game["game_id"]

        # Resolve Naver schedule status to lifecycle state
        away_nav = relay_crawler._naver_team_code(game["away_team_code"])
        home_nav = relay_crawler._naver_team_code(game["home_team_code"])
        nav_status_raw = naver_status_map.get((away_nav, home_nav))
        lifecycle_state = derive_lifecycle_from_naver_status(nav_status_raw)

        if lifecycle_state == "cancelled":
            logger.info(f"[SKIP] {game_id} is CANCELLED.")
            continue
        if lifecycle_state == "before":
            logger.info(f"[SKIP] {game_id} has not started yet.")
            all_finished = False
            continue
        if lifecycle_state == "result_pending_stabilization":
            from src.models.game import Game

            terminal_state = None
            with SessionLocal() as session:
                g_row = session.query(Game).filter(Game.game_id == game_id).first()
                if g_row and g_row.game_lifecycle_state in TERMINAL_STATES:
                    terminal_state = g_row.game_lifecycle_state
            if terminal_state:
                logger.info(f"[SKIP] {game_id} is already final in DB (game_lifecycle_state={terminal_state}).")
                continue
            else:
                logger.info(f"[LIVE] Game {game_id} transitioned to RESULT. Crawling final state to finalize...")

        all_finished = False
        active_candidates.append((game, lifecycle_state, nav_status_raw))

    selected_candidates = _select_live_shard(
        active_candidates,
        shard_key=today_str,
        max_items=max_active_games,
    )
    if len(selected_candidates) < len(active_candidates):
        selected_ids = [item[0]["game_id"] for item in selected_candidates]
        logger.info(
            "[LIVE] Sharding active games: "
            f"processing {len(selected_candidates)}/{len(active_candidates)} this cycle "
            f"(max_active_games={max_active_games}, selected={','.join(selected_ids)})"
        )

    # Dynamic request delay scaling based on the number of active games
    num_active_games = len(selected_candidates)
    if num_active_games > 0:
        scale_factor = 1.0 + max(0, num_active_games - 1) * 0.5
        relay_policy = getattr(relay_crawler, "policy", None)
        if relay_policy is not None:
            relay_policy.min_delay *= scale_factor
            relay_policy.max_delay *= scale_factor
            min_delay = relay_policy.min_delay
        else:
            min_delay = 0.0
        logger.info(
            f"[LIVE] Dynamic request delay scaling: factor {scale_factor:.2f}x for {num_active_games} active games "
            f"(min_delay={min_delay:.2f}s)"
        )

    # Process all selected games in parallel
    if selected_candidates:
        tasks = [
            _process_single_live_game(
                game,
                lifecycle_state,
                nav_status_raw,
                relay_crawler,
                detail_crawler,
                today_str,
            )
            for game, lifecycle_state, nav_status_raw in selected_candidates
        ]
        results = await asyncio.gather(*tasks)

        for touched_gid, resolved_lifecycle in results:
            if touched_gid:
                touched_game_ids.add(touched_gid)
            if resolved_lifecycle == "suspended":
                active_suspended_flag = True
            elif resolved_lifecycle == "running":
                active_playing_flag = True

    manifest_path = write_refresh_manifest(
        phase="live",
        target_date=today_str,
        game_ids=touched_game_ids,
        datasets=["game", "game_metadata", "game_inning_scores", "game_events", "game_play_by_play"],
    )

    if not touched_game_ids:
        logger.info(f"[INFO] No live games currently active right now. manifest={manifest_path}")
        return {
            "active": False,
            "active_playing": False,
            "active_suspended": False,
            "all_finished": all_finished,
            "game_ids_playing": [],
        }

    should_sync = sync_to_oci if sync_to_oci is not None else bool(os.getenv("OCI_DB_URL"))
    oci_sync_failures: list[dict[str, str]] = []
    if should_sync:
        oci_url = os.getenv("OCI_DB_URL")
        if oci_url:
            with SessionLocal() as sync_session:
                sync_engine = OCISync(oci_url, sync_session)
                try:
                    for game_id in sorted(touched_game_ids):
                        try:
                            sync_engine.sync_specific_game(game_id)
                            logger.info(f"[SYNC] ✅ Synced {game_id} to OCI.")
                        except Exception as exc:
                            oci_sync_failures.append(
                                {
                                    "game_id": game_id,
                                    "phase": "sync_specific_game",
                                    "error": str(exc),
                                }
                            )
                            logger.exception(
                                "[SYNC] OCI sync failed game_id=%s phase=sync_specific_game",
                                game_id,
                            )
                finally:
                    sync_engine.close()

    if oci_sync_failures:
        failed_ids = [failure["game_id"] for failure in oci_sync_failures]
        logger.warning(
            "Live cycle completed with OCI partial failures phase=sync_specific_game failed=%d game_ids=%s",
            len(oci_sync_failures),
            ",".join(failed_ids),
        )
        logger.info(
            "[SYNC] ⚠️ Live crawl succeeded with OCI partial failures: "
            f"failed={len(oci_sync_failures)} game_ids={','.join(failed_ids)}"
        )

    logger.info(f"[INFO] Live cycle finished. updated={len(touched_game_ids)} manifest={manifest_path}")
    return {
        "active": True,
        "active_playing": active_playing_flag,
        "active_suspended": active_suspended_flag,
        "all_finished": all_finished,
        "game_ids_playing": list(touched_game_ids),
        "oci_sync_failure_count": len(oci_sync_failures),
        "oci_sync_failed_game_ids": [failure["game_id"] for failure in oci_sync_failures],
    }


async def main_loop(base_interval_minutes: int, *, sync_to_oci: bool | None = None, dynamic: bool = False) -> None:
    last_active_time = None
    last_event_counts: dict[str, int] = {}
    while True:
        try:
            seoul_tz = ZoneInfo("Asia/Seoul")
            now = datetime.now(seoul_tz)

            # 1. Run the cycle
            cycle_result = await run_live_crawler_cycle(sync_to_oci=sync_to_oci)
            active = cycle_result["active"]
            active_playing = cycle_result["active_playing"]
            active_suspended = cycle_result["active_suspended"]
            game_ids_playing: list[str] = cycle_result.get("game_ids_playing", [])

            if active:
                last_active_time = now

            # 2. Determine next sleep interval
            if not dynamic:
                sleep_seconds = base_interval_minutes * 60 if active else 300
                mode_str = "FIXED"
                extra_note = ""
            else:
                base_interval, mode_str = _compute_base_dynamic_interval(
                    active,
                    active_playing,
                    active_suspended,
                    last_active_time,
                    now,
                    base_interval_minutes,
                )

                # Phase 4: Enriched interval using at-bat/pitch/inning state
                if game_ids_playing:
                    enriched_state = _query_enriched_game_state(game_ids_playing)
                    sleep_seconds, extra_note, last_event_counts = _compute_enriched_interval(
                        base_interval,
                        game_ids_playing,
                        last_event_counts,
                        enriched_state,
                    )
                else:
                    sleep_seconds = base_interval
                    extra_note = ""

            log_parts = [f"[WAIT] Next check in {sleep_seconds}s"]
            if mode_str:
                log_parts.append(f"[{mode_str}]")
            if extra_note:
                log_parts.append(extra_note)
            logger.info(" ".join(log_parts))
            await asyncio.sleep(sleep_seconds)

        except Exception:
            logger.exception("[CRITICAL ERROR] Live loop crashed")
            await asyncio.sleep(60)


def _compute_base_dynamic_interval(
    active: bool,
    active_playing: bool,
    active_suspended: bool,
    last_active_time: datetime | None,
    now: datetime,
    base_interval_minutes: int,
) -> tuple[int, str]:
    """Return (base_sleep_seconds, mode_label) for the existing dynamic logic."""
    if active:
        if active_playing:
            return 10, "ACTIVE (Inning playing)"
        elif active_suspended:
            return 60, "DELAYED (Rain delay/Stoppage)"
        else:
            return 30, "CHANGE (Inning change)"
    else:
        recently_active = False
        if last_active_time is not None:
            elapsed = (now - last_active_time).total_seconds()
            if elapsed < 600:
                recently_active = True
        if recently_active:
            return 60, "COOLDOWN (Recently finished)"
        elif 12 <= now.hour < 23:
            return 120, "GAME HOURS (No active games)"
        else:
            return 1800, "OFF HOURS"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KBO Live Score & PBP Daemon")
    parser.add_argument(
        "--interval", type=int, default=2, help="Crawling polling interval in minutes (default for fixed mode)"
    )
    parser.add_argument(
        "--dynamic",
        action="store_true",
        help="Enable enriched dynamic polling (10s playing, 60s delayed, 120s game-hours, 30m off-hours)",
    )
    parser.add_argument("--run-once", action="store_true", help="Run precisely one cycle and exit")
    parser.add_argument("--no-sync", action="store_true", help="Skip explicit OCI sync after local writes")
    args = parser.parse_args(argv)

    if args.run_once:
        asyncio.run(run_live_crawler_cycle(sync_to_oci=not args.no_sync))
    else:
        mode = "DYNAMIC" if args.dynamic else f"FIXED ({args.interval}m)"
        logger.info(f"🚀 Starting Real-time Daemon... Mode: {mode}")
        asyncio.run(main_loop(args.interval, sync_to_oci=not args.no_sync, dynamic=args.dynamic))


if __name__ == "__main__":
    main()
