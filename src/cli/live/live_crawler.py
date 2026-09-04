"""Real-time KBO live crawler.

Polls today's schedule, captures relay events plus a lightweight scoreboard snapshot,
and persists the changed games to the configured database.

"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from http import HTTPStatus
from threading import Lock, Thread
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import httpx
from playwright.async_api import Error as PlaywrightError
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)
LATE_GAME_START_INNING = 7
RECENT_ACTIVITY_WINDOW_SECONDS = 600
GAME_HOUR_START = 12
GAME_HOUR_END = 23

DB_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)
LIVE_CRAWLER_EXCEPTIONS = (
    PlaywrightError,
    TimeoutError,
    asyncio.TimeoutError,
    httpx.HTTPError,
    SQLAlchemyError,
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
)


def _raise_empty_kbo_pbp() -> None:
    msg = "KBO PBP crawl returned no events"
    raise ValueError(msg)


THREAD_EXCEPTIONS = (RuntimeError, OSError)


@dataclass(frozen=True, slots=True)
class LiveSaveOptions:
    """LiveSaveOptions class."""

    detail_crawler: GameDetailCrawler | None
    detail_snapshot_background: bool


@dataclass(frozen=True, slots=True)
class GameActivityState:
    """GameActivityState class."""

    active: bool
    active_playing: bool
    active_suspended: bool
    active_cleaning: bool = False
    last_active_time: datetime | None = None
    now: datetime = field(default_factory=datetime.now)


_LIVE_SHARD_CURSOR_BY_DATE: dict[str, int] = {}
_ACTIVE_DETAIL_SNAPSHOT_GAMES: set[str] = set()
_ACTIVE_DETAIL_SNAPSHOT_LOCK = Lock()

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.naver_relay_crawler import NaverRelayCrawler
from src.crawlers.schedule_crawler import ScheduleCrawler
from src.db.engine import SessionLocal
from src.repositories.game_repository import save_game_snapshot, save_relay_data
from src.utils.compliance import compliance
from src.utils.game_state import (
    TERMINAL_STATES,
    derive_lifecycle_from_naver_status,
)
from src.utils.game_status import GAME_STATUS_LIVE
from src.utils.refresh_manifest import write_refresh_manifest

if TYPE_CHECKING:
    from collections.abc import Sequence


def _has_ending_header(raw_pbp_rows: list[dict[str, Any]]) -> bool:
    """Check if the last inning_header contains game-ending keywords.

    Args:
        raw_pbp_rows: Raw Pbp Rows.

    """
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

    Return {game_id: {"event_count": int, "max_inning": int}}.

    Args:
        game_ids: Game Ids.

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
    except DB_EXCEPTIONS:
        logger.exception("[WARN] Failed to query enriched game state")
        return {}
    else:
        return state


def _compute_enriched_interval(
    base_interval: int,
    game_ids_playing: list[str],
    last_event_counts: dict[str, int],
    enriched_state: dict[str, dict[str, int]] | None = None,
) -> tuple[int, str, dict[str, int]]:
    """Improve the base dynamic interval using at-bat/inning/event-density awareness.

    Return (sleep_seconds, extra_note, updated_last_event_counts).

    Args:
        base_interval: Base Interval.
        game_ids_playing: Game Ids Playing.
        last_event_counts: Last Event Counts.
        enriched_state: Enriched State.

    """
    min_polling_interval = 5

    if not game_ids_playing or not enriched_state:
        return max(min_polling_interval, base_interval), "", dict(last_event_counts)

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
        if gs["max_inning"] >= LATE_GAME_START_INNING:
            multipliers.append(0.7)

    if not multipliers:
        return max(min_polling_interval, base_interval), "", updated_counts

    # Apply the most aggressive (lowest) multiplier among active games
    combined = min(multipliers)
    final = max(min_polling_interval, min(120, round(base_interval * combined)))

    note_parts = []
    if any(m < 1.0 for m in multipliers):
        note_parts.append("accelerated")
    if any(m > 1.0 for m in multipliers):
        note_parts.append("idle_backoff")
    extra_note = f"(enriched:{','.join(note_parts)} base={base_interval}s→{final}s)" if note_parts else ""

    return final, extra_note, updated_counts


_ACTIVE_HEALING_GAMES: set[str] = set()


def _submit_live_detail_snapshot_background(game_id: str, today_str: str) -> bool:
    """Queue a non-cadence-critical live detail snapshot crawl for one game.

    Args:
        game_id: Game ID.
        today_str: Today Str.

    """
    with _ACTIVE_DETAIL_SNAPSHOT_LOCK:
        if game_id in _ACTIVE_DETAIL_SNAPSHOT_GAMES:
            logger.info("[LIVE] Skipping background live detail snapshot for %s because it is already running", game_id)
            return False
        _ACTIVE_DETAIL_SNAPSHOT_GAMES.add(game_id)

    def _worker() -> None:
        started_at = time.monotonic()

        async def _crawl_and_save() -> None:
            detail_crawler = GameDetailCrawler(request_delay=0.1)
            detail = await detail_crawler.crawl_game(game_id, today_str, lightweight=True)
            elapsed = time.monotonic() - started_at
            if detail and save_game_snapshot(detail, status=GAME_STATUS_LIVE):
                manifest_path = write_refresh_manifest(
                    phase="live_detail",
                    target_date=today_str,
                    game_ids=[game_id],
                    datasets=["game", "game_metadata", "game_inning_scores"],
                )
                logger.info(
                    "[LIVE] Background live detail snapshot saved for %s elapsed=%.1fs manifest=%s",
                    game_id,
                    elapsed,
                    manifest_path,
                )
            else:
                logger.warning(
                    "[LIVE] Background live detail snapshot saved no rows for %s elapsed=%.1fs",
                    game_id,
                    elapsed,
                )

        try:
            logger.info("[LIVE] Starting background live detail snapshot for %s", game_id)
            asyncio.run(_crawl_and_save())
        except LIVE_CRAWLER_EXCEPTIONS:
            logger.exception(
                "[LIVE] Background live detail snapshot failed for %s elapsed=%.1fs",
                game_id,
                time.monotonic() - started_at,
            )
        finally:
            with _ACTIVE_DETAIL_SNAPSHOT_LOCK:
                _ACTIVE_DETAIL_SNAPSHOT_GAMES.discard(game_id)

    thread = Thread(target=_worker, name=f"live-detail-snapshot-{game_id}", daemon=True)
    try:
        logger.info("[LIVE] Queued background live detail snapshot for %s", game_id)
        thread.start()
    except THREAD_EXCEPTIONS:
        with _ACTIVE_DETAIL_SNAPSHOT_LOCK:
            _ACTIVE_DETAIL_SNAPSHOT_GAMES.discard(game_id)
        logger.exception("[LIVE] Failed to start background live detail snapshot for %s", game_id)
        return False

    return True


async def _run_kbo_fallback_healing(game_id: str) -> None:
    """Run KBO official website re-crawl as a background fallback task when validation fails.

    Args:
        game_id: Game ID.

    """
    try:
        from src.utils.kbo_relay_target import resolve_kbo_relay_target

        target = resolve_kbo_relay_target(game_id, season_type="regular")
        kbo_url = target.to_url()
        if not await compliance.is_allowed(kbo_url):
            logger.info("[COMPLIANCE] KBO PBP fallback skipped for %s", game_id)
            return

        from src.crawlers.pbp_crawler import PBPCrawler
        from src.utils.alerting import SlackWebhookClient

        logger.info(
            "[FALLBACK TRIGGER] PBP for %s is unverified. Triggering KBO website re-crawl in background...",
            game_id,
        )
        kbo_crawler = PBPCrawler()
        kbo_data = None
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info("[FALLBACK TRIGGER] Attempt %s to crawl KBO PBP for %s...", attempt, game_id)
                kbo_data = await kbo_crawler.crawl_game_events(game_id)
                if kbo_data and kbo_data.get("events"):
                    break
                _raise_empty_kbo_pbp()
            except (PlaywrightError, TimeoutError, RuntimeError, ValueError) as fallback_err:
                logger.warning(
                    "KBO fallback attempt %s failed for %s: %s",
                    attempt,
                    game_id,
                    fallback_err,
                    exc_info=True,
                )
                if attempt == max_attempts:
                    logger.exception(
                        "[FALLBACK ERROR] KBO fallback failed all %s attempts for %s",
                        max_attempts,
                        game_id,
                    )
                    break
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
                    msg = (
                        f"✅ KBO Fallback Success: Recovered {saved} unverified Naver PBP events "
                        f"from KBO for game {game_id}"
                    )
                    logger.info("[FALLBACK SUCCESS] %s", msg)
                    SlackWebhookClient.send_alert(msg)
            except LIVE_CRAWLER_EXCEPTIONS:
                logger.exception("Failed to save KBO fallback data for %s", game_id)
    except LIVE_CRAWLER_EXCEPTIONS:
        logger.exception("Unexpected exception in background KBO healing for %s", game_id)
    finally:
        _ACTIVE_HEALING_GAMES.discard(game_id)


async def _fetch_naver_live_statuses(relay_crawler: NaverRelayCrawler) -> dict[tuple[str, str], str]:
    seoul_tz = ZoneInfo("Asia/Seoul")
    today_str = datetime.now(seoul_tz).strftime("%Y-%m-%d")
    try:
        async with httpx.AsyncClient() as client:
            query = relay_crawler._schedule_query_context(query_date=today_str)
            response = await client.get(
                relay_crawler.schedule_api_base_url,
                params=query,
                headers=relay_crawler.headers,
                timeout=10.0,
            )
            if response.status_code == HTTPStatus.OK:
                payload = response.json()
                naver_games = list((payload.get("result") or {}).get("games") or [])
                status_map = {}
                for ng in naver_games:
                    away = ng.get("awayTeamCode")
                    home = ng.get("homeTeamCode")
                    if not away or not home:
                        continue
                    status = "CANCELLED" if ng.get("cancel") else (ng.get("statusCode") or ng.get("status"))
                    if status:
                        status_map[(away, home)] = status
                return status_map
    except (httpx.HTTPError, ValueError, TypeError):
        logger.exception("[WARN] Failed to fetch Naver live statuses")
    return {}


def _lookup_naver_status(
    game: dict[str, Any],
    relay_crawler: object,
    naver_status_map: dict[tuple[str, str], str],
) -> str | None:
    """Look up raw Naver status for a game with alias/legacy fallback."""
    away_code = game.get("away_team_code", "")
    home_code = game.get("home_team_code", "")
    away_nav = relay_crawler._naver_team_code(away_code) if hasattr(relay_crawler, "_naver_team_code") else away_code
    home_nav = relay_crawler._naver_team_code(home_code) if hasattr(relay_crawler, "_naver_team_code") else home_code

    if (away_nav, home_nav) in naver_status_map:
        return naver_status_map[(away_nav, home_nav)]
    if (away_code, home_code) in naver_status_map:
        return naver_status_map[(away_code, home_code)]

    legacy_aliases = {
        "DO": ["OB", "DO", "DB"],
        "DB": ["OB", "DO", "DB"],
        "OB": ["DO", "DB", "OB"],
        "KIA": ["HT", "KIA"],
        "HT": ["KIA", "HT"],
        "SSG": ["SK", "SSG"],
        "SK": ["SSG", "SK"],
        "WO": ["HD", "NX", "WO", "KI"],
        "HD": ["WO", "NX", "HD", "KI"],
        "NX": ["WO", "HD", "NX", "KI"],
        "KI": ["WO", "HD", "NX", "KI"],
    }
    away_candidates = [away_nav, away_code, *legacy_aliases.get(away_nav, []), *legacy_aliases.get(away_code, [])]
    home_candidates = [home_nav, home_code, *legacy_aliases.get(home_nav, []), *legacy_aliases.get(home_code, [])]

    for a in away_candidates:
        for h in home_candidates:
            if (a, h) in naver_status_map:
                return naver_status_map[(a, h)]

    return None


def _evaluate_game_lifecycles(
    today_games: list[dict[str, Any]],
    relay_crawler: NaverRelayCrawler,
    naver_status_map: dict[tuple[str, str], str],
) -> tuple[list[tuple[dict[str, Any], str | None, str | None]], bool]:
    active_candidates: list[tuple[dict[str, Any], str | None, str | None]] = []
    all_finished = True
    for game in today_games:
        game_id = game["game_id"]
        nav_status_raw = _lookup_naver_status(game, relay_crawler, naver_status_map)
        lifecycle_state = derive_lifecycle_from_naver_status(nav_status_raw)

        if lifecycle_state == "cancelled":
            logger.info("[SKIP] %s is CANCELLED.", game_id)
            continue
        if lifecycle_state == "before":
            logger.info("[SKIP] %s has not started yet.", game_id)
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
                logger.info("[SKIP] %s is already final in DB (game_lifecycle_state=%s).", game_id, terminal_state)
                continue
            logger.info("[LIVE] Game %s transitioned to RESULT. Crawling final state to finalize...", game_id)

        all_finished = False
        active_candidates.append((game, lifecycle_state, nav_status_raw))
    return active_candidates, all_finished


def _apply_dynamic_delay_scaling(
    relay_crawler: NaverRelayCrawler,
    selected_candidates: list[tuple[dict[str, Any], str | None, str | None]],
) -> None:
    num_active_games = len(selected_candidates)
    if num_active_games == 0:
        return
    scale_factor = 1.0 + max(0, num_active_games - 1) * 0.5
    relay_policy = getattr(relay_crawler, "policy", None)
    if relay_policy is not None:
        relay_policy.min_delay *= scale_factor
        relay_policy.max_delay *= scale_factor
        min_delay = relay_policy.min_delay
    else:
        min_delay = 0.0
    logger.info(
        "[LIVE] Dynamic request delay scaling: factor %.2fx for %s active games (min_delay=%.2fs)",
        scale_factor,
        num_active_games,
        min_delay,
    )


def _empty_live_result(*, all_finished: bool) -> dict[str, Any]:
    return {
        "active": False,
        "active_playing": False,
        "active_suspended": False,
        "all_finished": all_finished,
        "game_ids_playing": [],
    }


def _resolve_live_lifecycle(
    lifecycle_state: str | None,
    flat_events: list[dict[str, Any]],
    raw_pbp_rows: list[dict[str, Any]],
) -> str:
    last_desc = ""
    if flat_events:
        last_desc = flat_events[-1].get("description", "")
    elif raw_pbp_rows:
        last_desc = raw_pbp_rows[-1].get("play_description", "")

    detected_suspension = any(term in last_desc for term in ["중단", "지연", "우천", "서스펜디드"])
    detected_game_end = _has_ending_header(raw_pbp_rows) or any(
        term in last_desc for term in ["경기 종료", "게임 종료", "경기종료"]
    )

    if lifecycle_state == "result_pending_stabilization" or detected_game_end:
        return "result_pending_stabilization"
    if lifecycle_state == "suspended" or detected_suspension:
        return "suspended"
    if lifecycle_state == "delayed":
        return "delayed"
    return "running"


@dataclass(frozen=True)
class RelaySaveInput:
    """RelaySaveInput class."""

    game_id: str
    today_str: str
    flat_events: list[dict[str, Any]]
    raw_pbp_rows: list[dict[str, Any]]
    relay_data: dict[str, Any] | None
    resolved_lifecycle: str


async def _save_live_relay_and_snapshot(
    input_: RelaySaveInput,
    *,
    save_options: LiveSaveOptions,
) -> bool:
    if not input_.flat_events and not input_.raw_pbp_rows:
        return False

    touched = False
    saved_rows = save_relay_data(
        input_.game_id,
        input_.flat_events,
        raw_pbp_rows=input_.raw_pbp_rows,
        source_name="naver_live",
        parser_version=(input_.relay_data or {}).get("parser_version"),
        source_schema_version=(input_.relay_data or {}).get("source_schema_version"),
        payload_hash=(input_.relay_data or {}).get("payload_hash"),
        game_lifecycle_state=input_.resolved_lifecycle,
    )
    if saved_rows:
        touched = True
        logger.info("[LIVE] 📝 Synced %s relay rows for %s", saved_rows, input_.game_id)

    if save_options.detail_snapshot_background:
        _submit_live_detail_snapshot_background(input_.game_id, input_.today_str)
    elif save_options.detail_crawler is not None:
        detail = await save_options.detail_crawler.crawl_game(input_.game_id, input_.today_str, lightweight=True)
        if detail:
            lifecycle_override = detail.get("lifecycle_state")
            if lifecycle_override and input_.resolved_lifecycle in ("before", None):
                input_ = RelaySaveInput(
                    game_id=input_.game_id,
                    today_str=input_.today_str,
                    flat_events=input_.flat_events,
                    raw_pbp_rows=input_.raw_pbp_rows,
                    relay_data=input_.relay_data,
                    resolved_lifecycle=lifecycle_override,
                )
            if save_game_snapshot(detail, status=GAME_STATUS_LIVE):
                touched = True
                logger.info("[LIVE] 📊 Updated scoreboard snapshot for %s", input_.game_id)

    if input_.resolved_lifecycle == "result_pending_stabilization":
        _trigger_fallback_healing_if_unverified(input_.game_id)

    return touched


def _trigger_fallback_healing_if_unverified(game_id: str) -> None:
    from src.models.game import GameMetadata

    with SessionLocal() as session:
        meta = session.query(GameMetadata).filter(GameMetadata.game_id == game_id).first()
        val_status = (
            meta.source_payload.get("pbp_validation_status") if meta and isinstance(meta.source_payload, dict) else None
        )
        if val_status == "unverified" and game_id not in _ACTIVE_HEALING_GAMES:
            _ACTIVE_HEALING_GAMES.add(game_id)
            task = asyncio.create_task(_run_kbo_fallback_healing(game_id))
            task.add_done_callback(lambda _: _ACTIVE_HEALING_GAMES.discard(game_id))


@dataclass(frozen=True)
class LiveGameInput:
    """LiveGameInput class."""

    game: dict[str, Any]
    lifecycle_state: str | None
    nav_status_raw: str | None
    relay_crawler: NaverRelayCrawler
    today_str: str


async def _process_single_live_game(
    input_: LiveGameInput,
    *,
    save_options: LiveSaveOptions,
) -> tuple[str | None, str]:
    game_id = input_.game["game_id"]

    logger.info(
        "[LIVE] 🔍 Crawling active game: %s (lifecycle=%s, nav_status=%s)",
        game_id,
        input_.lifecycle_state,
        input_.nav_status_raw or "UNKNOWN",
    )

    relay_data = await input_.relay_crawler.crawl_game_events(game_id)
    flat_events = list((relay_data or {}).get("events") or [])
    raw_pbp_rows = list((relay_data or {}).get("raw_pbp_rows") or [])

    resolved_lifecycle = _resolve_live_lifecycle(input_.lifecycle_state, flat_events, raw_pbp_rows)

    touched = await _save_live_relay_and_snapshot(
        RelaySaveInput(
            game_id=game_id,
            today_str=input_.today_str,
            flat_events=flat_events,
            raw_pbp_rows=raw_pbp_rows,
            relay_data=relay_data,
            resolved_lifecycle=resolved_lifecycle,
        ),
        save_options=save_options,
    )

    return (game_id if touched else None), resolved_lifecycle


_DATE_STR_DIGITS = 8


def _load_today_games_from_db(today_str: str) -> list[dict[str, Any]]:
    """Load today's scheduled games from database when schedule crawler fails."""
    try:
        from src.models.game import Game

        if len(today_str) == _DATE_STR_DIGITS:
            date_formatted = f"{today_str[:4]}-{today_str[4:6]}-{today_str[6:]}"
        else:
            date_formatted = today_str
        with SessionLocal() as session:
            rows = session.query(Game).filter(Game.game_date == date_formatted).all()
            return [
                {
                    "game_id": r.game_id,
                    "game_date": str(r.game_date),
                    "away_team_code": r.away_team,
                    "home_team_code": r.home_team,
                }
                for r in rows
            ]
    except Exception:
        logger.exception("Failed to load today games from DB")
        return []


async def _resolve_today_games(
    sched_crawler: ScheduleCrawler,
    today_str: str,
    now: datetime,
) -> tuple[list[dict[str, Any]], str | None]:
    """Fetch or fallback today's scheduled games."""
    games = await sched_crawler.crawl_schedule(now.year, now.month)
    failure_reason = getattr(sched_crawler, "get_last_failure_reason", lambda _key: None)(
        f"{now.year}-{now.month:02d}:all"
    )
    today_games = [g for g in (games or []) if g.get("game_date", "").replace("-", "") == today_str]

    if not today_games and failure_reason and isinstance(failure_reason, str):
        today_games = _load_today_games_from_db(today_str)
        if not today_games:
            logger.info("[INFO] Schedule failed (%s); no DB games for %s.", failure_reason, today_str)
            return [], failure_reason

    return today_games, None


async def run_live_crawler_cycle(
    *,
    max_active_games: int | None = None,
    detail_snapshot_background: bool = False,
) -> dict[str, Any]:
    """Run one live polling cycle.

        Returns status dictionary.

    Args:
        max_active_games: Max Active Games.
        detail_snapshot_background: Detail Snapshot Background.

    """
    seoul_tz = ZoneInfo("Asia/Seoul")

    now = datetime.now(seoul_tz)
    today_str = now.strftime("%Y%m%d")

    logger.info("\n[%s] 🚨 Live Crawl Cycle Started", now.strftime("%Y-%m-%d %H:%M:%S"))

    sched_crawler = ScheduleCrawler()
    today_games, failure_reason = await _resolve_today_games(sched_crawler, today_str, now)

    if not today_games and failure_reason:
        return {
            "active": False,
            "active_playing": False,
            "active_suspended": False,
            "all_finished": False,
            "schedule_available": False,
            "schedule_failure_reason": failure_reason,
            "game_ids_playing": [],
        }

    if not today_games:
        logger.info("[INFO] No games scheduled for today (%s).", today_str)
        return {
            "active": False,
            "active_playing": False,
            "active_suspended": False,
            "all_finished": True,
            "game_ids_playing": [],
        }

    relay_crawler = NaverRelayCrawler()
    naver_status_map = await _fetch_naver_live_statuses(relay_crawler)

    detail_crawler = None if detail_snapshot_background else GameDetailCrawler(request_delay=0.1)
    touched_game_ids: set[str] = set()
    active_playing_flag = False
    active_suspended_flag = False

    active_candidates, all_finished = _evaluate_game_lifecycles(
        today_games,
        relay_crawler,
        naver_status_map,
    )

    selected_candidates = _select_live_shard(
        active_candidates,
        shard_key=today_str,
        max_items=max_active_games,
    )
    if len(selected_candidates) < len(active_candidates):
        selected_ids = [item[0]["game_id"] for item in selected_candidates]
        logger.info(
            "[LIVE] Sharding active games: processing %s/%s this cycle (max_active_games=%s, selected=%s)",
            len(selected_candidates),
            len(active_candidates),
            max_active_games,
            ",".join(selected_ids),
        )

    _apply_dynamic_delay_scaling(relay_crawler, selected_candidates)

    # Process all selected games in parallel
    if selected_candidates:
        tasks = [
            _process_single_live_game(
                LiveGameInput(
                    game=game,
                    lifecycle_state=lifecycle_state,
                    nav_status_raw=nav_status_raw,
                    relay_crawler=relay_crawler,
                    today_str=today_str,
                ),
                save_options=LiveSaveOptions(
                    detail_crawler=detail_crawler,
                    detail_snapshot_background=detail_snapshot_background,
                ),
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
        datasets=(
            ["game_events", "game_play_by_play"]
            if detail_snapshot_background
            else ["game", "game_metadata", "game_inning_scores", "game_events", "game_play_by_play"]
        ),
    )

    if not touched_game_ids:
        logger.info("[INFO] No live games currently active right now. manifest=%s", manifest_path)
        return _empty_live_result(all_finished=all_finished)

    logger.info("[INFO] Live cycle finished. updated=%s manifest=%s", len(touched_game_ids), manifest_path)
    return {
        "active": True,
        "active_playing": active_playing_flag,
        "active_suspended": active_suspended_flag,
        "all_finished": all_finished,
        "game_ids_playing": list(touched_game_ids),
    }


async def main_loop(base_interval_minutes: int, *, dynamic: bool = False) -> None:
    """Run the main entry point for this CLI command.

    Args:
        base_interval_minutes: Base Interval Minutes.
        dynamic: Dynamic.

    """
    last_active_time = None

    last_event_counts: dict[str, int] = {}
    while True:
        try:
            seoul_tz = ZoneInfo("Asia/Seoul")
            now = datetime.now(seoul_tz)

            # 1. Run the cycle
            cycle_result = await run_live_crawler_cycle()
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
                    state=GameActivityState(
                        active=active,
                        active_playing=active_playing,
                        active_suspended=active_suspended,
                        last_active_time=last_active_time,
                        now=now,
                    ),
                    base_interval_minutes=base_interval_minutes,
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

        except LIVE_CRAWLER_EXCEPTIONS:
            logger.exception("[CRITICAL ERROR] Live loop crashed")
            await asyncio.sleep(60)


def _compute_base_dynamic_interval(  # noqa: PLR0911
    *,
    state: GameActivityState,
    base_interval_minutes: int,
) -> tuple[int, str]:
    """Return (base_sleep_seconds, mode_label) using AdaptivePollingEngine.

    The engine's cadence config mirrors the historical dynamic intervals so the
    decision layer is centralized in AdaptivePollingEngine without behavior drift.

    Args:
        state: State.
        base_interval_minutes: Base Interval Minutes.

    """
    from src.utils.polling_policy import AdaptivePollingEngine, PollingPolicyConfig

    engine = AdaptivePollingEngine(
        PollingPolicyConfig(
            interval_before=120,
            interval_running=10,
            interval_high_leverage=5,
            interval_delayed=60,
            interval_stabilization=60,
            interval_terminal=1800,
        ),
    )

    if state.active:
        if state.active_cleaning:
            return 60, "CLEANING_TIME (5th inning break)"
        if state.active_suspended:
            decision = engine.evaluate("delayed")
            return decision.interval_seconds, "DELAYED (Rain delay/Stoppage)"
        if state.active_playing:
            decision = engine.evaluate("running")
            return decision.interval_seconds, "ACTIVE (Inning playing)"
        return 30, "CHANGE (Inning change)"

    recently_active = False
    if state.last_active_time is not None:
        elapsed = (state.now - state.last_active_time).total_seconds()
        if elapsed < RECENT_ACTIVITY_WINDOW_SECONDS:
            recently_active = True

    if recently_active:
        decision = engine.evaluate("result_pending_stabilization")
        return decision.interval_seconds, "COOLDOWN (Recently finished)"

    if GAME_HOUR_START <= state.now.hour < GAME_HOUR_END:
        decision = engine.evaluate("before")
        return decision.interval_seconds, "GAME HOURS (No active games)"

    decision = engine.evaluate("final")
    return decision.interval_seconds, "OFF HOURS"


def main(argv: Sequence[str] | None = None) -> int:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = argparse.ArgumentParser(description="KBO Live Score & PBP Daemon")

    parser.add_argument(
        "--interval",
        type=int,
        default=2,
        help="Crawling polling interval in minutes (default for fixed mode)",
    )
    parser.add_argument(
        "--dynamic",
        action="store_true",
        help="Enable enriched dynamic polling (10s playing, 60s delayed, 120s game-hours, 30m off-hours)",
    )
    parser.add_argument("--run-once", action="store_true", help="Run precisely one cycle and exit")
    args = parser.parse_args(argv)

    from src.utils.lock import ProcessLock

    lock = ProcessLock("live_refresh", blocking=False)
    if not lock.acquire():
        logger.warning("⚠️ Another instance of live_crawler is already running. Exiting.")
        return 1

    try:
        if args.run_once:
            asyncio.run(run_live_crawler_cycle())
        else:
            mode = "DYNAMIC" if args.dynamic else f"FIXED ({args.interval}m)"
            logger.info("🚀 Starting Real-time Daemon... Mode: %s", mode)
            asyncio.run(main_loop(args.interval, dynamic=args.dynamic))
    finally:
        lock.release()
    return 0


if __name__ == "__main__":
    main()
