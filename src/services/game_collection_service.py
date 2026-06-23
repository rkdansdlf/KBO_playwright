"""Shared helpers for game detail and relay collection workflows."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Protocol

from sqlalchemy.exc import SQLAlchemyError

from src.constants import DATE_STR_LEN
from src.db.engine import SessionLocal
from src.models.game import Game, GameBattingStat, GameEvent, GamePitchingStat, GamePlayByPlay
from src.repositories.game_repository import save_game_detail, save_relay_data
from src.services.game_write_contract import GameWriteContract, GameWriteSource
from src.services.pbp_sh_sf_derivation import apply_sh_sf_to_batting_stats
from src.utils.team_codes import normalize_kbo_game_id

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

DETAIL_COLLECTION_FAILURE_REASONS_RETRYABLE = {
    "no_detail_payload",
    "incomplete_detail",
    "navigation_error",
    "timeout",
    "exception",
    "missing",
}
DETAIL_COLLECTION_FAILURE_REASONS_NON_RETRYABLE = {
    "filtered",
    "save_failed",
    "detail_payload_filtered",
    "detail_save_failed",
    "cancelled",
}


class DetailCrawler(Protocol):
    async def crawl_games(
        self,
        games: list[dict[str, str]],
        concurrency: int | None = None,
        *,
        lightweight: bool = False,
    ) -> list[dict[str, Any]]: ...

    async def close(self) -> None: ...


class RelayCrawler(Protocol):
    async def crawl_game_events(self, game_id: str) -> dict[str, Any] | None: ...

    async def close(self) -> None: ...


@dataclass(frozen=True)
class GameCollectionTarget:
    game_id: str
    game_date: str

    def as_crawler_input(self) -> dict[str, str]:
        return {"game_id": self.game_id, "game_date": self.game_date}


@dataclass(frozen=True)
class ExistingGameData:
    has_detail: bool = False
    has_relay: bool = False


@dataclass
class GameCollectionResult:
    total_targets: int = 0
    detail_targets: int = 0
    detail_saved: int = 0
    detail_failed: int = 0
    detail_skipped_existing: int = 0
    relay_targets: int = 0
    relay_saved_games: int = 0
    relay_rows_saved: int = 0
    relay_missing: int = 0
    relay_skipped_existing: int = 0
    processed_game_ids: list[str] = field(default_factory=list)
    items: dict[str, GameCollectionItemResult] = field(default_factory=dict)


@dataclass
class GameCollectionItemResult:
    game_id: str
    game_date: str
    detail_status: str = "pending"
    relay_status: str = "not_requested"
    detail_saved: bool = False
    relay_rows_saved: int = 0
    failure_reason: str | None = None


def build_game_id_range(year: int, month: int | None) -> tuple[str, str]:
    if month:
        start = date(year, month, 1)
        end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    else:
        start = date(year, 1, 1)
        end = date(year + 1, 1, 1)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def load_game_targets_from_db(year: int, month: int | None = None) -> list[GameCollectionTarget]:
    start_id, end_id = build_game_id_range(year, month)
    with SessionLocal() as session:
        rows = (
            session.query(Game.game_id, Game.game_date)
            .filter(Game.game_id >= start_id, Game.game_id < end_id)
            .order_by(Game.game_id.asc())
            .all()
        )
    return [
        GameCollectionTarget(
            game_id=normalize_kbo_game_id(game_id),
            game_date=_format_game_date(game_date, fallback_game_id=game_id),
        )
        for game_id, game_date in rows
    ]


def load_game_targets_by_ids(game_ids: list[str]) -> list[GameCollectionTarget]:
    """game_id 목록으로 GameCollectionTarget 리스트를 조회합니다."""
    with SessionLocal() as session:
        rows = (
            session.query(Game.game_id, Game.game_date)
            .filter(Game.game_id.in_(game_ids))
            .order_by(Game.game_id.asc())
            .all()
        )
    return [
        GameCollectionTarget(
            game_id=normalize_kbo_game_id(game_id),
            game_date=_format_game_date(game_date, fallback_game_id=game_id),
        )
        for game_id, game_date in rows
    ]


def normalize_game_targets(games: Iterable[Any]) -> list[GameCollectionTarget]:
    targets: list[GameCollectionTarget] = []
    seen: set[str] = set()
    for game in games:
        game_id = _get_value(game, "game_id")
        if not game_id:
            continue
        normalized_id = normalize_kbo_game_id(game_id)
        if not normalized_id or normalized_id in seen:
            continue
        game_date = _format_game_date(_get_value(game, "game_date"), fallback_game_id=normalized_id)
        targets.append(GameCollectionTarget(game_id=normalized_id, game_date=game_date))
        seen.add(normalized_id)
    return targets


def inspect_existing_game_data(targets: Iterable[GameCollectionTarget]) -> dict[str, ExistingGameData]:
    target_list = list(targets)
    game_ids = [target.game_id for target in target_list]
    if not game_ids:
        return {}

    with SessionLocal() as session:
        batting_ids = _ids_with_rows(session, GameBattingStat, game_ids)
        pitching_ids = _ids_with_rows(session, GamePitchingStat, game_ids)
        event_ids = _ids_with_rows(session, GameEvent, game_ids)
        pbp_ids = _ids_with_rows(session, GamePlayByPlay, game_ids)

    relay_ids = event_ids | pbp_ids
    return {
        game_id: ExistingGameData(
            has_detail=game_id in batting_ids and game_id in pitching_ids,
            has_relay=game_id in relay_ids,
        )
        for game_id in game_ids
    }


async def crawl_and_save_game_details(
    games: Iterable[Any],
    *,
    detail_crawler: DetailCrawler,
    relay_crawler: RelayCrawler | None = None,
    force: bool = False,
    concurrency: int | None = None,
    relay_requires_detail: bool = True,
    should_save_detail: Callable[[dict[str, Any]], bool] | None = None,
    pause_every: int | None = None,
    pause_seconds: float = 0.0,
    log: Callable[[str], None] = logger.info,
    write_contract: GameWriteContract | None = None,
    source_stage: str = "detail",
    source_crawler: str | None = None,
    source_reason: str = "detail_recovery",
    relay_source_reason: str = "relay_recovery",
) -> GameCollectionResult:
    targets = normalize_game_targets(games)
    result = GameCollectionResult(total_targets=len(targets))
    result.items = {
        target.game_id: GameCollectionItemResult(game_id=target.game_id, game_date=target.game_date)
        for target in targets
    }
    if not targets:
        return result

    contract = write_contract or GameWriteContract(run_label="game_collection", log=log)
    detail_source = GameWriteSource(
        source_stage,
        source_crawler or detail_crawler.__class__.__name__,
        source_reason,
    )
    for target in targets:
        contract.claim_game(target.game_id, detail_source)

    exist_map = inspect_existing_game_data(targets)
    detail_ready = await _collect_detail_phase(
        targets,
        exist_map,
        detail_crawler,
        contract,
        detail_source,
        force=force,
        concurrency=concurrency,
        should_save_detail=should_save_detail,
        pause_every=pause_every,
        pause_seconds=pause_seconds,
        log=log,
        result=result,
    )

    if relay_crawler:
        await _collect_relay_phase(
            targets,
            exist_map,
            detail_ready,
            relay_crawler,
            contract,
            force=force,
            relay_requires_detail=relay_requires_detail,
            relay_source_reason=relay_source_reason,
            pause_every=pause_every,
            pause_seconds=pause_seconds,
            log=log,
            result=result,
        )

    # Derive SH/SF from PBP events for games where batting stats have them as 0
    _derive_sh_sf_for_results(result, log=log)

    if write_contract is None:
        log(contract.summary())

    return result


async def _collect_detail_phase(
    targets: list[GameCollectionTarget],
    exist_map: dict[str, ExistingGameData],
    detail_crawler: DetailCrawler,
    contract: GameWriteContract,
    detail_source: GameWriteSource,
    *,
    force: bool,
    concurrency: int | None,
    should_save_detail: Callable[[dict[str, Any]], bool] | None,
    pause_every: int | None,
    pause_seconds: float,
    log: Callable[[str], None],
    result: GameCollectionResult,
) -> set[str]:
    detail_ready: set[str] = {
        target.game_id for target in targets if exist_map.get(target.game_id, ExistingGameData()).has_detail
    }
    detail_targets = [
        target for target in targets if force or not exist_map.get(target.game_id, ExistingGameData()).has_detail
    ]
    result.detail_targets = len(detail_targets)
    result.detail_skipped_existing = len(targets) - len(detail_targets)

    _mark_skipped_detail_targets(targets, exist_map, force=force, result=result, log=log)

    if not detail_targets:
        return detail_ready

    batch_size = pause_every or 20
    total_batches = (len(detail_targets) + batch_size - 1) // batch_size
    for batch_num, b_idx in enumerate(range(0, len(detail_targets), batch_size), start=1):
        batch = detail_targets[b_idx : b_idx + batch_size]
        await _pause_between_detail_batches(b_idx, pause_seconds, detail_crawler, log)
        log(f"[*] Processing detail batch {batch_num}/{total_batches} ({len(batch)} games)...")

        payloads = await detail_crawler.crawl_games(
            [target.as_crawler_input() for target in batch],
            concurrency=concurrency,
        )
        payload_by_id = {
            normalize_kbo_game_id(payload.get("game_id")): payload for payload in payloads if payload.get("game_id")
        }

        for index, target in enumerate(batch, start=1):
            global_index = b_idx + index
            _process_detail_target(
                target,
                payload_by_id.get(target.game_id),
                detail_crawler,
                contract,
                detail_source,
                should_save_detail,
                result,
                detail_ready,
                global_index=global_index,
                total_targets=len(detail_targets),
                log=log,
            )

    return detail_ready


def _mark_skipped_detail_targets(
    targets: list[GameCollectionTarget],
    exist_map: dict[str, ExistingGameData],
    *,
    force: bool,
    result: GameCollectionResult,
    log: Callable[[str], None],
) -> None:
    if not result.detail_skipped_existing:
        return
    log(f"[SKIP] Detail already exists for {result.detail_skipped_existing} game(s). Use --force to recrawl.")
    for target in targets:
        if exist_map.get(target.game_id, ExistingGameData()).has_detail and not force:
            result.items[target.game_id].detail_status = "skipped_existing"


async def _pause_between_detail_batches(
    batch_start_index: int,
    pause_seconds: float,
    detail_crawler: DetailCrawler,
    log: Callable[[str], None],
) -> None:
    if batch_start_index <= 0:
        return
    if pause_seconds > 0:
        log(f"   [PAUSE] Sleeping for {pause_seconds}s between batches...")
        await asyncio.sleep(pause_seconds)
    await detail_crawler.close()


def _process_detail_target(
    target: GameCollectionTarget,
    payload: dict[str, Any] | None,
    detail_crawler: DetailCrawler,
    contract: GameWriteContract,
    detail_source: GameWriteSource,
    should_save_detail: Callable[[dict[str, Any]], bool] | None,
    result: GameCollectionResult,
    detail_ready: set[str],
    *,
    global_index: int,
    total_targets: int,
    log: Callable[[str], None],
) -> None:
    log(f"[DETAIL] {global_index}/{total_targets} {target.game_id}")
    failure_reason = _detail_payload_failure_reason(target, payload, detail_crawler, should_save_detail)
    if failure_reason is not None:
        _mark_detail_failed(target, failure_reason, result, log)
        return
    if _save_detail_payload(target, payload or {}, contract, detail_source, result, detail_ready):
        log("   [DB] Detail saved")
    else:
        log("   [ERROR] Detail save failed")


def _detail_payload_failure_reason(
    target: GameCollectionTarget,
    payload: dict[str, Any] | None,
    detail_crawler: DetailCrawler,
    should_save_detail: Callable[[dict[str, Any]], bool] | None,
) -> tuple[str, str, str] | None:
    if not payload:
        return "crawl_failed", _get_failure_reason(detail_crawler, target.game_id), "no_detail_payload"
    if not _has_required_detail_rows(payload):
        return "filtered", _get_failure_reason(detail_crawler, target.game_id), "incomplete_detail"
    if should_save_detail and not should_save_detail(payload):
        return "filtered", "detail_payload_filtered", "filtered"
    return None


def _mark_detail_failed(
    target: GameCollectionTarget,
    failure: tuple[str, str | None, str],
    result: GameCollectionResult,
    log: Callable[[str], None],
) -> None:
    status, reason, default = failure
    result.detail_failed += 1
    item = result.items[target.game_id]
    item.detail_status = status
    item.failure_reason = _normalize_detail_failure_reason(reason, default=default)
    if default == "no_detail_payload":
        log("   [WARN] No detail payload returned")
    elif default == "incomplete_detail":
        log("   [WARN] Detail payload is missing required hitter/pitcher rows")
    else:
        log("   [WARN] Detail payload did not pass save predicate")


def _save_detail_payload(
    target: GameCollectionTarget,
    payload: dict[str, Any],
    contract: GameWriteContract,
    detail_source: GameWriteSource,
    result: GameCollectionResult,
    detail_ready: set[str],
) -> bool:
    if not save_game_detail(
        payload,
        write_contract=contract,
        source_stage=detail_source.stage,
        source_crawler=detail_source.crawler,
        source_reason=detail_source.reason,
    ):
        result.detail_failed += 1
        item = result.items[target.game_id]
        item.detail_status = "save_failed"
        item.failure_reason = _normalize_detail_failure_reason("detail_save_failed", default="save_failed")
        return False
    result.detail_saved += 1
    result.processed_game_ids.append(target.game_id)
    detail_ready.add(target.game_id)
    item = result.items[target.game_id]
    item.detail_status = "saved"
    item.detail_saved = True
    return True


async def _collect_relay_phase(
    targets: list[GameCollectionTarget],
    exist_map: dict[str, ExistingGameData],
    detail_ready: set[str],
    relay_crawler: RelayCrawler,
    contract: GameWriteContract,
    *,
    force: bool,
    relay_requires_detail: bool,
    relay_source_reason: str,
    pause_every: int | None,
    pause_seconds: float,
    log: Callable[[str], None],
    result: GameCollectionResult,
) -> None:
    relay_source = GameWriteSource("relay", relay_crawler.__class__.__name__, relay_source_reason)
    relay_targets = [
        target
        for target in targets
        if (force or not exist_map.get(target.game_id, ExistingGameData()).has_relay)
        and (not relay_requires_detail or target.game_id in detail_ready)
    ]
    result.relay_targets = len(relay_targets)
    result.relay_skipped_existing = len(targets) - len(relay_targets)
    if result.relay_skipped_existing:
        log(f"[SKIP] Relay already exists for {result.relay_skipped_existing} game(s). Use --force to recrawl.")
        for target in targets:
            item = result.items[target.game_id]
            has_relay = exist_map.get(target.game_id, ExistingGameData()).has_relay
            if has_relay and not force:
                item.relay_status = "skipped_existing"
            elif relay_requires_detail and target.game_id not in detail_ready:
                item.relay_status = "skipped_no_detail"

    for index, target in enumerate(relay_targets, start=1):
        contract.claim_game(target.game_id, relay_source)
        log(f"[RELAY] {index}/{len(relay_targets)} {target.game_id}")
        relay_data = await relay_crawler.crawl_game_events(target.game_id)
        item = result.items[target.game_id]
        flat_events = list((relay_data or {}).get("events") or [])
        raw_pbp_rows = list((relay_data or {}).get("raw_pbp_rows") or [])
        if flat_events or raw_pbp_rows:
            saved_rows = save_relay_data(
                target.game_id,
                flat_events,
                raw_pbp_rows=raw_pbp_rows,
                write_contract=contract,
                source_stage=relay_source.stage,
                source_crawler=relay_source.crawler,
                source_reason=relay_source.reason,
                parser_version=(relay_data or {}).get("parser_version"),
                source_schema_version=(relay_data or {}).get("source_schema_version"),
                payload_hash=(relay_data or {}).get("payload_hash"),
            )
            result.relay_rows_saved += saved_rows
            item.relay_rows_saved = saved_rows
            if saved_rows:
                result.relay_saved_games += 1
                item.relay_status = "saved"
                if target.game_id not in result.processed_game_ids:
                    result.processed_game_ids.append(target.game_id)
                log(f"   [DB] Relay saved ({saved_rows} rows)")
            else:
                result.relay_missing += 1
                item.relay_status = "save_failed"
                item.failure_reason = item.failure_reason or "relay_save_returned_zero"
                log("   [WARN] Relay save returned 0 rows")
        else:
            result.relay_missing += 1
            item.relay_status = "missing"
            item.failure_reason = (
                item.failure_reason or _get_failure_reason(relay_crawler, target.game_id) or "no_relay_payload"
            )
            log("   [INFO] No relay data available")
        await _maybe_pause(index, pause_every, pause_seconds, log)


def _get_value(obj: object, key: str) -> object | None:
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _format_game_date(value: object, *, fallback_game_id: str) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    text = str(value or "").replace("-", "").strip()
    if len(text) == DATE_STR_LEN and text.isdigit():
        return text
    return str(fallback_game_id)[:8]


def _ids_with_rows(session: Session, model: type[Any], game_ids: list[str]) -> set[str]:
    return {row[0] for row in session.query(model.game_id).filter(model.game_id.in_(game_ids)).distinct().all()}


def _get_failure_reason(crawler: object, game_id: str) -> str | None:
    getter = getattr(crawler, "get_last_failure_reason", None)
    if not callable(getter):
        return None
    try:
        return getter(game_id)
    except (RuntimeError, TypeError, ValueError) as exc:
        logger.warning("Failed to get last failure reason from crawler: %s", exc)
        return None


def _normalize_detail_failure_reason(raw_reason: str | None, *, default: str) -> str:
    reason = (raw_reason or "").strip().lower()
    if not reason:
        return default
    if reason in DETAIL_COLLECTION_FAILURE_REASONS_NON_RETRYABLE:
        if reason in {"filtered", "detail_payload_filtered"}:
            return "filtered"
        if reason in {"save_failed", "detail_save_failed"}:
            return "save_failed"
        return reason
    if reason in DETAIL_COLLECTION_FAILURE_REASONS_RETRYABLE:
        return reason
    if reason in {"incomplete_detail", "no_detail_payload", "timeout", "navigation_error", "exception", "missing"}:
        return reason
    return default


def _has_required_detail_rows(payload: dict[str, Any]) -> bool:
    hitters = payload.get("hitters") or {}
    pitchers = payload.get("pitchers") or {}
    has_full_box = (
        bool(hitters.get("away"))
        and bool(hitters.get("home"))
        and bool(pitchers.get("away"))
        and bool(pitchers.get("home"))
    )
    if has_full_box:
        return True

    # Partial recovery check: must have at least team codes and SOME score or metadata info
    teams = payload.get("teams") or {}
    away = teams.get("away") or {}
    home = teams.get("home") or {}
    metadata = payload.get("metadata") or {}

    has_teams = bool(away.get("code")) and bool(home.get("code"))
    has_scores = (
        bool(away.get("line_score"))
        or bool(home.get("line_score"))
        or away.get("score") is not None
        or home.get("score") is not None
    )
    has_metadata = bool(metadata.get("stadium")) or bool(metadata.get("attendance"))

    return has_teams and (has_scores or has_metadata)


async def _maybe_pause(
    index: int,
    pause_every: int | None,
    pause_seconds: float,
    log: Callable[[str], None],
) -> None:
    if not pause_every or pause_every <= 0 or pause_seconds <= 0:
        return
    if index % pause_every == 0:
        log(f"[PAUSE] Sleeping for {pause_seconds:g}s before continuing...")
        await asyncio.sleep(pause_seconds)


def _derive_sh_sf_for_results(result: GameCollectionResult, log: Callable[[str], None]) -> None:
    """Derive sacrifice_hits/sacrifice_flies from PBP events for collected games."""
    updated_total = 0
    game_ids = [gid for gid, item in result.items.items() if item.detail_status == "success"]
    if not game_ids:
        return
    with SessionLocal() as session:
        for game_id in game_ids:
            try:
                updated = apply_sh_sf_to_batting_stats(session, game_id)
                if updated:
                    updated_total += updated
            except (SQLAlchemyError, RuntimeError, ValueError, TypeError):
                logger.exception("SH/SF derivation failed for %s", game_id)
        if updated_total:
            session.commit()
            log(f"[SH/SF] Derived {updated_total} SH/SF values from PBP events.")
        else:
            log("[SH/SF] No SH/SF values needed derivation.")
