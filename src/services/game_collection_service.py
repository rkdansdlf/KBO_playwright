"""Shared helpers for game detail and relay collection workflows."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol

from src.db.engine import SessionLocal
from src.models.game import Game, GameBattingStat, GameEvent, GamePitchingStat, GamePlayByPlay
from src.repositories.game_repository import save_game_detail, save_relay_data
from src.utils.safe_print import safe_print as print
from src.utils.team_codes import normalize_kbo_game_id


class DetailCrawler(Protocol):
    async def crawl_games(
        self,
        games: List[Dict[str, str]],
        concurrency: Optional[int] = None,
        lightweight: bool = False,
    ) -> List[Dict[str, Any]]:
        ...


class RelayCrawler(Protocol):
    async def crawl_game_events(self, game_id: str) -> Optional[Dict[str, Any]]:
        ...


@dataclass(frozen=True)
class GameCollectionTarget:
    game_id: str
    game_date: str

    def as_crawler_input(self) -> Dict[str, str]:
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
    processed_game_ids: List[str] = field(default_factory=list)
    items: Dict[str, "GameCollectionItemResult"] = field(default_factory=dict)


@dataclass
class GameCollectionItemResult:
    game_id: str
    game_date: str
    detail_status: str = "pending"
    relay_status: str = "not_requested"
    detail_saved: bool = False
    relay_rows_saved: int = 0
    failure_reason: Optional[str] = None


def build_game_id_range(year: int, month: Optional[int]) -> tuple[str, str]:
    if month:
        start = date(year, month, 1)
        if month == 12:
            end = date(year + 1, 1, 1)
        else:
            end = date(year, month + 1, 1)
    else:
        start = date(year, 1, 1)
        end = date(year + 1, 1, 1)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def load_game_targets_from_db(year: int, month: Optional[int] = None) -> List[GameCollectionTarget]:
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


def normalize_game_targets(games: Iterable[Any]) -> List[GameCollectionTarget]:
    targets: List[GameCollectionTarget] = []
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


def inspect_existing_game_data(targets: Iterable[GameCollectionTarget]) -> Dict[str, ExistingGameData]:
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
    relay_crawler: Optional[RelayCrawler] = None,
    force: bool = False,
    concurrency: Optional[int] = None,
    relay_requires_detail: bool = True,
    should_save_detail: Optional[Callable[[Dict[str, Any]], bool]] = None,
    pause_every: Optional[int] = None,
    pause_seconds: float = 0.0,
    log: Callable[[str], None] = print,
) -> GameCollectionResult:
    targets = normalize_game_targets(games)
    result = GameCollectionResult(total_targets=len(targets))
    result.items = {
        target.game_id: GameCollectionItemResult(game_id=target.game_id, game_date=target.game_date)
        for target in targets
    }
    if not targets:
        return result

    existing = inspect_existing_game_data(targets)
    detail_ready_game_ids = {
        target.game_id
        for target in targets
        if existing.get(target.game_id, ExistingGameData()).has_detail
    }
    detail_targets = [
        target for target in targets if force or not existing.get(target.game_id, ExistingGameData()).has_detail
    ]
    result.detail_targets = len(detail_targets)
    result.detail_skipped_existing = len(targets) - len(detail_targets)

    if result.detail_skipped_existing:
        log(f"[SKIP] Detail already exists for {result.detail_skipped_existing} game(s). Use --force to recrawl.")
        for target in targets:
            if existing.get(target.game_id, ExistingGameData()).has_detail and not force:
                result.items[target.game_id].detail_status = "skipped_existing"

    if detail_targets:
        payloads = await detail_crawler.crawl_games(
            [target.as_crawler_input() for target in detail_targets],
            concurrency=concurrency,
        )
        payload_by_id = {
            normalize_kbo_game_id(payload.get("game_id")): payload
            for payload in payloads
            if payload.get("game_id")
        }

        for index, target in enumerate(detail_targets, start=1):
            payload = payload_by_id.get(target.game_id)
            log(f"[DETAIL] {index}/{len(detail_targets)} {target.game_id}")
            if not payload:
                result.detail_failed += 1
                item = result.items[target.game_id]
                item.detail_status = "crawl_failed"
                item.failure_reason = _get_failure_reason(detail_crawler, target.game_id) or "no_detail_payload"
                log("   [WARN] No detail payload returned")
                continue
            if should_save_detail and not should_save_detail(payload):
                result.detail_failed += 1
                item = result.items[target.game_id]
                item.detail_status = "filtered"
                item.failure_reason = "detail_payload_filtered"
                log("   [WARN] Detail payload did not pass save predicate")
                continue
            if save_game_detail(payload):
                result.detail_saved += 1
                result.processed_game_ids.append(target.game_id)
                detail_ready_game_ids.add(target.game_id)
                item = result.items[target.game_id]
                item.detail_status = "saved"
                item.detail_saved = True
                log("   [DB] Detail saved")
            else:
                result.detail_failed += 1
                item = result.items[target.game_id]
                item.detail_status = "save_failed"
                item.failure_reason = "detail_save_failed"
                log("   [ERROR] Detail save failed")

    if relay_crawler:
        relay_targets = [
            target
            for target in targets
            if (force or not existing.get(target.game_id, ExistingGameData()).has_relay)
            and (not relay_requires_detail or target.game_id in detail_ready_game_ids)
        ]
        result.relay_targets = len(relay_targets)
        result.relay_skipped_existing = len(targets) - len(relay_targets)
        if result.relay_skipped_existing:
            log(f"[SKIP] Relay already exists for {result.relay_skipped_existing} game(s). Use --force to recrawl.")
            for target in targets:
                item = result.items[target.game_id]
                has_relay = existing.get(target.game_id, ExistingGameData()).has_relay
                if has_relay and not force:
                    item.relay_status = "skipped_existing"
                elif relay_requires_detail and target.game_id not in detail_ready_game_ids:
                    item.relay_status = "skipped_no_detail"

        for index, target in enumerate(relay_targets, start=1):
            log(f"[RELAY] {index}/{len(relay_targets)} {target.game_id}")
            relay_data = await relay_crawler.crawl_game_events(target.game_id)
            item = result.items[target.game_id]
            if relay_data and relay_data.get("events"):
                saved_rows = save_relay_data(target.game_id, relay_data["events"])
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
                item.failure_reason = item.failure_reason or "no_relay_payload"
                log("   [INFO] No relay data available")
            await _maybe_pause(index, pause_every, pause_seconds, log)

    return result


def _get_value(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _format_game_date(value: Any, *, fallback_game_id: str) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    text = str(value or "").replace("-", "").strip()
    if len(text) == 8 and text.isdigit():
        return text
    return str(fallback_game_id)[:8]


def _ids_with_rows(session, model, game_ids: List[str]) -> set[str]:
    return {
        row[0]
        for row in session.query(model.game_id)
        .filter(model.game_id.in_(game_ids))
        .distinct()
        .all()
    }


def _get_failure_reason(crawler: Any, game_id: str) -> Optional[str]:
    getter = getattr(crawler, "get_last_failure_reason", None)
    if not callable(getter):
        return None
    try:
        return getter(game_id)
    except Exception:
        return None


async def _maybe_pause(
    index: int,
    pause_every: Optional[int],
    pause_seconds: float,
    log: Callable[[str], None],
) -> None:
    if not pause_every or pause_every <= 0 or pause_seconds <= 0:
        return
    if index % pause_every == 0:
        log(f"[PAUSE] Sleeping for {pause_seconds:g}s before continuing...")
        await asyncio.sleep(pause_seconds)
