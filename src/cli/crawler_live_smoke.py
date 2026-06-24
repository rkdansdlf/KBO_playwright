"""Opt-in live smoke checks for crawler release verification."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import json
import logging
import os
from datetime import datetime
from typing import TYPE_CHECKING, Any

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.relay_crawler import RelayCrawler
from src.crawlers.schedule_crawler import ScheduleCrawler
from src.utils.schedule_validation import is_detail_candidate_game
from src.utils.team_codes import normalize_kbo_game_id

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

logger = logging.getLogger(__name__)

SCOPES = ("schedule", "detail", "relay", "all")


def _network_allowed(*, allow_network: bool) -> bool:
    return allow_network or os.getenv("KBO_LIVE_SMOKE", "").strip() == "1"


def _base_result(target_date: str, scope: str) -> dict[str, Any]:
    return {
        "ok": False,
        "target_date": target_date,
        "scope": scope,
        "candidates": [],
        "results": [],
        "failure_reasons": {},
    }


def _candidate(game_id: str, game_date: str) -> dict[str, str]:
    return {"game_id": normalize_kbo_game_id(game_id), "game_date": game_date}


def _select_schedule_candidates(
    games: Sequence[Mapping[str, Any]],
    *,
    target_date: str,
    limit: int,
) -> list[dict[str, str]]:
    selected: list[dict[str, str]] = []
    target_day = datetime.strptime(target_date, "%Y%m%d").date()
    for game in games:
        game_date = str(game.get("game_date") or "").replace("-", "")
        if game_date != target_date:
            continue
        if not is_detail_candidate_game(game, today=target_day):
            continue
        game_id = str(game.get("game_id") or "").strip()
        if not game_id:
            continue
        selected.append(_candidate(game_id, target_date))
        if len(selected) >= limit:
            break
    return selected


def _row_count(payload: Mapping[str, Any] | None, section: str, side: str) -> int:
    rows = ((payload or {}).get(section) or {}).get(side) or []
    return len(rows) if isinstance(rows, list) else 0


def _detail_complete(payload: Mapping[str, Any] | None) -> bool:
    return all(
        _row_count(payload, section, side) > 0 for section in ("hitters", "pitchers") for side in ("away", "home")
    )


async def _resolve_candidates(
    *,
    target_date: str,
    game_id: str | None,
    limit: int,
    schedule_crawler: ScheduleCrawler,
) -> list[dict[str, str]]:
    if game_id:
        normalized = normalize_kbo_game_id(game_id)
        if normalized[:8] != target_date:
            msg = "--game-id date prefix must match --date"
            raise ValueError(msg)
        return [_candidate(normalized, target_date)]

    year = int(target_date[:4])
    month = int(target_date[4:6])
    games = await schedule_crawler.crawl_schedule(year, month)
    return _select_schedule_candidates(games, target_date=target_date, limit=limit)


async def run_smoke(  # noqa: PLR0913
    *,
    target_date: str,
    scope: str,
    game_id: str | None = None,
    limit: int = 1,
    schedule_crawler: ScheduleCrawler | None = None,
    detail_crawler: GameDetailCrawler | None = None,
    relay_crawler: RelayCrawler | None = None,
) -> dict[str, Any]:
    if scope not in SCOPES:
        msg = f"Unsupported scope: {scope}"
        raise ValueError(msg)
    if limit < 1:
        msg = "--limit must be at least 1"
        raise ValueError(msg)

    result = _base_result(target_date, scope)
    schedule = schedule_crawler or ScheduleCrawler()
    detail = detail_crawler or GameDetailCrawler()
    relay = relay_crawler or RelayCrawler()

    candidates = await _resolve_candidates(
        target_date=target_date,
        game_id=game_id,
        limit=limit,
        schedule_crawler=schedule,
    )
    result["candidates"] = [item["game_id"] for item in candidates]
    if not candidates:
        result["failure_reasons"]["schedule"] = ["no_detail_candidates"]
        return result

    run_detail = scope in {"detail", "all"}
    run_relay = scope in {"relay", "all"}
    if scope == "schedule":
        result["ok"] = True
        return result

    for item in candidates:
        game_result: dict[str, Any] = {
            "game_id": item["game_id"],
            "game_date": item["game_date"],
        }

        if run_detail:
            payload = await detail.crawl_game(item["game_id"], item["game_date"], lightweight=False)
            detail_ok = _detail_complete(payload)
            failure_reason = None
            if not detail_ok:
                getter = getattr(detail, "get_last_failure_reason", None)
                failure_reason = getter(item["game_id"]) if callable(getter) else None
                failure_reason = failure_reason or "incomplete_detail"
                result["failure_reasons"].setdefault(item["game_id"], []).append(failure_reason)
            game_result["detail"] = {
                "ok": detail_ok,
                "failure_reason": failure_reason,
                "hitters": {
                    "away": _row_count(payload, "hitters", "away"),
                    "home": _row_count(payload, "hitters", "home"),
                },
                "pitchers": {
                    "away": _row_count(payload, "pitchers", "away"),
                    "home": _row_count(payload, "pitchers", "home"),
                },
            }

        if run_relay:
            payload = await relay.crawl_game_relay(item["game_id"])
            event_count = len((payload or {}).get("events") or [])
            raw_count = len((payload or {}).get("raw_pbp_rows") or [])
            relay_ok = event_count > 0 or raw_count > 0
            failure_reason = None
            if not relay_ok:
                getter = getattr(relay, "get_last_failure_reason", None)
                failure_reason = getter(item["game_id"]) if callable(getter) else None
                failure_reason = failure_reason or "relay_empty"
                result["failure_reasons"].setdefault(item["game_id"], []).append(failure_reason)
            game_result["relay"] = {
                "ok": relay_ok,
                "failure_reason": failure_reason,
                "events": event_count,
                "raw_pbp_rows": raw_count,
            }

        result["results"].append(game_result)

    result["ok"] = not result["failure_reasons"]
    return result


def _print_human_summary(result: Mapping[str, Any]) -> None:
    status = "passed" if result.get("ok") else "failed"
    logger.info(
        "[SMOKE] %s: date=%s scope=%s candidates=%s",
        status,
        result.get("target_date"),
        result.get("scope"),
        len(result.get("candidates") or []),
    )
    for game_result in result.get("results") or []:
        logger.info("  - %s: %s", game_result.get("game_id"), game_result)
    if result.get("failure_reasons"):
        logger.info("[SMOKE] failure_reasons=%s", result["failure_reasons"])


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Opt-in live smoke check for KBO crawler release verification")
    parser.add_argument("--date", required=True, help="Target date in YYYYMMDD format")
    parser.add_argument("--game-id", help="Specific game_id to smoke test")
    parser.add_argument("--limit", type=int, default=1, help="Maximum schedule candidates when --game-id is omitted")
    parser.add_argument("--scope", choices=SCOPES, default="all", help="Crawler scope to smoke test")
    parser.add_argument("--allow-network", action="store_true", help="Allow live KBO/Naver network access")
    parser.add_argument("--json", action="store_true", help="Print result as JSON")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if len(args.date) != 8 or not args.date.isdigit():
        parser.error("--date must be YYYYMMDD")
    if args.limit < 1:
        parser.error("--limit must be at least 1")

    if not _network_allowed(allow_network=args.allow_network):
        result = _base_result(args.date, args.scope)
        result["failure_reasons"] = {"network": ["network_not_allowed"]}
        if args.json:
            logger.info(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            logger.error("Live smoke requires --allow-network or KBO_LIVE_SMOKE=1")
        return 2

    try:
        if args.json:
            with contextlib.redirect_stdout(io.StringIO()):
                result = asyncio.run(
                    run_smoke(
                        target_date=args.date,
                        scope=args.scope,
                        game_id=args.game_id,
                        limit=args.limit,
                    ),
                )
            logger.info(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            result = asyncio.run(
                run_smoke(
                    target_date=args.date,
                    scope=args.scope,
                    game_id=args.game_id,
                    limit=args.limit,
                ),
            )
            _print_human_summary(result)
    except ValueError:
        logger.exception("[ERROR] Smoke test configuration is invalid")
        return 2

    return 0 if result.get("ok") else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
