"""Collect Naver/KBO API responses as test fixtures.

Usage:
    python3 scripts/diagnostic/collect_relay_fixtures.py --game-id 20260412SKLG0
    python3 scripts/diagnostic/collect_relay_fixtures.py --date 2026-04-12

Saves responses to tests/fixtures/naver_live/, tests/fixtures/naver_result/,
and tests/fixtures/kbo_live_text/.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.crawlers.relay_crawler import RelayCrawler

logger = logging.getLogger(__name__)

FIXTURE_DIRS = {
    "naver_live": "tests/fixtures/naver_live",
    "naver_result": "tests/fixtures/naver_result",
    "kbo_live_text": "tests/fixtures/kbo_live_text",
    "naver_schedule": "tests/fixtures/naver_live",
}


def _ensure_dirs():
    for dirpath in FIXTURE_DIRS.values():
        Path(dirpath).mkdir(parents=True, exist_ok=True)


async def fetch_naver_schedule(game_id: str, crawler: RelayCrawler) -> dict[str, Any] | None:
    async with httpx.AsyncClient() as client:
        query_dates = crawler._schedule_query_dates(game_id)
        for qd in query_dates:
            query = crawler._schedule_query_context(game_id, query_date=qd)
            payload, _ = await crawler._request_json(client, crawler.schedule_api_base_url, params=query)
            if payload and payload.get("result", {}).get("games"):
                return payload
    return None


async def fetch_naver_relay(naver_id: str, crawler: RelayCrawler) -> dict[str, Any] | None:
    async with httpx.AsyncClient() as client:
        innings_data: dict[str, Any] = {}
        for inn in range(1, 16):
            url = f"{crawler.api_base_url.format(game_id=naver_id)}?inning={inn}"
            data, _ = await crawler._request_json(client, url)
            if data is None:
                break
            result = data.get("result") or {}
            relay_data = result.get("textRelayData") or {}
            text_relays = relay_data.get("textRelays") or []
            if not text_relays:
                break
            has_logs = any(len(tr.get("textOptions", [])) > 0 for tr in text_relays)
            if not has_logs and innings_data:
                break
            innings_data[str(inn)] = data
        if not innings_data:
            return None
        return innings_data


async def _resolve_game_ids(args) -> list[str]:
    if args.game_id:
        return [args.game_id]
    crawler = RelayCrawler()
    game_ids: list[str] = []
    async with httpx.AsyncClient() as client:
        query = crawler._schedule_query_context(query_date=args.date.replace("-", ""))
        payload, _ = await crawler._request_json(client, crawler.schedule_api_base_url, params=query)
        if payload:
            for g in (payload.get("result") or {}).get("games") or []:
                game_ids.append(str(g.get("gameId", "")))
            logger.info("Found %d games on %s", len(game_ids), args.date)
    return game_ids


async def _process_game_fixture(gid: str, crawler: RelayCrawler) -> str:
    schedule = await fetch_naver_schedule(gid, crawler)
    game_status = "unknown"
    if schedule:
        for g in (schedule.get("result") or {}).get("games") or []:
            if str(g.get("gameId", "")).endswith(gid[-12:]):
                game_status = str(g.get("status", "unknown"))
                break
        prefix = "naver_result" if game_status == "RESULT" else "naver_live"
        schedule_path = Path(FIXTURE_DIRS[prefix], f"schedule_{gid[:8]}.json")
        with schedule_path.open("w", encoding="utf-8") as f:
            json.dump(schedule, f, ensure_ascii=False, indent=2)
        logger.info("  Saved schedule to %s (status=%s)", schedule_path, game_status)
    naver_id = crawler._map_to_naver_id(gid)
    innings_data = await fetch_naver_relay(naver_id, crawler)
    if innings_data:
        prefix = "naver_result" if game_status == "RESULT" else "naver_live"
        for inn_str, payload in innings_data.items():
            relay_path = Path(FIXTURE_DIRS[prefix], f"relay_inning_{inn_str.zfill(2)}.json")
            with relay_path.open("w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        logger.info("  Saved %d inning relays to %s/", len(innings_data), prefix)
    return game_status


async def main():
    parser = argparse.ArgumentParser(description="Collect Naver/KBO relay fixtures")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--game-id", help="KBO game ID to collect fixtures for")
    group.add_argument("--date", help="Date (YYYY-MM-DD) to scan for games")
    args = parser.parse_args()
    _ensure_dirs()
    game_ids = await _resolve_game_ids(args)
    crawler = RelayCrawler()
    for gid in game_ids:
        logger.info("--- Processing %s ---", gid)
        await _process_game_fixture(gid, crawler)
    logger.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
