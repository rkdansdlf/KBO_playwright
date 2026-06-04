"""
Collect Naver/KBO API responses as test fixtures.

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
import os
import sys

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.crawlers.relay_crawler import RelayCrawler
from src.utils.safe_print import safe_print as print

FIXTURE_DIRS = {
    "naver_live": "tests/fixtures/naver_live",
    "naver_result": "tests/fixtures/naver_result",
    "kbo_live_text": "tests/fixtures/kbo_live_text",
    "naver_schedule": "tests/fixtures/naver_live",
}


def _ensure_dirs():
    for dirpath in FIXTURE_DIRS.values():
        os.makedirs(dirpath, exist_ok=True)


async def fetch_naver_schedule(game_id: str, crawler: RelayCrawler) -> dict | None:
    async with httpx.AsyncClient() as client:
        query_dates = crawler._schedule_query_dates(game_id)
        for qd in query_dates:
            query = crawler._schedule_query_context(game_id, query_date=qd)
            payload, _ = await crawler._request_json(client, crawler.schedule_api_base_url, params=query)
            if payload and payload.get("result", {}).get("games"):
                return payload
    return None


async def fetch_naver_relay(naver_id: str, crawler: RelayCrawler) -> dict | None:
    async with httpx.AsyncClient() as client:
        innings_data = {}
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


async def main():
    parser = argparse.ArgumentParser(description="Collect Naver/KBO relay fixtures")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--game-id", help="KBO game ID to collect fixtures for")
    group.add_argument("--date", help="Date (YYYY-MM-DD) to scan for games")
    args = parser.parse_args()

    _ensure_dirs()
    crawler = RelayCrawler()

    game_ids = []
    if args.game_id:
        game_ids = [args.game_id]
    elif args.date:
        crawler = RelayCrawler()
        async with httpx.AsyncClient() as client:
            query = crawler._schedule_query_context(query_date=args.date.replace("-", ""))
            payload, _ = await crawler._request_json(client, crawler.schedule_api_base_url, params=query)
            if payload:
                games = (payload.get("result") or {}).get("games") or []
                for g in games:
                    naver_id = str(g.get("gameId", ""))
                    game_ids.append(naver_id)
                print(f"Found {len(game_ids)} games on {args.date}")

    for gid in game_ids:
        print(f"\n--- Processing {gid} ---")

        schedule = await fetch_naver_schedule(gid, crawler)
        if schedule:
            game_status = "unknown"
            games_list = (schedule.get("result") or {}).get("games") or []
            for g in games_list:
                if str(g.get("gameId", "")).endswith(gid[-12:]):
                    game_status = str(g.get("status", "unknown"))
                    break
            prefix = "naver_result" if game_status == "RESULT" else "naver_live"
            schedule_path = os.path.join(FIXTURE_DIRS[prefix], f"schedule_{gid[:8]}.json")
            with open(schedule_path, "w", encoding="utf-8") as f:
                json.dump(schedule, f, ensure_ascii=False, indent=2)
            print(f"  Saved schedule to {schedule_path} (status={game_status})")

        naver_id = crawler._map_to_naver_id(gid)
        innings_data = await fetch_naver_relay(naver_id, crawler)
        if innings_data:
            prefix = "naver_result" if game_status == "RESULT" else "naver_live"
            for inn_str, payload in innings_data.items():
                inn_padded = inn_str.zfill(2)
                relay_path = os.path.join(FIXTURE_DIRS[prefix], f"relay_inning_{inn_padded}.json")
                with open(relay_path, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
            print(f"  Saved {len(innings_data)} inning relays to {prefix}/")

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
