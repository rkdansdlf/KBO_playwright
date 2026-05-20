"""
퓨처스리그 선수들의 연도별 타격 기록을 수집하는 CLI 스크립트.

이 스크립트는 다음 단계를 수행합니다:
1. 특정 시즌의 모든 현역 선수 ID 목록을 가져옵니다.
2. 각 선수에 대해 퓨처스리그 기록 페이지로 이동하여 연도별 타격 데이터를 크롤링하고 파싱합니다.
3. 파싱된 데이터를 데이터베이스에 UPSERT(존재하면 업데이트, 없으면 삽입)하여 저장합니다.
"""
from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter
from typing import Sequence, Set
from datetime import datetime

from src.crawlers.player_list_crawler import PlayerListCrawler
from src.crawlers.futures.futures_batting import fetch_and_parse_futures_batting
from src.repositories.player_repository import PlayerRepository
from src.repositories.save_futures_batting import save_futures_batting
from src.parsers.player_profile_parser import PlayerProfileParsed
from src.utils.safe_print import safe_print as print
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.player_validation import normalize_player_id


async def gather_active_player_ids(season_year: int, delay: float) -> Set[str]:
    """지정된 시즌의 모든 현역 선수 ID를 수집합니다."""
    print(f"Gathering active player list for {season_year}...")
    crawler = PlayerListCrawler(request_delay=delay)
    result = await crawler.crawl_all_players(season_year=season_year)

    ids: Set[str] = set()
    for bucket in ("hitters", "pitchers"):
        for player in result.get(bucket, []):
            pid = player.get("player_id")
            if pid:
                ids.add(pid)

    print(f"Found {len(ids)} active players")
    return ids


async def process_player(
    player_id: str,
    repository: PlayerRepository,
    delay: float,
    pool,
) -> tuple[str, int]:
    """
    단일 선수의 퓨처스리그 기록을 크롤링하고 데이터베이스에 저장합니다.

    Returns:
        (player_id, 저장된 시즌 기록 수)
    """
    result = await process_player_result(player_id, repository, delay, pool)
    return (str(result["player_id"]), int(result["saved"]))


async def process_player_result(
    player_id: str,
    repository: PlayerRepository,
    delay: float,
    pool,
) -> dict:
    normalized_id = normalize_player_id(player_id)
    if normalized_id is None:
        return {
            "player_id": player_id,
            "status": "failed",
            "saved": 0,
            "failure_reason": "invalid_player_id",
        }

    # 퓨처스리그 연도별 기록 페이지 URL 생성
    player_id = str(normalized_id)
    profile_url = f"https://www.koreabaseball.com/Futures/Player/HitterTotal.aspx?playerId={player_id}"

    try:
        # 데이터 크롤링 및 파싱
        rows = await fetch_and_parse_futures_batting(player_id, profile_url, pool=pool)
    except Exception as exc:
        return {
            "player_id": player_id,
            "status": "failed",
            "saved": 0,
            "failure_reason": "exception",
            "error": str(exc),
        }

    if not rows:
        return {
            "player_id": player_id,
            "status": "skipped",
            "saved": 0,
            "failure_reason": "futures_empty",
        }

    # 데이터베이스에 선수 정보가 없으면 새로 생성
    player = await asyncio.to_thread(
        repository.upsert_player_profile,
        player_id,
        PlayerProfileParsed(is_active=True)
    )

    if not player:
        print(f"[WARN] Could not create player record for {player_id}")
        return {
            "player_id": player_id,
            "status": "failed",
            "saved": 0,
            "failure_reason": "profile_upsert_failed",
        }

    # 파싱된 기록을 데이터베이스에 저장
    saved = await asyncio.to_thread(
        save_futures_batting,
        player_id,
        rows
    )

    if saved > 0:
        return {
            "player_id": player_id,
            "status": "success",
            "saved": saved,
            "failure_reason": None,
        }

    return {
        "player_id": player_id,
        "status": "failed",
        "saved": 0,
        "failure_reason": "save_failed",
    }


async def crawl_futures(args: argparse.Namespace) -> dict:
    """퓨처스리그 크롤링 메인 로직."""
    print(f"\n=== Futures League Batting Stats Crawler ===")
    print(f"Season: {args.season}")
    print(f"Concurrency: {args.concurrency}")
    print(f"Delay: {args.delay}s\n")

    # 1단계: 크롤링 대상 선수 ID 목록 수집
    player_ids = await gather_active_player_ids(args.season, args.delay)

    if args.limit:
        player_ids = set(sorted(player_ids)[:args.limit])
        print(f"Limited to {len(player_ids)} players\n")

    summary = {
        "ok": False,
        "season": args.season,
        "processed": 0,
        "success_count": 0,
        "total_saved": 0,
        "failure_counts": {},
        "results": [],
    }

    if not player_ids:
        print("No players to process")
        summary["failure_counts"] = {"player_list_empty": 1}
        if getattr(args, "json_summary", False):
            print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
        return summary

    # 2단계: 각 선수를 병렬로 처리
    print(f"Processing {len(player_ids)} players...\n")

    repository = PlayerRepository()
    pool = AsyncPlaywrightPool(
        max_pages=args.concurrency,
        context_kwargs={"locale": "ko-KR"},
    )
    semaphore = asyncio.Semaphore(args.concurrency)  # 동시 요청 수 제어

    results: list[dict] = []
    failure_counts: Counter = Counter()

    async def runner(pid: str):
        async with semaphore:
            result = await process_player_result(pid, repository, args.delay, pool)
            results.append(result)

            player_id = result["player_id"]
            saved = result["saved"]
            failure_reason = result.get("failure_reason")
            if result["status"] == "success":
                print(f"[OK] {player_id}: {saved} seasons")
            elif failure_reason == "futures_empty":
                failure_counts[failure_reason] += 1
                print(f"[SKIP] {player_id}: no Futures data")
            else:
                failure_counts[failure_reason or "exception"] += 1
                print(f"[ERROR] {player_id}: {failure_reason}")

    async with pool:
        await asyncio.gather(*(runner(pid) for pid in sorted(player_ids)))

    # 3단계: 결과 요약
    print(f"\n=== Summary ===")
    total_saved = sum(result["saved"] for result in results)
    success_count = sum(1 for result in results if result["status"] == "success")

    print(f"Total players processed: {len(results)}")
    print(f"Players with Futures data: {success_count}")
    print(f"Total seasons saved: {total_saved}")
    print(f"Failures/skips: {sum(failure_counts.values())}")

    if failure_counts:
        print("\nFailure reasons:")
        for reason, count in sorted(failure_counts.items()):
            print(f"  {reason}: {count}")

    summary.update(
        {
            "ok": not any(result["status"] == "failed" for result in results),
            "processed": len(results),
            "success_count": success_count,
            "total_saved": total_saved,
            "failure_counts": dict(failure_counts),
            "results": results,
        }
    )
    if getattr(args, "json_summary", False):
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    """CLI 인자 파서를 생성합니다."""
    parser = argparse.ArgumentParser(
        description="Crawl year-by-year Futures batting stats for active players"
    )
    parser.add_argument(
        "--season",
        type=int,
        default=datetime.now().year,
        help="기준 시즌 (기본값: 현재 연도)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="동시 요청 수 (기본값: 3)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="요청 간 지연 시간(초) (기본값: 2.0)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="처리할 최대 선수 수 (테스트용)",
    )
    parser.add_argument(
        "--json-summary",
        action="store_true",
        help="Print a machine-readable summary after the run",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(crawl_futures(args))


if __name__ == "__main__":
    main()
