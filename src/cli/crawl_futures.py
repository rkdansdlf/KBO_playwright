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
from typing import Sequence, Set
from datetime import datetime

from src.crawlers.player_list_crawler import PlayerListCrawler
from src.crawlers.futures_batting import fetch_and_parse_futures_batting
from src.repositories.player_repository import PlayerRepository
from src.repositories.save_futures_batting import save_futures_batting
from src.parsers.player_profile_parser import PlayerProfileParsed
from src.utils.safe_print import safe_print as print
from src.utils.playwright_pool import AsyncPlaywrightPool


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
    # 퓨처스리그 연도별 기록 페이지 URL 생성
    profile_url = f"https://www.koreabaseball.com/Futures/Player/HitterTotal.aspx?playerId={player_id}"

    # 데이터 크롤링 및 파싱
    rows = await fetch_and_parse_futures_batting(player_id, profile_url, pool=pool)

    if not rows:
        return (player_id, 0)

    # 데이터베이스에 선수 정보가 없으면 새로 생성
    player = await asyncio.to_thread(
        repository.upsert_player_profile,
        player_id,
        PlayerProfileParsed(is_active=True)
    )

    if not player:
        print(f"[WARN] Could not create player record for {player_id}")
        return (player_id, 0)

    # 파싱된 기록을 데이터베이스에 저장
    saved = await asyncio.to_thread(
        save_futures_batting,
        player.id,
        rows
    )

    return (player_id, saved)


async def crawl_futures(args: argparse.Namespace) -> None:
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

    if not player_ids:
        print("No players to process")
        return

    # 2단계: 각 선수를 병렬로 처리
    print(f"Processing {len(player_ids)} players...\n")

    repository = PlayerRepository()
    pool = AsyncPlaywrightPool(
        max_pages=args.concurrency,
        context_kwargs={"locale": "ko-KR"},
    )
    semaphore = asyncio.Semaphore(args.concurrency)  # 동시 요청 수 제어

    results = []
    errors = []

    async def runner(pid: str):
        async with semaphore:
            try:
                result = await process_player(pid, repository, args.delay, pool)
                results.append(result)

                player_id, saved = result
                if saved > 0:
                    print(f"[OK] {player_id}: {saved} seasons")
                else:
                    print(f"[SKIP] {player_id}: no Futures data")

            except Exception as exc:
                errors.append((pid, str(exc)))
                print(f"[ERROR] {pid}: {exc}")

    async with pool:
        await asyncio.gather(*(runner(pid) for pid in sorted(player_ids)))

    # 3단계: 결과 요약
    print(f"\n=== Summary ===")
    total_saved = sum(saved for _, saved in results)
    success_count = sum(1 for _, saved in results if saved > 0)

    print(f"Total players processed: {len(results)}")
    print(f"Players with Futures data: {success_count}")
    print(f"Total seasons saved: {total_saved}")
    print(f"Errors: {len(errors)}")

    if errors:
        print("\nErrors:")
        for pid, err in errors[:10]:  # Show first 10
            print(f"  {pid}: {err}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more")


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
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(crawl_futures(args))


if __name__ == "__main__":
    main()
