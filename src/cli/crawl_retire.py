"""
은퇴 또는 비활동 선수의 데이터를 수집하기 위한 CLI 스크립트.

이 스크립트는 다음 과정을 통해 과거 선수들의 기록을 수집합니다:
1. 특정 기간(예: 1982-2023)의 모든 선수 ID와 현재 시즌의 현역 선수 ID를 비교하여
   은퇴/비활동 선수 ID 목록을 식별합니다.
2. 식별된 각 선수에 대해 은퇴 선수 기록 페이지(타자/투수)에 접근합니다.
3. 선수의 프로필 정보와 연도별 시즌 기록을 파싱하여 데이터베이스에 저장합니다.
"""
from __future__ import annotations

import argparse
import asyncio
from typing import Sequence, Set

from src.crawlers.retire import RetiredPlayerListingCrawler, RetiredPlayerDetailCrawler
from src.parsers.player_profile_parser import parse_profile, PlayerProfileParsed
from src.parsers.retired_player_parser import (
    parse_retired_hitter_tables,
    parse_retired_pitcher_table,
)
from src.repositories.player_repository import PlayerRepository


async def determine_inactive_ids(
    start_year: int,
    end_year: int,
    active_year: int,
    request_delay: float,
) -> Set[str]:
    """과거 시즌과 현재 시즌의 선수 명단을 비교하여 은퇴/비활동 선수 ID를 식별합니다."""
    listing_crawler = RetiredPlayerListingCrawler(request_delay=request_delay)
    return await listing_crawler.determine_inactive_player_ids(
        start_year=start_year,
        end_year=end_year,
        active_year=active_year,
    )


async def process_player(
    player_id: str,
    detail_crawler: RetiredPlayerDetailCrawler,
    repository: PlayerRepository,
) -> None:
    """단일 은퇴 선수의 상세 정보(프로필, 시즌 기록)를 크롤링하고 저장합니다."""
    # 타자 및 투수 페이지에서 선수 정보를 가져옵니다.
    detail_payload = await detail_crawler.fetch_player(player_id)
    hitter_payload = detail_payload.get("hitter")
    pitcher_payload = detail_payload.get("pitcher")

    # 프로필 텍스트를 추출하고 파싱합니다.
    profile_text = None
    if hitter_payload:
        profile_text = hitter_payload.get("profile_text")
    if not profile_text and pitcher_payload:
        profile_text = pitcher_payload.get("profile_text")

    if profile_text:
        parsed_profile = parse_profile(profile_text, is_active=False)
    else:
        parsed_profile = PlayerProfileParsed(is_active=False)

    # 선수 프로필 정보를 데이터베이스에 UPSERT합니다.
    player = await asyncio.to_thread(
        repository.upsert_player_profile,
        player_id,
        parsed_profile,
    )

    if not player:
        return

    # 타자 기록이 있으면 파싱하여 저장합니다.
    if hitter_payload:
        batting_records = parse_retired_hitter_tables(hitter_payload.get("tables", []))
        for record in batting_records:
            await asyncio.to_thread(repository.upsert_season_batting, player.id, record)

    # 투수 기록이 있으면 파싱하여 저장합니다.
    if pitcher_payload:
        tables = pitcher_payload.get("tables", [])
        if tables:
            pitching_records = parse_retired_pitcher_table(tables[0])
            for record in pitching_records:
                await asyncio.to_thread(repository.upsert_season_pitching, player.id, record)


async def crawl_retired_players(args: argparse.Namespace) -> None:
    """은퇴 선수 데이터 수집 파이프라인의 메인 로직."""
    # 1단계: 은퇴/비활동 선수 ID 목록을 결정합니다.
    inactive_ids = await determine_inactive_ids(
        start_year=args.start_year,
        end_year=args.end_year,
        active_year=args.active_year or args.end_year,
        request_delay=args.delay,
    )

    inactive_list = sorted(inactive_ids)
    if args.limit:
        inactive_list = inactive_list[: args.limit]

    print(f"📋 Retired candidates: {len(inactive_list)}")
    if not inactive_list:
        return

    # 2단계: 각 선수를 병렬로 처리합니다.
    detail_crawler = RetiredPlayerDetailCrawler(request_delay=args.delay)
    repository = PlayerRepository()
    semaphore = asyncio.Semaphore(args.concurrency)  # 동시 요청 수 제어

    async def runner(pid: str):
        async with semaphore:
            try:
                await process_player(pid, detail_crawler, repository)
                print(f"✅ Processed retired player {pid}")
            except Exception as exc:
                print(f"❌ Failed to process player {pid}: {exc}")

    await asyncio.gather(*(runner(pid) for pid in inactive_list))


def build_arg_parser() -> argparse.ArgumentParser:
    """CLI 인자 파서를 생성합니다."""
    parser = argparse.ArgumentParser(description="Retired player crawling pipeline")
    parser.add_argument("--start-year", type=int, default=1982, help="비교 시작 연도")
    parser.add_argument("--end-year", type=int, default=2024, help="비교 종료 연도")
    parser.add_argument("--active-year", type=int, default=None, help="현역 선수 기준 연도")
    parser.add_argument("--concurrency", type=int, default=3, help="동시 요청 수")
    parser.add_argument("--delay", type=float, default=1.5, help="요청 간 지연 시간(초)")
    parser.add_argument("--limit", type=int, default=None, help="처리할 최대 선수 수 (디버깅용)")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(crawl_retired_players(args))


if __name__ == "__main__":
    main()
