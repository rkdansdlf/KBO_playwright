"""KBO GameCenter의 박스스코어(box score)를 크롤링하기 위한 CLI 스크립트.

이 스크립트는 데이터베이스에서 크롤링이 필요한 경기 목록을 가져와, 각 경기의 상세
데이터(메타데이터, 팀 정보, 선수별 기록 등)를 수집하고 저장하는 역할을 합니다.
"""
from __future__ import annotations

import argparse
import asyncio
from typing import Sequence, List

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.game_repository import GameRepository


async def crawl_game_details(args: argparse.Namespace) -> None:
    """경기 상세 정보 크롤링 및 저장 로직을 수행합니다."""
    repo = GameRepository()
    # 데이터베이스에서 크롤링할 경기 목록을 가져옵니다.
    schedules = repo.fetch_schedules(status=args.status, limit=args.limit)

    if not schedules:
        print("ℹ️  No schedules found for crawl")
        return

    print(f"📋 Games to crawl: {len(schedules)}")

    inputs = []
    for sched in schedules:
        # 크롤링 상태를 'in_progress'로 업데이트합니다.
        repo.update_crawl_status(sched.game_id, 'in_progress')
        game_date = sched.game_date.strftime('%Y%m%d') if sched.game_date else sched.game_id[:8]
        inputs.append({'game_id': sched.game_id, 'game_date': game_date})

    # GameDetailCrawler를 사용하여 경기 상세 정보를 병렬로 크롤링합니다.
    crawler = GameDetailCrawler(request_delay=args.delay)
    results = await crawler.crawl_games(inputs)

    fetched_ids = {payload['game_id'] for payload in results}

    # 크롤링된 데이터를 데이터베이스에 저장합니다.
    for payload in results:
        repo.save_game_detail(payload)

    # 크롤링에 실패한 경기의 상태를 'failed'로 업데이트합니다.
    missing = [g for g in schedules if g.game_id not in fetched_ids]
    for sched in missing:
        repo.update_crawl_status(sched.game_id, 'failed', 'Crawler returned no data')


def build_arg_parser() -> argparse.ArgumentParser:
    """CLI 인자 파서를 생성합니다."""
    parser = argparse.ArgumentParser(description="Crawl KBO GameCenter details")
    parser.add_argument("--status", type=str, default="pending", help="크롤링할 경기의 상태 (기본값: pending)")
    parser.add_argument("--limit", type=int, default=10, help="크롤링할 최대 경기 수 (기본값: 10)")
    parser.add_argument("--delay", type=float, default=1.5, help="요청 간 지연 시간(초) (기본값: 1.5)")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(crawl_game_details(args))


if __name__ == "__main__":  # pragma: no cover
    main()

