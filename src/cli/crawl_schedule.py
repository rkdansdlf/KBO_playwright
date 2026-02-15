"""KBO 리그의 시즌 경기 일정을 수집하여 데이터베이스에 저장하는 CLI 스크립트.

지정된 연도와 월에 해당하는 경기 일정 정보를 KBO 공식 사이트에서 크롤링하고,
이를 `game_schedules` 테이블에 저장(UPSERT)합니다.
"""
from __future__ import annotations

import argparse
import asyncio
from typing import Sequence, List

from src.crawlers.schedule_crawler import ScheduleCrawler
from src.repositories.game_repository import save_schedule_game


async def crawl_schedule(args: argparse.Namespace) -> None:
    """경기 일정 크롤링 및 저장 로직을 수행합니다."""
    months = parse_months(args.months)
    crawler = ScheduleCrawler(request_delay=args.delay)

    # 지정된 연도와 월의 경기 정보를 크롤링합니다.
    games = await crawler.crawl_season(args.year, months)
    print(f"[SCHEDULE] Total games discovered: {len(games)}")

    # 수집된 경기 정보를 데이터베이스에 저장합니다.
    saved = 0
    failed = 0
    for game in games:
        if save_schedule_game(game):
            saved += 1
        else:
            failed += 1
    print(f"[SCHEDULE] Saved: {saved}, Failed: {failed}")


def parse_months(months_arg: str | None) -> List[int]:
    """월 인자(e.g., "3-5,8")를 파싱하여 월 리스트(e.g., [3, 4, 5, 8])를 생성합니다."""
    if not months_arg:
        return list(range(3, 11))  # KBO 정규시즌은 보통 3월-10월
    parts = [p.strip() for p in months_arg.split(',') if p.strip()]
    months: List[int] = []
    for part in parts:
        if '-' in part:
            start, end = part.split('-', 1)
            try:
                start_m = int(start)
                end_m = int(end)
            except ValueError:
                continue
            months.extend(range(start_m, end_m + 1))
        else:
            try:
                months.append(int(part))
            except ValueError:
                continue
    return sorted(set(months))


def build_arg_parser() -> argparse.ArgumentParser:
    """CLI 인자 파서를 생성합니다."""
    parser = argparse.ArgumentParser(description="KBO season schedule crawler")
    parser.add_argument("--year", type=int, required=True, help="크롤링할 시즌 연도 (예: 2024)")
    parser.add_argument("--months", type=str, default=None, help="크롤링할 월 (쉼표로 구분, 범위 지정 가능. 예: 3-5,8)")
    parser.add_argument("--delay", type=float, default=1.2, help="요청 간 지연 시간(초)")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(crawl_schedule(args))


if __name__ == "__main__":  # pragma: no cover
    main()
