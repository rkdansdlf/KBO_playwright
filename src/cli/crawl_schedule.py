"""KBO 리그의 시즌 경기 일정을 수집하여 데이터베이스에 저장하는 CLI 스크립트.

지정된 연도와 월에 해당하는 경기 일정 정보를 KBO 공식 사이트에서 크롤링하고,
이를 `game_schedules` 테이블에 저장(UPSERT)합니다.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from collections.abc import Sequence
from datetime import datetime

from dateutil.relativedelta import relativedelta

from src.crawlers.schedule_crawler import ScheduleCrawler
from src.services.schedule_collection_service import save_schedule_games

logger = logging.getLogger(__name__)


async def crawl_schedule(args: argparse.Namespace) -> None:
    """경기 일정 크롤링 및 저장 로직을 수행합니다."""
    if getattr(args, "upcoming", False):
        await _crawl_upcoming_months(args)
        return
    months = parse_months(args.months)
    crawler = ScheduleCrawler(request_delay=args.delay)

    # 지정된 연도와 월의 경기 정보를 크롤링합니다.
    games = await crawler.crawl_season(args.year, months)
    logger.info(f"[SCHEDULE] Total games discovered: {len(games)}")

    # 수집된 경기 정보를 데이터베이스에 저장합니다.
    result = save_schedule_games(games)
    logger.info(f"[SCHEDULE] Saved: {result.saved}, Failed: {result.failed}")


async def _crawl_upcoming_months(args: argparse.Namespace) -> None:
    """현재월 + 다음월 일정을 크롤링합니다."""
    crawler = ScheduleCrawler(request_delay=args.delay)
    now = datetime.now()
    targets = [(now.year, now.month), ((now + relativedelta(months=1)).year, (now + relativedelta(months=1)).month)]

    if args.year and args.months:
        y = int(args.year)
        ms = [int(m.strip()) for m in str(args.months).split(",") if m.strip()]
        targets = [(y, m) for m in ms]

    logger.info(f"[UPCOMING] Crawling schedule for: {targets}")
    total_saved = 0
    for year, month in targets:
        games = await crawler.crawl_schedule(year, month)
        result = save_schedule_games(games)
        total_saved += result.saved
        logger.info(f"[UPCOMING] {year}-{month:02d}: {len(games)} games, {result.saved} upserted")
    logger.info(f"[UPCOMING] Done. Total upserts: {total_saved}")


def parse_months(months_arg: str | None) -> list[int]:
    """월 인자(e.g., "3-5,8")를 파싱하여 월 리스트(e.g., [3, 4, 5, 8])를 생성합니다."""
    if not months_arg:
        return list(range(3, 11))  # KBO 정규시즌은 보통 3월-10월
    parts = [p.strip() for p in months_arg.split(",") if p.strip()]
    months: list[int] = []
    for part in parts:
        if "-" in part:
            start, end = part.split("-", 1)
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
    parser.add_argument("--year", type=int, help="크롤링할 시즌 연도 (예: 2024)")
    parser.add_argument("--months", type=str, default=None, help="크롤링할 월 (쉼표로 구분, 범위 지정 가능. 예: 3-5,8)")
    parser.add_argument("--delay", type=float, default=1.2, help="요청 간 지연 시간(초)")
    parser.add_argument(
        "--upcoming", action="store_true", help="현재월+다음월 일정만 크롤링 (기존 crawl_upcoming 대체)"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(crawl_schedule(args))


if __name__ == "__main__":  # pragma: no cover
    main()
