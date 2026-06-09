"""
CLI for crawling stadium operation notices from LG Twins, Doosan Bears, and Naver Search.

Usage:
    python -m src.cli.crawl_operation_notices --team LG --save
    python -m src.cli.crawl_operation_notices --team OB --save --pages 3
    python -m src.cli.crawl_operation_notices --save            # both official teams
    python -m src.cli.crawl_operation_notices --incremental     # stop at last seen ID
    python -m src.cli.crawl_operation_notices --source naver --save        # Naver 검색
    python -m src.cli.crawl_operation_notices --source naver --days 1      # 오늘자만
    python -m src.cli.crawl_operation_notices --source all --save          # 전체 소스
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from collections.abc import Sequence

from src.crawlers.operation_notice_doosan_crawler import OperationNoticeDoosanCrawler
from src.crawlers.operation_notice_lg_crawler import OperationNoticeLGCrawler
from src.db.engine import SessionLocal
from src.repositories.operation_notice_repository import OperationNoticeRepository

logger = logging.getLogger(__name__)

TEAM_CRAWLERS = {
    "LG": (OperationNoticeLGCrawler, "LG트윈스공식"),
    "OB": (OperationNoticeDoosanCrawler, "두산베어스공식"),
}


async def _run_official_crawlers(args: argparse.Namespace) -> None:
    """LG/Doosan 공식 홈페이지 크롤러 실행."""
    teams = [args.team.upper()] if args.team else list(TEAM_CRAWLERS.keys())

    for team_code in teams:
        if team_code not in TEAM_CRAWLERS:
            logger.warning("Unknown team: %s (supported: %s)", team_code, list(TEAM_CRAWLERS.keys()))
            continue

        crawler_cls, source_name = TEAM_CRAWLERS[team_code]
        stop_id: str | None = None

        if args.incremental:
            with SessionLocal() as session:
                repo = OperationNoticeRepository(session)
                stop_id = repo.get_latest_external_id("JAMSIL", source_name)
                if stop_id:
                    logger.info(f"[{team_code}] Incremental mode: stopping at external_id={stop_id}")

        crawler = crawler_cls(max_pages=args.pages)
        await crawler.run(save=args.save, stop_at_external_id=stop_id)


async def _run_naver_crawler(args: argparse.Namespace) -> None:
    """Naver 검색 API 기반 공지 크롤러 실행."""
    from src.crawlers.operation_notice_naver_crawler import OperationNoticeNaverCrawler

    days = getattr(args, "days", 3)
    crawler = OperationNoticeNaverCrawler(days_back=days)
    await crawler.run(save=args.save)


async def run(args: argparse.Namespace) -> None:
    source = getattr(args, "source", "official")

    if source == "naver":
        await _run_naver_crawler(args)
    elif source == "all":
        await _run_official_crawlers(args)
        await _run_naver_crawler(args)
    else:
        # Default: official team websites
        await _run_official_crawlers(args)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Crawl stadium operation notices (official + Naver search)")
    parser.add_argument("--save", action="store_true", help="Save results to database")
    parser.add_argument(
        "--source",
        type=str,
        default="official",
        choices=["official", "naver", "all"],
        help="Data source: 'official' (LG/Doosan websites), 'naver' (Naver Search API), 'all'",
    )
    parser.add_argument(
        "--team",
        type=str,
        default=None,
        help="Team code for --source official: LG or OB. Omit to crawl both.",
    )
    parser.add_argument("--pages", type=int, default=5, help="Max pages per team (official mode)")
    parser.add_argument(
        "--days",
        type=int,
        default=3,
        help="Days back for Naver search (default: 3)",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Stop when a previously seen article is encountered (official mode)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(run(args))


if __name__ == "__main__":  # pragma: no cover
    main()
