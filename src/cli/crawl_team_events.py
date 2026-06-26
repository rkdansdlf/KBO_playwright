"""구단별 이벤트/뉴스 정보를 수집하여 TeamEvent 테이블에 저장하는 CLI 스크립트."""

from __future__ import annotations

import argparse
import asyncio
from typing import TYPE_CHECKING

from src.crawlers.team_event_crawler import TeamEventCrawler

if TYPE_CHECKING:
    from collections.abc import Sequence


async def run(args: argparse.Namespace) -> None:
    """
    Runs run.

    Args:
        args: Args.

    """
    crawler = TeamEventCrawler(days_back=args.days)
    await crawler.run(save=args.save, team_filter=args.team)


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Builds arg parser.

    Returns:
        The result of the operation.

    """
    parser = argparse.ArgumentParser(description="KBO team event/news crawler")
    parser.add_argument("--save", action="store_true", help="Save results to database")
    parser.add_argument("--days", type=int, default=30, help="Days back to crawl")
    parser.add_argument("--team", type=str, default=None, help="Team code filter (e.g. LG)")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Main entry point for this CLI command."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(run(args))


if __name__ == "__main__":  # pragma: no cover
    main()
