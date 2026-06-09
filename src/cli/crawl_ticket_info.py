"""KBO 티켓 가격/예매오픈 규칙을 수집하여 TicketPrice/TicketOpenRule 테이블에 저장하는 CLI 스크립트."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence

from src.crawlers.ticket_crawler import TicketCrawler


async def run(args: argparse.Namespace) -> None:
    crawler = TicketCrawler()
    await crawler.run(save=args.save, season=args.season)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="KBO ticket info crawler")
    parser.add_argument("--save", action="store_true", help="Save results to database")
    parser.add_argument("--season", type=int, default=None, help="Season year (default: current)")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(run(args))


if __name__ == "__main__":  # pragma: no cover
    main()
