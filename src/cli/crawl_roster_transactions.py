"""KBO 1군 등록/말소 트랜잭션을 수집하여 RosterTransaction 테이블에 저장하는 CLI 스크립트."""

from __future__ import annotations

import argparse
import asyncio
from typing import Sequence

from src.crawlers.roster_transaction_crawler import RosterTransactionCrawler


async def run(args: argparse.Namespace) -> None:
    crawler = RosterTransactionCrawler()
    await crawler.run(save=args.save, target_date=args.date)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="KBO roster transaction crawler")
    parser.add_argument("--save", action="store_true", help="Save results to database")
    parser.add_argument("--date", type=str, default=None, help="Target date (YYYY-MM-DD, default: today)")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(run(args))


if __name__ == "__main__":  # pragma: no cover
    main()
