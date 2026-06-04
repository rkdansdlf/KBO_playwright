"""P0 데이터 수집 통합 CLI: 이벤트 + 콜업/말소 + 티켓을 순차 실행합니다."""

from __future__ import annotations

import argparse
import asyncio
from typing import Sequence

from src.utils.safe_print import safe_print as print


async def run_events(save: bool = False):
    from src.crawlers.team_event_crawler import TeamEventCrawler

    crawler = TeamEventCrawler(days_back=30)
    await crawler.run(save=save)


async def run_roster(save: bool = False):
    from src.crawlers.roster_transaction_crawler import RosterTransactionCrawler

    crawler = RosterTransactionCrawler()
    await crawler.run(save=save)


async def run_ticket(save: bool = False):
    from src.crawlers.ticket_crawler import TicketCrawler

    crawler = TicketCrawler()
    await crawler.run(save=save)


async def run_all(save: bool = False):
    print("=== P0 data collection ===")
    await run_events(save=save)
    await run_roster(save=save)
    await run_ticket(save=save)
    print("=== P0 complete ===")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="P0 Data Collection (events + roster + ticket)")
    parser.add_argument(
        "--type",
        choices=["events", "roster", "ticket", "all"],
        default="all",
        help="Data type to collect",
    )
    parser.add_argument("--save", action="store_true", help="Save results to database")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    runner_map = {
        "events": lambda: run_events(save=args.save),
        "roster": lambda: run_roster(save=args.save),
        "ticket": lambda: run_ticket(save=args.save),
        "all": lambda: run_all(save=args.save),
    }

    runner = runner_map[args.type]
    result = runner()
    if asyncio.iscoroutine(result):
        asyncio.run(result)


if __name__ == "__main__":
    main()
