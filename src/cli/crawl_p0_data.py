"""P0 데이터 수집 통합 CLI: 이벤트 + 콜업/말소 + 티켓을 순차 실행합니다."""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime
from collections.abc import Sequence

logger = logging.getLogger(__name__)


async def run_events(save: bool = False, *, days: int = 30, team: str | None = None) -> int:
    from src.crawlers.team_event_crawler import TeamEventCrawler

    crawler = TeamEventCrawler(days_back=days)
    events = await crawler.run(save=save, team_filter=team)
    return len(events)


async def run_roster(save: bool = False, *, target_date: str | None = None) -> int:
    from src.crawlers.roster_transaction_crawler import RosterTransactionCrawler

    crawler = RosterTransactionCrawler()
    transactions = await crawler.run(save=save, target_date=target_date)
    return len(transactions)


async def run_ticket(save: bool = False, *, season: int | None = None) -> int:
    from src.crawlers.ticket_crawler import TicketCrawler

    crawler = TicketCrawler()
    prices = await crawler.run(save=save, season=season)
    return len(prices)


async def run_all(
    save: bool = False,
    *,
    days: int = 30,
    team: str | None = None,
    season: int | None = None,
    target_date: str | None = None,
) -> dict[str, int]:
    logger.info("=== P0 data collection ===")
    counts = {
        "events": await run_events(save=save, days=days, team=team),
        "roster": await run_roster(save=save, target_date=target_date),
        "ticket": await run_ticket(save=save, season=season),
    }
    logger.info(f"=== P0 complete: events={counts['events']} roster={counts['roster']} ticket={counts['ticket']} ===")
    return counts


def _normalize_target_date(value: str | None) -> str | None:
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        return None
    for date_format in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(raw, date_format).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Invalid --target-date: {value!r}. Use YYYYMMDD or YYYY-MM-DD.")


async def run_from_args(args: argparse.Namespace) -> dict[str, int]:
    target_date = _normalize_target_date(args.target_date)
    runner_map = {
        "events": lambda: run_events(save=args.save, days=args.days, team=args.team),
        "roster": lambda: run_roster(save=args.save, target_date=target_date),
        "ticket": lambda: run_ticket(save=args.save, season=args.season),
        "all": lambda: run_all(
            save=args.save,
            days=args.days,
            team=args.team,
            season=args.season,
            target_date=target_date,
        ),
    }

    result = await runner_map[args.type]()
    if isinstance(result, dict):
        return result
    return {args.type: int(result)}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="P0 Data Collection (events + roster + ticket)")
    parser.add_argument(
        "--type",
        choices=["events", "roster", "ticket", "all"],
        default="all",
        help="Data type to collect",
    )
    parser.add_argument("--save", action="store_true", help="Save results to database")
    parser.add_argument("--days", type=int, default=30, help="Days back to crawl for team events")
    parser.add_argument("--team", type=str, default=None, help="Team code filter for team events (e.g. LG)")
    parser.add_argument("--season", type=int, default=None, help="Season year for ticket prices")
    parser.add_argument("--target-date", type=str, default=None, help="Roster target date (YYYYMMDD or YYYY-MM-DD)")
    return parser


def main(argv: Sequence[str] | None = None) -> dict[str, int]:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    return asyncio.run(run_from_args(args))


if __name__ == "__main__":
    main()
