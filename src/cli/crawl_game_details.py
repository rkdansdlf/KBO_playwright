"""
KBO Game Data Collector (Schedule + Detail + Relay)
"""
from __future__ import annotations
import argparse
import asyncio
from typing import Sequence

from src.crawlers.schedule_crawler import ScheduleCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.relay_crawler import RelayCrawler
from src.services.game_collection_service import crawl_and_save_game_details
from src.utils.safe_print import safe_print as print

async def run_pipeline(args: argparse.Namespace):
    print(f"[INFO] Fetching schedule for {args.year}-{args.month:02d}...")
    sched_crawler = ScheduleCrawler()
    games = await sched_crawler.crawl_schedule(args.year, args.month)
    
    if not games:
        print("[ERROR] No games found for the given period.")
        return

    if args.limit: games = games[:args.limit]
    print(f"[SUCCESS] Found {len(games)} games. Starting detail collection...")

    detail_crawler = GameDetailCrawler(request_delay=args.delay)
    relay_crawler = RelayCrawler(request_delay=args.delay) if args.relay else None
    result = await crawl_and_save_game_details(
        games,
        detail_crawler=detail_crawler,
        relay_crawler=relay_crawler,
        force=args.force,
        concurrency=args.concurrency,
        log=print,
    )

    print(
        "\n[FINISH] Pipeline finished: "
        f"detail_saved={result.detail_saved}/{result.detail_targets}, "
        f"detail_failed={result.detail_failed}, "
        f"detail_skipped={result.detail_skipped_existing}, "
        f"relay_games={result.relay_saved_games}, "
        f"relay_rows={result.relay_rows_saved}, "
        f"relay_skipped={result.relay_skipped_existing}."
    )

def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="KBO Full Data Pipeline")
    parser.add_argument("--year", type=int, required=True, help="Year (e.g. 2024)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    parser.add_argument("--limit", type=int, help="Limit number of games for testing")
    parser.add_argument(
        "--relay",
        action="store_true",
        help="Include direct relay fallback rows. Prefer scripts/fetch_kbo_pbp.py for completed-game PBP recovery.",
    )
    parser.add_argument("--delay", type=float, default=1.0, help="Request delay")
    parser.add_argument("--concurrency", type=int, default=None, help="Max concurrent games")
    parser.add_argument("--force", action="store_true", help="Recrawl and overwrite existing detail/relay rows")
    args = parser.parse_args(argv)
    asyncio.run(run_pipeline(args))

if __name__ == "__main__":
    main()
