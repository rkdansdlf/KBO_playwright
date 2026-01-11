"""
KBO Game Data Collector (Schedule + Detail + Relay)
"""
from __future__ import annotations
import argparse
import asyncio
from datetime import datetime
from typing import Sequence

from src.crawlers.schedule_crawler import ScheduleCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.relay_crawler import RelayCrawler
from src.repositories.game_repository import save_game_detail, save_relay_data
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
    inputs = [{"game_id": g["game_id"], "game_date": g["game_date"]} for g in games]
    details = await detail_crawler.crawl_games(inputs)

    relay_crawler = RelayCrawler(request_delay=args.delay)
    success_count = 0
    for detail in details:
        game_id = detail['game_id']
        game_date = detail.get('game_date', game_id[:8]).replace('-', '')
        
        if save_game_detail(detail):
            print(f"[DB] Saved Detail for Game {game_id}.")
            if args.relay:
                print(f"[INFO] Fetching Relay for {game_id}...")
                relay_data = await relay_crawler.crawl_game_relay(game_id, game_date)
                if relay_data and 'innings' in relay_data:
                    flat_events = []
                    seq = 1
                    for inning in relay_data['innings']:
                        for play in inning.get('plays', []):
                            play['inning'], play['inning_half'] = inning['inning'], inning['half']
                            play['event_seq'] = seq
                            seq += 1
                            flat_events.append(play)
                    pbp_count = save_relay_data(game_id, flat_events)
                    print(f"[DB] Saved {pbp_count} PBP events for {game_id}.")
            success_count += 1
        else:
            print(f"[ERROR] Failed to save Game {game_id}.")

    print(f"\n[FINISH] Pipeline finished: {success_count}/{len(games)} games processed.")

def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="KBO Full Data Pipeline")
    parser.add_argument("--year", type=int, required=True, help="Year (e.g. 2024)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    parser.add_argument("--limit", type=int, help="Limit number of games for testing")
    parser.add_argument("--relay", action="store_true", help="Include Play-by-Play data")
    parser.add_argument("--delay", type=float, default=1.0, help="Request delay")
    args = parser.parse_args(argv)
    asyncio.run(run_pipeline(args))

if __name__ == "__main__":
    main()
