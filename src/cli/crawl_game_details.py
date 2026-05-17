"""
KBO Game Data Collector (Schedule + Detail + Relay)
"""
from __future__ import annotations
import argparse
import asyncio
from datetime import datetime
from typing import Sequence
from zoneinfo import ZoneInfo

from src.crawlers.schedule_crawler import ScheduleCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.relay_crawler import RelayCrawler
from src.db.engine import SessionLocal
from src.services.game_collection_service import crawl_and_save_game_details
from src.services.player_id_resolver import PlayerIdResolver
from src.utils.safe_print import safe_print as print
from src.utils.schedule_validation import is_detail_candidate_game
from src.utils.team_codes import normalize_kbo_game_id

KST = ZoneInfo("Asia/Seoul")


def _parse_game_ids(value: str | None) -> list[str]:
    if not value:
        return []
    return [normalize_kbo_game_id(token.strip()) for token in value.split(",") if token.strip()]


async def run_pipeline(args: argparse.Namespace):
    print(f"[INFO] Fetching schedule for {args.year}-{args.month:02d}...")
    sched_crawler = ScheduleCrawler()
    games = await sched_crawler.crawl_schedule(args.year, args.month)
    
    if not games:
        print("[ERROR] No games found for the given period.")
        return

    today_kst = datetime.now(KST).date()
    detail_games = [game for game in games if is_detail_candidate_game(game, today=today_kst)]
    skipped_count = len(games) - len(detail_games)
    if skipped_count:
        print(f"[INFO] Skipping {skipped_count} non-detail schedule games.")

    requested_game_ids = _parse_game_ids(getattr(args, "game_ids", None))
    if requested_game_ids:
        requested_set = set(requested_game_ids)
        detail_games = [
            game
            for game in detail_games
            if normalize_kbo_game_id(str(game.get("game_id") or "")) in requested_set
        ]
        found_set = {normalize_kbo_game_id(str(game.get("game_id") or "")) for game in detail_games}
        missing_ids = sorted(requested_set - found_set)
        if missing_ids:
            print(f"[WARN] Requested game_ids not found in schedule/detail candidates: {','.join(missing_ids)}")

    if args.limit: detail_games = detail_games[:args.limit]
    if not detail_games:
        print("[ERROR] No detail candidates found for the given period.")
        return
    games = detail_games
    print(f"[SUCCESS] Found {len(games)} games. Starting detail collection...")

    resolver_session = SessionLocal()
    try:
        resolver = PlayerIdResolver(
            resolver_session,
            strict_game_resolution=True,
            allow_auto_register=False,
        )
        resolver.preload_season_index(args.year)
        detail_crawler = GameDetailCrawler(request_delay=args.delay, resolver=resolver)
        relay_crawler = RelayCrawler(request_delay=args.delay) if args.relay else None
        result = await crawl_and_save_game_details(
            games,
            detail_crawler=detail_crawler,
            relay_crawler=relay_crawler,
            force=args.force,
            concurrency=args.concurrency,
            log=print,
        )
    finally:
        resolver_session.close()

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
    parser.add_argument("--game-ids", type=str, help="Specific Game IDs to crawl, comma separated")
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
