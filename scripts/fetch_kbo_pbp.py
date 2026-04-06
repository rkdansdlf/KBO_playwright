import argparse
import sys
import os
import time
from typing import List

# Adjust sys.path to run from scripts/ folder easily
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.crawlers.pbp_bs4_crawler import PBPBS4Crawler
from src.repositories.game_repository import save_relay_data
from src.utils.safe_print import safe_print as print

from src.db.engine import SessionLocal
from src.models.game import Game, GameEvent

def main():
    parser = argparse.ArgumentParser(description="KBO Play-by-Play (game_events) Backfill Fetcher (BS4)")
    parser.add_argument("--season", type=int, help="Season year to fetch (e.g. 2024)")
    parser.add_argument("--limit", type=int, help="Limit maximum games to process")
    parser.add_argument("--game-ids", type=str, help="Specific Game IDs to fetch, comma separated (e.g. 20240323SSHH0)")
    parser.add_argument("--dry-run", action="store_true", help="Parse events but do not save to DB")
    parser.add_argument("--missing-only", action="store_true", default=True, help="Skip games that already have events (Default: True)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing events (Disable missing-only)")

    args = parser.parse_args()

    if not args.season and not args.game_ids:
        print("[ERROR] Must provide --season or --game-ids")
        sys.exit(1)

    if args.force:
        args.missing_only = False

    game_ids = []

    # Check if we query from DB
    with SessionLocal() as session:
        if args.game_ids:
            game_ids = [gid.strip() for gid in args.game_ids.split(",") if gid.strip()]
        else:
            query = session.query(Game.game_id).filter(Game.game_id.like(f"{args.season}%"))
            query = query.filter(Game.game_status == 'COMPLETED')
            
            results = query.all()
            game_ids = [r[0] for r in results]

        # Missing-only logic: Filter out game_ids that already have entries in GameEvent
        if args.missing_only and game_ids:
            existing_events = session.query(GameEvent.game_id).filter(GameEvent.game_id.in_(game_ids)).distinct().all()
            existing_set = {r[0] for r in existing_events}
            original_count = len(game_ids)
            game_ids = [gid for gid in game_ids if gid not in existing_set]
            print(f"[INFO] Missing-only mode: Skipped {original_count - len(game_ids)} games already in DB.")

    if not game_ids:
        print("[INFO] No games found to process.")
        return

    if args.limit:
        game_ids = game_ids[:args.limit]

    print(f"[INFO] Total games to process: {len(game_ids)}")
    if args.dry_run:
        print("[WARN] Dry Run mode activated. No data will be saved.")

    crawler = PBPBS4Crawler()

    # Sequential Processing with Delay
    for idx, gid in enumerate(game_ids, start=1):
        print(f"\n[PROGRESS] Processing {idx}/{len(game_ids)}: {gid}")
        
        try:
            res = crawler.crawl_game_events(gid)
            if res and res.get('events'):
                events = res['events']
                if args.dry_run:
                    print(f"[DRY-RUN] Extracted {len(events)} events for {gid}")
                    # Optionally print the first event as a teaser
                    if events:
                         print(f"   Sample Event: {events[0]}")
                else:
                    saved = save_relay_data(gid, events)
                    print(f"[SUCCESS] Saved {saved} events for {gid}")
            else:
                print(f"[SKIP] No events extracted for {gid}")
                
        except Exception as e:
            print(f"[ERROR] Failed to process {gid}: {e}")

        # Rate Limiting
        if idx < len(game_ids):
            time.sleep(1.5)

    print("\n[INFO] Backfill fetching completed.")

if __name__ == "__main__":
    main()
