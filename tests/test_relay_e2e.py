"""
End-to-end test for RELAY crawler.
Tests fetching and saving play-by-play data.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
from src.crawlers.relay_crawler import fetch_and_parse_relay
from src.repositories.relay_repository import save_relay_data, get_game_relay_summary
from src.utils.safe_print import safe_print as print


async def main():
    print("\n=== RELAY Crawler E2E Test ===\n")

    # Use a recent game with completed data
    game_id = "20251013SKSS0"
    game_date = "20251013"

    # Step 1: Crawl RELAY section
    print(f"Step 1: Crawling RELAY for game {game_id}...")
    relay_data = await fetch_and_parse_relay(game_id, game_date)

    if not relay_data:
        print("No RELAY data returned. Test failed.")
        return

    innings = relay_data.get('innings', [])
    print(f"Parsed {len(innings)} innings\n")

    # Show sample
    if innings:
        print("Sample inning data:")
        sample = innings[0]
        print(f"  Inning {sample['inning']} {sample['half']}")
        print(f"  Plays: {len(sample['plays'])}")
        if sample['plays']:
            print(f"  First play: {sample['plays'][0].get('description', '')[:50]}...")
    print()

    # Step 2: Save to database
    print(f"Step 2: Saving RELAY data to database...")
    saved = save_relay_data(game_id, innings)
    print(f"Saved {saved} plays\n")

    # Step 3: Verify
    print("Step 3: Verifying saved data...")
    summary = get_game_relay_summary(game_id)

    print(f"Game: {summary['game_id']}")
    print(f"Total plays: {summary['total_plays']}")
    print(f"Innings recorded: {summary['innings']}")
    print("\nEvent types:")
    for event_type, count in summary['event_types'].items():
        if count > 0:
            print(f"  {event_type}: {count}")

    print("\n=== Test Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
