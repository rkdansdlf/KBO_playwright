"""Debug Futures crawler to see what data is being extracted."""
import asyncio
from src.crawlers.futures import FuturesProfileCrawler
from src.parsers.futures_stats_parser import parse_futures_tables
from src.utils.safe_print import safe_print as print

async def main():
    crawler = FuturesProfileCrawler(request_delay=2)

    # Test with player 51868 (one of the processed players)
    player_id = "51868"
    print(f"Fetching Futures data for player {player_id}...")

    payload = await crawler.fetch_player_futures(player_id)

    print(f"\nPlayer ID: {payload.get('player_id')}")
    print(f"Profile text present: {bool(payload.get('profile_text'))}")
    print(f"Number of tables: {len(payload.get('tables', []))}")

    tables = payload.get('tables', [])
    for i, table in enumerate(tables):
        print(f"\n--- Table {i+1} ---")
        print(f"Table Type Marker: {table.get('_table_type')}")
        print(f"Caption: {table.get('caption')}")
        print(f"Summary: {table.get('summary')}")
        print(f"Headers: {table.get('headers')}")
        print(f"Rows: {len(table.get('rows', []))}")
        if table.get('rows'):
            print(f"First row sample: {table.get('rows')[0][:5] if table.get('rows')[0] else 'empty'}")

    # Try parsing
    print("\n\n=== PARSING RESULTS ===")
    stats = parse_futures_tables(tables)
    print(f"Batting records: {len(stats.get('batting', []))}")
    print(f"Pitching records: {len(stats.get('pitching', []))}")

    if stats.get('batting'):
        print("\nBatting sample:")
        for record in stats['batting'][:2]:
            print(f"  {record}")
    else:
        print("\nNo batting records parsed. Checking classifier...")
        from src.parsers.futures_stats_parser import _classify_tables
        h_tables, p_tables = _classify_tables(tables)
        print(f"Classified as hitters: {len(h_tables)}")
        print(f"Classified as pitchers: {len(p_tables)}")
        if h_tables:
            print("\nFirst hitter table headers:")
            print(f"  {h_tables[0].get('headers')}")
            print("First 2 rows:")
            for row in h_tables[0].get('rows', [])[:2]:
                print(f"  {row}")

    if stats.get('pitching'):
        print("\nPitching sample:")
        for record in stats['pitching'][:2]:
            print(f"  {record}")

if __name__ == "__main__":
    asyncio.run(main())
