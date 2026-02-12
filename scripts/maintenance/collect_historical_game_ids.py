import asyncio
import json
import os
from datetime import datetime
from src.crawlers.schedule_crawler import ScheduleCrawler

async def collect_historical_game_ids(start_year: int, end_year: int):
    crawler = ScheduleCrawler(request_delay=1.0)
    all_games = []
    
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "historical_game_ids.json")
    
    # Load existing if any
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r') as f:
                all_games = json.load(f)
            print(f"Loaded {len(all_games)} existing game IDs.")
        except:
            pass

    existing_ids = {g['game_id'] for g in all_games if g.get('game_id')}

    for year in range(start_year, end_year + 1):
        # Most KBO seasons run from April to October
        # 2001 specifically started in April.
        # Regular Season series_id is typically "0,9,6"
        series_id = "0,9,6" 
        for month in range(4, 11):
            print(f"---> Processing {year}-{month:02d}")
            try:
                month_games = await crawler.crawl_schedule(year, month, series_id=series_id)
                
                new_count = 0
                for g in month_games:
                    if g['game_id'] not in existing_ids:
                        all_games.append(g)
                        existing_ids.add(g['game_id'])
                        new_count += 1
                
                print(f"   Fetched {len(month_games)} games. New: {new_count}")
                
                # Intermediate save
                with open(output_file, 'w') as f:
                    json.dump(all_games, f, indent=2, ensure_ascii=False)
                    
            except Exception as e:
                print(f"❌ Error for {year}-{month}: {e}")
            
            # Tiny sleep between months
            await asyncio.sleep(0.5)

    print(f"✅ Total collected games: {len(all_games)}")
    print(f"💾 Saved to {output_file}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=2001)
    parser.add_argument("--end", type=int, default=2009)
    args = parser.parse_args()
    
    asyncio.run(collect_historical_game_ids(args.start, args.end))
