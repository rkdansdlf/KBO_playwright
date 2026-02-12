
import sys
import os
import asyncio

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


from src.crawlers.game_detail_crawler import GameDetailCrawler

async def verify_fix(game_id):
    print(f"🕵️‍♀️ Verifying fix for {game_id}...")
    crawler = GameDetailCrawler()
    
    # 1. Crawl
    data = await crawler.crawl_game(game_id, game_id[:8])
    if not data:
        print("❌ Crawl returned None")
        return

    print("✅ Crawl Data Retrieved")
    # Check sample hitter stats
    home_hitters = data['hitters']['home']
    sample_hitter = home_hitters[0] if home_hitters else None
    if sample_hitter:
        print(f"   Sample Hitter: {sample_hitter['player_name']}")
        print(f"   Stats: {sample_hitter['stats']}")
        if sample_hitter['stats'].get('runs') is not None:
             print(f"   Runs: {sample_hitter['stats']['runs']} (Expected > 0 for some players)")
        else:
             print("   ❌ 'runs' key missing in stats!")
    
    # 2. Save to DB (Optional, but good to verify DB write)
    # We need to use Repository logic or simple insert.
    # Actually, let's just inspect the crawled data first. 
    # If crawled data has Runs, the DB save should work (assuming schema is fine).
    
    # Let's count total runs in crawled data
    total_runs = sum(h['stats'].get('runs', 0) or 0 for h in data['hitters']['home'])
    print(f"   Total Home Runs parsed: {total_runs}")
    
    total_runs_away = sum(h['stats'].get('runs', 0) or 0 for h in data['hitters']['away'])
    print(f"   Total Away Runs parsed: {total_runs_away}")
    
    expected_home = data['teams']['home']['score']
    expected_away = data['teams']['away']['score']
    
    print(f"   Scoreboard: Home={expected_home}, Away={expected_away}")
    
    if total_runs == expected_home and total_runs_away == expected_away:
         print("✅ MATCH! verification successful.")
    else:
         print(f"❌ MISMATCH! Parsed Totals ({total_runs_away}-{total_runs}) != Scoreboard ({expected_away}-{expected_home})")

if __name__ == "__main__":
    game_id = "20180503WONC0"
    asyncio.run(verify_fix(game_id))
