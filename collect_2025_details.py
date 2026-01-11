
import asyncio
import time
from sqlalchemy import text
from src.db.engine import SessionLocal
from src.models.game import Game, GameMetadata
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.relay_crawler import RelayCrawler
from src.repositories.game_repository import save_game_detail, save_relay_data

async def collect_2025_details():
    session = SessionLocal()
    detail_crawler = GameDetailCrawler(request_delay=1.0)
    relay_crawler = RelayCrawler(request_delay=1.0)
    
    try:
        # Get all 2025 games (Regular, Exhibition, Post)
        # season_id: 20250, 20251, 20255
        print("🔍 Querying pending games for 2025...")
        
        # We want games that DO NOT have metadata yet.
        # Efficient query check:
        # SELECT g.game_id 
        # FROM game g 
        # LEFT JOIN game_metadata m ON g.game_id = m.game_id 
        # WHERE g.season_id IN (20250, 20251, 20255) 
        #   AND m.game_id IS NULL
        # ORDER BY g.game_date ASC
        
        query_text = """
            SELECT g.game_id, g.game_date, g.home_team, g.away_team
            FROM game g
            LEFT JOIN game_metadata m ON g.game_id = m.game_id
            WHERE cast(g.season_id as text) LIKE '2025%'
              AND m.game_id IS NULL
            ORDER BY g.game_date ASC
        """
        
        result = session.execute(text(query_text)).fetchall()
        
        print(f"📋 Found {len(result)} games needing detail collection.")
        
        count = 0
        total = len(result)
        
        # Convert row tuples to simple dicts or use directly
        pending_games = [{'game_id': r[0], 'game_date': str(r[1]), 'home': r[2], 'away': r[3]} for r in result]
        
        session.close() # Close session for query
        
        for idx, game in enumerate(pending_games):
            gid = game['game_id']
            gdate = game['game_date']
            
            print(f"[{idx+1}/{total}] Crawling {gid} ({gdate})...")
            
            try:
                # 1. Crawl BoxScore & Stats
                detail_data = await detail_crawler.crawl_game(gid, gdate)
                
                # 2. Crawl Relay (Play-by-play)
                relay_data = await relay_crawler.crawl_game_relay(gid, gdate)
                
                # 3. Save
                if detail_data:
                    # Session inside save_game_detail
                    save_game_detail(detail_data)
                
                if relay_data:
                    # Flatten innings -> events list
                    events = []
                    for inning_data in relay_data.get('innings', []):
                        inn_num = inning_data['inning']
                        half = inning_data['half']
                        for play in inning_data.get('plays', []):
                            evt = play.copy()
                            evt['inning'] = inn_num
                            evt['inning_half'] = half
                            events.append(evt)
                    
                    # Session inside save_relay_data
                    save_relay_data(gid, events)
                    
                if detail_data or relay_data:
                    count += 1
                    print(f"   ✅ Saved {gid}")
                else:
                    print(f"   ⚠️ No data found for {gid}")
                    
            except Exception as e:
                print(f"   ❌ Error processing {gid}: {e}")
            
            # Additional small delay to be polite
            await asyncio.sleep(0.5)
            
    except Exception as e:
        print(f"❌ Critical Error: {e}")
    finally:
        pass

if __name__ == "__main__":
    asyncio.run(collect_2025_details())
