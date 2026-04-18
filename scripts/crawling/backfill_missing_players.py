#!/usr/bin/env python3
"""
Backfill missing player profiles in player_basic.
Identifies players in player_season_batting who are missing from player_basic,
and uses PlayerProfileCrawler to fetch and save their data.
"""
import asyncio
import sys
from pathlib import Path
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.player_profile_crawler import PlayerProfileCrawler
from src.db.engine import SessionLocal
from src.models.player import PlayerBasic

async def backfill_players():
    print("Finding players in stats missing from player_basic...")
    
    with SessionLocal() as session:
        # Get missing IDs
        query = text("""
            SELECT DISTINCT b.player_id, b.team_code 
            FROM player_season_batting b 
            LEFT JOIN player_basic p ON b.player_id = p.player_id 
            WHERE p.player_id IS NULL
        """)
        missing = session.execute(query).fetchall()
        
    if not missing:
        print("No missing players found.")
        return

    print(f"Found {len(missing)} missing players. Starting crawl...")
    
    crawler = PlayerProfileCrawler()
    
    for p_id, team_code in missing:
        str_id = str(p_id)
        print(f"📡 Crawling ID {str_id} (Team: {team_code})...")
        try:
            profile = await crawler.crawl_player_profile(str_id)
            if not profile:
                print(f"⚠️  Could not fetch profile for {str_id}")
                continue
                
            print(f"✅ Found: {profile.get('name') or str_id}")
            
            with SessionLocal() as session:
                new_player = PlayerBasic(
                    player_id=p_id,
                    name=profile.get("name") or "Unknown",
                    team=team_code, # Use the team from stats as fallback
                    photo_url=profile.get("photo_url"),
                    bats=profile.get("bats"),
                    throws=profile.get("throws"),
                    debut_year=profile.get("debut_year"),
                    salary_original=profile.get("salary_original"),
                    signing_bonus_original=profile.get("signing_bonus_original"),
                    draft_info=profile.get("draft_info"),
                    status="heuristic" # Mark as found via backfill
                )
                session.add(new_player)
                session.commit()
                print(f"💾 Saved {str_id} to player_basic")
                
        except Exception as e:
            print(f"❌ Error crawling {str_id}: {e}")
            
        # Small delay to be polite
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(backfill_players())
