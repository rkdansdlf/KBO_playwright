
import asyncio
import os
import sys

# Ensure project root is in path
sys.path.append(os.getcwd())

from sqlalchemy import select, update
from src.db.engine import SessionLocal
from src.models.franchise import Franchise
from src.models.team import Team
from src.models.team_history import TeamHistory
from src.crawlers.team_info_crawler import TeamInfoCrawler
from src.crawlers.team_history_crawler import TeamHistoryCrawler
from src.utils.team_codes import resolve_team_code

async def main():
    print("🚀 Starting Team Data Quality Improvement Crawl...")
    
    # 1. Crawl Admin Info (TeamInfoCrawler)
    info_crawler = TeamInfoCrawler()
    try:
        info_data = await info_crawler.crawl()
        await info_crawler.close()
        
        # Save Info Data
        if info_data:
            print(f"💾 Updating Franchise Metadata for {len(info_data)} teams...")
            with SessionLocal() as session:
                for item in info_data:
                    # Find Franchise by name (Assuming 'name' column in Franchise matches or contains crawled name)
                    # "Samsung Lions" -> "Samsung"
                    # Current Franchise names in DB are Korean? e.g. "삼성 라이온즈"
                    # Crawled name is "삼성 라이온즈"
                    # Perfect match likely.
                    
                    stmt = select(Franchise).where(Franchise.name == item['name'])
                    result = session.execute(stmt).scalars().first()
                    
                    if not result:
                        # Try partial match
                        stmt = select(Franchise).where(Franchise.name.like(f"%{item['name']}%"))
                        result = session.execute(stmt).scalars().first()
                        
                    if result:
                        meta = result.metadata_json or {}
                        meta.update({
                            "found_year": item["found_year"],
                            "owner": item["owner"],
                            "ceo": item["ceo"],
                            "address": item["address"],
                            "phone": item["phone"]
                        })
                        result.metadata_json = meta
                        result.web_url = item["homepage"]
                        session.add(result)
                        print(f"   ✅ Updated {result.name}")
                    else:
                        print(f"   ⚠️ Franchise not found for {item['name']}")
                session.commit()
    except Exception as e:
        print(f"❌ TeamInfoCrawler Failed: {e}")
        if info_crawler: await info_crawler.close()

    # 2. Crawl History (TeamHistoryCrawler)
    hist_crawler = TeamHistoryCrawler()
    try:
        history_data = await hist_crawler.crawl()
        await hist_crawler.close()
        
        # Save History Data
        if history_data:
            print(f"💾 Saving {len(history_data)} Team History records...")
            with SessionLocal() as session:
                # Cache team codes to franchise_id map
                # We need to look up TEAMS table to find franchise_id for a given team_code.
                
                # Fetch all teams
                teams = session.execute(select(Team)).scalars().all()
                team_map = {t.team_id: t.franchise_id for t in teams} # code -> franchise_id
                
                saved_count = 0
                for entry in history_data:
                    team_name = entry['team_name']
                    season = entry['season']
                    
                    # 1. Resolve Code
                    code = resolve_team_code(team_name)
                    if not code:
                        print(f"   ⚠️ Could not resolve code for '{team_name}' ({season})")
                        continue
                        
                    # 2. Resolve Franchise ID
                    franchise_id = team_map.get(code)
                    if not franchise_id:
                         # Try fallback? Or maybe data seeding missing specific code?
                         # e.g. "SAM" might not be in 'teams' table if Seed Data is old?
                         # Currently Seed Data has most.
                         print(f"   ⚠️ No Team record/Franchise ID for code '{code}'")
                         continue

                    # 3. Upsert Logic
                    # Check if history exists for this season + franchise (or season + code?)
                    # History is snapshot. One entry per season per franchise? 
                    # Yes.
                    
                    stmt = select(TeamHistory).where(
                        TeamHistory.season == season,
                        TeamHistory.team_code == code
                    )
                    existing = session.execute(stmt).scalars().first()
                    
                    if existing:
                        existing.team_name = team_name
                        existing.logo_url = entry['logo_url']
                        existing.ranking = entry['ranking']
                        existing.franchise_id = franchise_id 
                        # Update other fields?
                    else:
                        new_hist = TeamHistory(
                            season=season,
                            team_code=code,
                            team_name=team_name,
                            logo_url=entry['logo_url'],
                            ranking=entry['ranking'],
                            franchise_id=franchise_id
                        )
                        session.add(new_hist)
                    saved_count += 1
                
                session.commit()
                print(f"✅ Saved/Updated {saved_count} history records.")

    except Exception as e:
        print(f"❌ TeamHistoryCrawler Failed: {e}")
        import traceback
        traceback.print_exc()
        if hist_crawler: await hist_crawler.close()

if __name__ == "__main__":
    asyncio.run(main())
