
import asyncio
from src.crawlers.schedule_crawler import ScheduleCrawler
from src.repositories.game_repository import save_schedule_game
from src.db.engine import SessionLocal
from src.models.season import KboSeason

def populate_seasons(year: int):
    """Ensure KBO seasons exist for the given year and types."""
    # Define mapping: (suffix, code, name)
    # Using simple suffixes: 0=Regular, 1=Exhibition, 5=Postseason (Generic)
    types = [
        (0, 0, "Regular Season"),
        (1, 1, "Exhibition"),
        (5, 5, "Postseason") 
    ]
    
    session = SessionLocal()
    try:
        for suffix, type_code, type_name in types:
            season_id = year * 10 + suffix
            exists = session.query(KboSeason).filter_by(season_id=season_id).first()
            if not exists:
                s = KboSeason(
                    season_id=season_id,
                    season_year=year,
                    league_type_code=type_code,
                    league_type_name=type_name
                )
                session.add(s)
                print(f"   [DB] Created Season: {season_id} ({type_name})")
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"   [DB] Error populating seasons for {year}: {e}")
    finally:
        session.close()

async def collect_all_schedules():
    crawler = ScheduleCrawler(request_delay=0.8)
    
    # Series config: (Dropdown Value, Internal Suffix)
    # 1: Exhibition -> suffix 1
    # 0,9,6: Regular -> suffix 0
    # 3,4,5,7: Postseason -> suffix 5
    series_config = [
        ("1", 1, "Exhibition"),
        ("0,9,6", 0, "Regular"),
        ("3,4,5,7", 5, "Postseason")
    ]
    
    start_year = 2025
    end_year = 2018
    # Optimize: Month ranges. Exhibition(3), Regular(3-10), Post(10-11)
    # But safe to iterate 3-11 for all.
    months = range(3, 12) 
    
    print(f"🚀 Starting Schedule Collection (With Series Types): {start_year} -> {end_year}")
    
    total_saved = 0
    
    for year in range(start_year, end_year - 1, -1):
        print(f"\n📅 Processing Season {year}...")
        
        # 1. Ensure DB has Season IDs
        populate_seasons(year)
        
        for series_code, suffix, series_name in series_config:
            # Generate expected season_id
            season_id_db = year * 10 + suffix
            print(f"   👉 Crawling {series_name} (Code: {series_code}) -> SeasonID: {season_id_db}")
            
            for month in months:
                try:
                    # Pass series_id to crawler
                    games = await crawler.crawl_schedule(year, month, series_id=series_code)
                    if not games:
                        continue
                    
                    saved_count = 0
                    for game in games:
                        # CRITICAL: Overwrite season_year with our composite season_id
                        # game_repository uses 'season_year' key to populate game.season_id
                        game['season_year'] = season_id_db 
                        
                        if save_schedule_game(game):
                            saved_count += 1
                    
                    if saved_count > 0:
                        print(f"      ✅ {series_name} {year}-{month:02d}: Saved {saved_count} games")
                    total_saved += saved_count
                    
                except Exception as e:
                    print(f"      ❌ Error {year}-{month:02d}: {e}")
                
    print(f"\n✨ Completed! Total games saved: {total_saved}")

if __name__ == "__main__":
    asyncio.run(collect_all_schedules())
