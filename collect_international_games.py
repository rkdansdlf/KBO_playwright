
import asyncio
import argparse
from sqlalchemy.orm import Session
from src.db.engine import SessionLocal
from src.models.game import Game
from src.crawlers.international_crawler import InternationalScheduleCrawler

# List of target international URLs
# Ideally these would be dynamic, but for now we target the user's specific request (Premier 2024)
TARGET_URLS = [
    "https://www.koreabaseball.com/Schedule/International/Etc/Premier2024.aspx",
    # Add more if needed (e.g. APBC 2023)
    # "https://www.koreabaseball.com/Schedule/International/Etc/APBC2023.aspx"
]

async def collect_international_games(save: bool = False):
    crawler = InternationalScheduleCrawler()
    total_games = []
    
    try:
        for url in TARGET_URLS:
            games = await crawler.crawl_schedule(url)
            total_games.extend(games)
            
        print(f"\n📊 Collected {len(total_games)} total international games.")
        
        if save:
            save_games(total_games)
            
    finally:
        await crawler.close()

def save_games(games_data: list):
    session = SessionLocal()
    try:
        saved_count = 0
        for data in games_data:
            # Upsert logic
            game_id = data['game_id']
            existing = session.query(Game).filter_by(game_id=game_id).first()
            
            if existing:
                # Update scores
                existing.away_score = data['away_score']
                existing.home_score = data['home_score']
                existing.status = data['status']
                existing.stadium = data['stadium']
                # Don't overwrite created_at
            else:
                # Create new
                new_game = Game(
                    game_id=game_id,
                    season_id=data['season_id'], # Ensure this aligns with models
                    game_date=data['game_date'],
                    # game_time is not in Game model (usually in metadata)
                    home_team=data['home_team'],
                    away_team=data['away_team'],
                    stadium=data['stadium'],
                    away_score=data['away_score'],
                    home_score=data['home_score'],
                    # status is not in Game model
                    # doubleheader is not in Game model based on view_file (it has 'doubleheader'?? No checks line 21-46. No doubleheader column shown in view_file output!)
                    # Wait, let me check view_file output again.
                    # Lines 23-35: id, game_id, game_date, stadium, home_team, away_team, home_score, away_score...
                    # winning_team, winning_score, season_id.
                    # NO doubleheader column!
                    # So I must remove doubleheader too.
                )
                session.add(new_game)
                saved_count += 1
                
        session.commit()
        print(f"✅ Saved/Updated {len(games_data)} games ({saved_count} new) to database.")
        
    except Exception as e:
        session.rollback()
        print(f"❌ Error saving games: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect International KBO Games")
    parser.add_argument("--save", action="store_true", help="Save collected games to SQLite DB")
    args = parser.parse_args()
    
    asyncio.run(collect_international_games(save=args.save))
