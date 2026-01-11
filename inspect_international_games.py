
from src.db.engine import SessionLocal
from src.models.game import Game

def inspect_international():
    session = SessionLocal()
    try:
        # Query international games (season_id ends in 90, e.g. 202490)
        games = session.query(Game).filter(Game.season_id == 202490).all()
        print(f"📊 Found {len(games)} International Games in DB")
        
        for g in games[:10]:
            print(f"[{g.game_date}] {g.away_team} vs {g.home_team} | Score: {g.away_score}-{g.home_score} @ {g.stadium} | ID: {g.game_id}")
            
    finally:
        session.close()

if __name__ == "__main__":
    inspect_international()
