
import sys
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Add project root to path
sys.path.append(os.getcwd())

from src.db.engine import SessionLocal
from src.models.game import Game, GameMetadata, GameBattingStat, GamePitchingStat, GameEvent

def verify_data():
    session = SessionLocal()
    try:
        game = session.query(Game).filter_by(game_id="20241001LTNC0").first()
        if not game:
            print("❌ Game not found")
            return
        
        print(f"✅ Game found: {game.game_id} ({game.game_date})")
        print(f"   Score: {game.away_team} {game.away_score} : {game.home_score} {game.home_team}")

        metadata = session.query(GameMetadata).filter_by(game_id=game.game_id).first()
        if metadata:
            print(f"✅ Metadata found: Attendance={metadata.attendance}, Time={metadata.game_time_minutes}min")
        else:
            print("❌ Metadata missing")

        batters = session.query(GameBattingStat).filter_by(game_id=game.game_id).count()
        print(f"✅ Batting Stats: {batters} records")

        pitchers = session.query(GamePitchingStat).filter_by(game_id=game.game_id).count()
        print(f"✅ Pitching Stats: {pitchers} records")

        events = session.query(GameEvent).filter_by(game_id=game.game_id).count()
        print(f"✅ Game Events: {events} records")

    finally:
        session.close()

if __name__ == "__main__":
    verify_data()
