
from src.db.engine import SessionLocal
from src.models.game import Game
from sqlalchemy import text

def check_2025_status():
    session = SessionLocal()
    try:
        # Check games for 2025 (season_id starts with 2025)
        # We used logic: 20250, 20251, 20255
        query = text("SELECT count(*) FROM game WHERE cast(season_id as text) LIKE '2025%'")
        count = session.execute(query).scalar()
        
        print(f"📊 Total 2025 Games in DB: {count}")
        
        # Breakdown by type
        types = {20251: "Exhibition", 20250: "Regular", 20255: "Postseason"}
        for sid, name in types.items():
            c = session.query(Game).filter_by(season_id=sid).count()
            print(f"   - {name} ({sid}): {c}")
            
    finally:
        session.close()

if __name__ == "__main__":
    check_2025_status()
