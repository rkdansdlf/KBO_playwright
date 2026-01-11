
from src.db.engine import SessionLocal
from src.models.season import KboSeason

def check_seasons():
    session = SessionLocal()
    try:
        seasons = session.query(KboSeason).all()
        print(f"📊 Found {len(seasons)} seasons in DB:")
        for s in seasons:
            print(f"  ID: {s.season_id} | Year: {s.season_year} | Type: {s.league_type_code} ({s.league_type_name})")
    finally:
        session.close()

if __name__ == "__main__":
    check_seasons()
