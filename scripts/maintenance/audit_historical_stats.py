
from sqlalchemy import create_engine, text
from src.db.engine import Engine

def main():
    with Engine.connect() as conn:
        print("📊 Historical Stats Audit (1982-2000)")
        print(f"{'Season':<8} {'Batters':<10} {'Pitchers':<10}")
        print("-" * 30)
        
        for year in range(1982, 2001):
            batters = conn.execute(text(f"SELECT COUNT(*) FROM player_season_batting WHERE season = {year}")).scalar()
            pitchers = conn.execute(text(f"SELECT COUNT(*) FROM player_season_pitching WHERE season = {year}")).scalar()
            print(f"{year:<8} {batters:<10} {pitchers:<10}")

if __name__ == "__main__":
    main()
