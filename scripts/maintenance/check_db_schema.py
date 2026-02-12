from src.db.engine import Engine
from sqlalchemy import text

def check_schema():
    with Engine.connect() as conn:
        print("--- Checking Seasons ---")
        seasons = conn.execute(text("SELECT season_id, season_year FROM kbo_seasons ORDER BY season_year")).fetchall()
        print(f"Total {len(seasons)} seasons.")
        print([s.season_year for s in seasons])

        print("\n--- Checking game_inning_scores FKs ---")
        fks = conn.execute(text("PRAGMA foreign_key_list(game_inning_scores)")).fetchall()
        for fk in fks:
            print(fk)

        print("\n--- Checking game FKs ---")
        fks = conn.execute(text("PRAGMA foreign_key_list(game)")).fetchall()
        for fk in fks:
            print(fk)

if __name__ == "__main__":
    check_schema()
