
from src.db.engine import Engine
from sqlalchemy import text

def repair_schema():
    # Tables that need to be dropped and recreated
    tables_to_drop = [
        "game_summary",
        "box_score",
        "game_play_by_play",
        "game_metadata",
        "game_inning_scores",
        "game_lineups",
        "game_batting_stats",
        "game_pitching_stats",
        "game_events"
    ]
    
    with Engine.connect() as conn:
        print("🗑️ Dropping malformed tables...")
        for table in tables_to_drop:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
                print(f"   - Dropped {table}")
            except Exception as e:
                print(f"   ⚠️ Error dropping {table}: {e}")
        conn.commit()

    print("\n✅ Tables dropped. Now run 'init_db.py' to recreate them correctly.")

if __name__ == "__main__":
    repair_schema()
