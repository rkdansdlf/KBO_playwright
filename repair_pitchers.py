
import sys
import os
from sqlalchemy import text
from src.db.engine import SessionLocal
from src.repositories.game_repository import repair_game_parent_from_existing_children

def batch_repair_pitchers():
    session = SessionLocal()
    
    # Find games that have pitching stats but are missing pitcher names in the game table
    query = text("""
        SELECT g.game_id 
        FROM game g
        WHERE g.game_status IN ('정식', '종료', 'COMPLETED')
          AND (g.away_pitcher IS NULL OR g.away_pitcher = '' OR g.home_pitcher IS NULL OR g.home_pitcher = '')
          AND EXISTS (SELECT 1 FROM game_pitching_stats WHERE game_id = g.game_id AND is_starting = 1)
    """)
    targets = session.execute(query).fetchall()
    session.close()

    if not targets:
        print("No games found requiring pitcher repair.")
        return

    print(f"🔧 Found {len(targets)} games to repair starting pitcher info.")
    
    success_count = 0
    for row in targets:
        game_id = row[0]
        if repair_game_parent_from_existing_children(game_id):
            # print(f"  ✅ Repaired {game_id}")
            success_count += 1
        else:
            print(f"  ❌ Failed to repair {game_id}")

    print(f"\n✨ Repair Complete: {success_count}/{len(targets)} games updated.")

if __name__ == "__main__":
    batch_repair_pitchers()
