
import sys
import os
from sqlalchemy import text
from src.db.engine import SessionLocal

def validate_starting_pitchers():
    session = SessionLocal()
    
    # 1. Total games by year
    print("--- Game Summary by Year ---")
    query_summary = text("""
        SELECT strftime('%Y', game_date) as year, 
               COUNT(*) as total,
               SUM(CASE WHEN away_pitcher IS NOT NULL AND away_pitcher != '' THEN 1 ELSE 0 END) as with_away_pitcher,
               SUM(CASE WHEN home_pitcher IS NOT NULL AND home_pitcher != '' THEN 1 ELSE 0 END) as with_home_pitcher
        FROM game
        GROUP BY year
        ORDER BY year DESC
    """)
    rows = session.execute(query_summary).fetchall()
    print(f"{'Year':<6} | {'Total':<6} | {'Away P':<8} | {'Home P':<8}")
    for row in rows:
        print(f"{row[0]:<6} | {row[1]:<6} | {row[2]:<8} | {row[3]:<8}")

    # 2. Inconsistency: Completed games with NO pitcher info in game table but WITH stats in game_pitching_stats
    print("\n--- Inconsistency: Game table missing pitchers but stats exist ---")
    query_inconsistent = text("""
        SELECT g.game_id, g.game_status, 
               (SELECT COUNT(*) FROM game_pitching_stats WHERE game_id = g.game_id AND is_starting = 1) as starter_stats_count
        FROM game g
        WHERE g.game_status IN ('정식', '종료', 'COMPLETED')
          AND (g.away_pitcher IS NULL OR g.away_pitcher = '' OR g.home_pitcher IS NULL OR g.home_pitcher = '')
          AND EXISTS (SELECT 1 FROM game_pitching_stats WHERE game_id = g.game_id)
        LIMIT 10
    """)
    rows = session.execute(query_inconsistent).fetchall()
    if rows:
        print(f"{'Game ID':<15} | {'Status':<10} | {'Starters in Stats'}")
        for row in rows:
            print(f"{row[0]:<15} | {row[1]:<10} | {row[2]}")
    else:
        print("No inconsistencies found between game table and pitching stats for completed games.")

    # 3. Check for recently crawled games (2026)
    print("\n--- Current Season (2026) Status ---")
    query_2026_status = text("""
        SELECT game_status, COUNT(*) as count,
               SUM(CASE WHEN away_pitcher IS NOT NULL AND away_pitcher != '' THEN 1 ELSE 0 END) as with_pitcher
        FROM game
        WHERE game_date LIKE '2026%'
        GROUP BY game_status
    """)
    rows = session.execute(query_2026_status).fetchall()
    print(f"{'Status':<15} | {'Count':<6} | {'With Pitcher'}")
    for row in rows:
        print(f"{str(row[0]):<15} | {row[1]:<6} | {row[2]}")

    session.close()

if __name__ == "__main__":
    validate_starting_pitchers()
