import sys
import os
import argparse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Add project root to path
sys.path.append(os.getcwd())

from src.db.engine import SessionLocal

def verify_season(year):
    print(f"\n" + "="*50)
    print(f"🕵️‍♂️ Verifying Season: {year}")
    print("="*50)
    
    with SessionLocal() as session:
        # 1. Total Games
        total_games = session.execute(text(f"SELECT COUNT(*) FROM game WHERE season_id = {year}")).scalar()
        print(f"📊 Total Games: {total_games}")
        
        # 2. Games with Scores
        scored_games = session.execute(text(f"SELECT COUNT(*) FROM game WHERE season_id = {year} AND home_score IS NOT NULL")).scalar()
        print(f"✅ Scored Games: {scored_games}")
        
        if total_games == 0:
            print("❌ No games found!")
            return

        # 3. Batting Stats Count
        batting_stats = session.execute(text(f"SELECT COUNT(*) FROM game_batting_stats WHERE game_id LIKE '{year}%'")).scalar()
        print(f"🏏 Batting Stats Records: {batting_stats}")
        
        # 4. Pitching Stats Count
        pitching_stats = session.execute(text(f"SELECT COUNT(*) FROM game_pitching_stats WHERE game_id LIKE '{year}%'")).scalar()
        print(f"⚾ Pitching Stats Records: {pitching_stats}")
        
        # 5. Missing Detail Games
        # Identify games that have a record in `game` but no batting stats
        missing_details = session.execute(text(f"""
            SELECT game_id FROM game 
            WHERE season_id = {year} 
            AND game_id NOT IN (SELECT DISTINCT game_id FROM game_batting_stats)
        """)).fetchall()
        
        missing_count = len(missing_details)
        print(f"⚠️  Games Missing Details: {missing_count}")
        if missing_count > 0:
            print(f"    Sample Missing IDs: {[r[0] for r in missing_details[:5]]}")

        # 6. Team Participation Check
        print("\n🏆 Games per Team (Home + Away):")
        team_counts = session.execute(text(f"""
            SELECT team, COUNT(*) as cnt FROM (
                SELECT home_team as team FROM game WHERE season_id = {year}
                UNION ALL
                SELECT away_team as team FROM game WHERE season_id = {year}
            ) t GROUP BY team ORDER BY cnt DESC
        """)).fetchall()
        
        for team, count in team_counts:
            print(f"   - {team}: {count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify KBO game backfill data")
    parser.add_argument("year", type=int, help="Year to verify")
    args = parser.parse_args()
    
    verify_season(args.year)
