import sys
import os
from sqlalchemy import func, text


# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.db.engine import get_db_session
from src.models.game import Game, GameBattingStat, GamePitchingStat
from src.models.player import PlayerBasic

def audit_quality():
    print("🔍 Auditing KBO Data Quality (2001-2010)...")
    
    with get_db_session() as session:
        # 1. Game counts by year
        print("\n--- Game Counts by Season ---")
        game_counts = session.query(
            func.strftime('%Y', Game.game_date).label('year'),
            func.count(Game.id)
        ).filter(
            func.strftime('%Y', Game.game_date).between('2001', '2010')
        ).group_by('year').order_by('year').all()
        
        print(f"{'Year':<10} | {'Game Count':<10}")
        print("-" * 23)
        for year, count in game_counts:
            print(f"{year:<10} | {count:<10}")

        # 2. Batting Stats NULL Player IDs
        print("\n--- Batting Stats: Unresolved Player IDs ---")
        batting_nulls = session.query(
            func.strftime('%Y', Game.game_date).label('year'),
            func.count(GameBattingStat.id).label('total'),
            func.sum(text("CASE WHEN player_id IS NULL OR player_id = 0 THEN 1 ELSE 0 END")).label('nulls')
        ).join(Game, Game.game_id == GameBattingStat.game_id)\
         .filter(func.strftime('%Y', Game.game_date).between('2001', '2010'))\
         .group_by('year').order_by('year').all()
        
        print(f"{'Year':<10} | {'Total Rows':<12} | {'NULL IDs':<10} | {'NULL %':<10}")
        print("-" * 55)
        for year, total, nulls in batting_nulls:
            pct = (nulls / total * 100) if total > 0 else 0
            print(f"{year:<10} | {total:<12} | {nulls:<10} | {pct:.2f}%")

        # 3. Pitching Stats NULL Player IDs
        print("\n--- Pitching Stats: Unresolved Player IDs ---")
        pitching_nulls = session.query(
            func.strftime('%Y', Game.game_date).label('year'),
            func.count(GamePitchingStat.id).label('total'),
            func.sum(text("CASE WHEN player_id IS NULL OR player_id = 0 THEN 1 ELSE 0 END")).label('nulls')
        ).join(Game, Game.game_id == GamePitchingStat.game_id)\
         .filter(func.strftime('%Y', Game.game_date).between('2001', '2010'))\
         .group_by('year').order_by('year').all()
        
        print(f"{'Year':<10} | {'Total Rows':<12} | {'NULL IDs':<10} | {'NULL %':<10}")
        print("-" * 55)
        for year, total, nulls in pitching_nulls:
            pct = (nulls / total * 100) if total > 0 else 0
            print(f"{year:<10} | {total:<12} | {nulls:<10} | {pct:.2f}%")

        # 4. Top Unresolved Players
        print("\n--- Top Unresolved Players (Hitters) ---")
        top_unresolved_hitters = session.query(
            GameBattingStat.player_name,
            GameBattingStat.team_code,
            func.count(GameBattingStat.id).label('appearances')
        ).filter(text("player_id IS NULL OR player_id = 0"))\
         .group_by(GameBattingStat.player_name, GameBattingStat.team_code)\
         .order_by(text('appearances DESC'))\
         .limit(15).all()
        
        print(f"{'Name':<15} | {'Team':<10} | {'Apps':<10}")
        print("-" * 40)
        for name, team, apps in top_unresolved_hitters:
            print(f"{name:<15} | {team:<10} | {apps:<10}")

        print("\n--- Top Unresolved Players (Pitchers) ---")
        top_unresolved_pitchers = session.query(
            GamePitchingStat.player_name,
            GamePitchingStat.team_code,
            func.count(GamePitchingStat.id).label('appearances')
        ).filter(text("player_id IS NULL OR player_id = 0"))\
         .group_by(GamePitchingStat.player_name, GamePitchingStat.team_code)\
         .order_by(text('appearances DESC'))\
         .limit(15).all()
        
        print(f"{'Name':<15} | {'Team':<10} | {'Apps':<10}")
        print("-" * 40)
        for name, team, apps in top_unresolved_pitchers:
            print(f"{name:<15} | {team:<10} | {apps:<10}")

if __name__ == "__main__":
    audit_quality()
