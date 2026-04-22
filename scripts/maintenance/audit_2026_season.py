"""
Audit 2026 Season Game Data.
Checks for missing boxscore scores or missing pitcher/hitter stats for all games in the 2026 season.
"""
import os
import sys

# Add project root to path
sys.path.append(os.getcwd())

from src.db.engine import SessionLocal
from src.models.game import Game, GameInningScore, GameBattingStat, GamePitchingStat
from sqlalchemy import func

def audit_2026_season():
    print("📊 Auditing 2026 season data for missing stats...")
    
    with SessionLocal() as session:
        # 1. Get all games for 2026 that are not CANCELLED
        games = session.query(Game).filter(
            Game.game_date.like('2026%'),
            Game.game_status != 'CANCELLED'
        ).all()
        
        print(f"🔍 Total games found for 2026: {len(games)}")
        
        missing_stats_games = []
        
        for game in games:
            game_id = game.game_id
            
            # Check for scores
            has_scores = game.home_score is not None and game.away_score is not None
            
            # Check for child stats
            hitter_count = session.query(func.count(GameBattingStat.id)).filter_by(game_id=game_id).scalar()
            pitcher_count = session.query(func.count(GamePitchingStat.id)).filter_by(game_id=game_id).scalar()
            inning_count = session.query(func.count(GameInningScore.id)).filter_by(game_id=game_id).scalar()
            
            issue_reasons = []
            if not has_scores:
                issue_reasons.append("Missing Score")
            if inning_count == 0:
                issue_reasons.append("Missing Inning Scores")
            if hitter_count == 0:
                issue_reasons.append("Missing Hitter Stats")
            if pitcher_count == 0:
                issue_reasons.append("Missing Pitcher Stats")
            
            if issue_reasons:
                print(f"❌ {game_id} ({game.game_date}): {', '.join(issue_reasons)} | Status: {game.game_status}")
                missing_stats_games.append(game_id)
        
        print(f"\n✅ Audit complete. Found {len(missing_stats_games)} games with missing or incomplete data.")
        
        if missing_stats_games:
            print("\n💡 Recommendation: Run the following command to repair these games:")
            print(f"venv/bin/python3 -m src.cli.run_daily_update --date 20260322...20261030 (as needed)")
            
            # Group by date for easier re-running
            dates = sorted(list(set([gid[:8] for gid in missing_stats_games])))
            print(f"Affected dates: {', '.join(dates)}")

if __name__ == "__main__":
    audit_2026_season()
