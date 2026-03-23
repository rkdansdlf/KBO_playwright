"""
Verify SQLite data integrity with advanced mathematical bounding.
"""
from sqlalchemy import func
from src.db.engine import SessionLocal
from src.models.team import Team, TeamDailyRoster
from src.models.game import Game, GameLineup, GameBattingStat, GamePitchingStat, GameInningScore
from src.utils.safe_print import safe_print as print

def check_data_quality(session):
    """Check for data mathematical and relational quality issues."""
    print("\n🔍 Advanced Data Quality & Math Checks")
    print("=" * 50)

    issues = []

    # Removed schedule query

    # Advanced Game Math Consistency Checks
    print("   - Verifying Box Score Mathematics (Last 50 finished games)...")
    recent_games = session.query(Game).filter(Game.game_status == "종료").order_by(Game.game_date.desc()).limit(50).all()
    for g in recent_games:
        # 1. Inning sum check
        home_innings = session.query(func.sum(GameInningScore.runs)).filter_by(game_id=g.game_id, team_side='home').scalar() or 0
        away_innings = session.query(func.sum(GameInningScore.runs)).filter_by(game_id=g.game_id, team_side='away').scalar() or 0
        
        if home_innings != g.home_score:
            issues.append(f"⚠️ Game {g.game_id} Home Score Mismatch: Board {g.home_score} vs Innings {home_innings}")
        if away_innings != g.away_score:
            issues.append(f"⚠️ Game {g.game_id} Away Score Mismatch: Board {g.away_score} vs Innings {away_innings}")
            
        # 2. Batting stat RBI check
        home_rbi = session.query(func.sum(GameBattingStat.rbi)).filter_by(game_id=g.game_id, team_side='home').scalar() or 0
        away_rbi = session.query(func.sum(GameBattingStat.rbi)).filter_by(game_id=g.game_id, team_side='away').scalar() or 0
        
        # RBI can be <= total runs, but mathematically impossibility requires RBI > Runs
        if home_rbi > g.home_score:
            issues.append(f"⚠️ Game {g.game_id} Home RBI Bound Error: RBI {home_rbi} > Total Runs {g.home_score}")
        if away_rbi > g.away_score:
            issues.append(f"⚠️ Game {g.game_id} Away RBI Bound Error: RBI {away_rbi} > Total Runs {g.away_score}")

    if not issues:
        print("✅ No data quality issues found!")
    else:
        for issue in issues:
            print(issue)

    return len(issues)

def main():
    """Run all verification checks"""
    print("\n" + "🔬" * 30)
    print("Database Structure & Math Verification")
    print("🔬" * 30)

    with SessionLocal() as session:
        try:
            # Stats Logging
            games = session.query(Game).count()
            batting = session.query(GameBattingStat).count()
            pitching = session.query(GamePitchingStat).count()
            
            print(f"📊 Basic Counts: Games ({games}), Batting Rows ({batting}), Pitching Rows ({pitching})")
            
            # Check data quality
            issue_count = check_data_quality(session)

            if issue_count == 0:
                print("\n✅ SQLite data passed mathematical bounds tests.")
            else:
                print("\n⚠️ Please review the mathematical anomalies above.")

        except Exception as e:
            print(f"\n❌ Verification error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()
