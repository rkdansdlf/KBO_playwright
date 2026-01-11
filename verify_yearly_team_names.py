
from sqlalchemy import func
from src.db.engine import SessionLocal
from src.models.game import Game
from src.utils.team_history import iter_team_history
from collections import defaultdict

def verify_yearly_teams():
    session = SessionLocal()
    try:
        # 1. Build Expected Map: Year -> Set of Valid Team Codes
        history = list(iter_team_history())
        expected_teams_by_year = defaultdict(set)
        
        # We collected data from 2018 to 2025
        target_years = range(2018, 2026)
        
        for year in target_years:
            for entry in history:
                end_season = entry.end_season or 9999
                if entry.start_season <= year <= end_season:
                    expected_teams_by_year[year].add(entry.team_code)
            # Add All-Star teams manually as they are not in valid franchise history but valid for games
            expected_teams_by_year[year].add('EA')
            expected_teams_by_year[year].add('WE')
            
        # 2. Query Actual Data: Year -> Set of Used Team Codes
        # Game IDs start with YYYY
        actual_teams_by_year = defaultdict(set)
        
        games = session.query(Game.game_id, Game.home_team, Game.away_team).all()
        for g in games:
            year = int(g.game_id[:4])
            if year in target_years:
                actual_teams_by_year[year].add(g.home_team)
                actual_teams_by_year[year].add(g.away_team)
                
        # 3. Compare and Print
        print("📊 Yearly Team Name Verification (2018-2025)\n")
        
        all_valid = True
        
        for year in sorted(target_years, reverse=True):
            expected = expected_teams_by_year[year]
            actual = actual_teams_by_year[year]
            
            missing = expected - actual
            unexpected = actual - expected
            
            print(f"🗓️  {year} Season")
            print(f"   - Expected: {sorted(list(expected))}")
            print(f"   - Actual:   {sorted(list(actual))}")
            
            if not missing and not unexpected:
                print("   ✅ MATCH")
            else:
                all_valid = False
                if missing:
                    # It's okay to miss 'WO' (Woori) if 'NX' (Nexen) is used, but wait, history defines ranges
                    # Actually, if we miss EA/WE (All-Star) it might be because we didn't crawl ASG or it was cancelled.
                    print(f"   ⚠️  Missing Expected: {sorted(list(missing))}")
                if unexpected:
                    print(f"   ❌ Found Unexpected: {sorted(list(unexpected))}")
            print("-" * 50)
            
        if all_valid:
            print("\n✨ All team names match historical expectations!")
        else:
            print("\n⚠️  Some discrepancies found (Check All-Star games or missing ranges).")

    finally:
        session.close()

if __name__ == "__main__":
    verify_yearly_teams()
