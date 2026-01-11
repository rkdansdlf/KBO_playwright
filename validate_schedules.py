
from src.db.engine import SessionLocal
from src.models.game import Game
from src.models.team import Team
from sqlalchemy import func

def validate_schedules():
    session = SessionLocal()
    try:
        total_games = session.query(Game).count()
        print(f"📊 Total Games in Table: {total_games}")

        # 1. Null/Empty Check
        null_home = session.query(Game).filter((Game.home_team == None) | (Game.home_team == '')).count()
        null_away = session.query(Game).filter((Game.away_team == None) | (Game.away_team == '')).count()
        null_date = session.query(Game).filter(Game.game_date == None).count()
        null_id = session.query(Game).filter(Game.game_id == None).count()
        null_season = session.query(Game).filter(Game.season_id == None).count()

        print("\n🔍 Null/Empty Value Check:")
        print(f"   - Null Home Team: {null_home}")
        print(f"   - Null Away Team: {null_away}")
        print(f"   - Null Game Date: {null_date}")
        print(f"   - Null Game ID: {null_id}")
        print(f"   - Null Season ID: {null_season}")

        # 2. Duplicate Check
        dups = session.query(Game.game_id, func.count(Game.game_id)).group_by(Game.game_id).having(func.count(Game.game_id) > 1).all()
        print(f"\n🔍 Duplicate Game IDs: {len(dups)}")
        for d in dups[:5]:
            print(f"   - {d[0]}: {d[1]} times")
        if len(dups) > 5:
            print("   ...")

        # 3. Team Code Validity Check
        valid_team_codes = {t.team_id for t in session.query(Team).all()}
        print(f"\n🔍 Valid Team Codes in DB: {sorted(valid_team_codes)}")

        all_home_codes = session.query(Game.home_team).distinct().all()
        all_away_codes = session.query(Game.away_team).distinct().all()
        used_codes = {c[0] for c in all_home_codes} | {c[0] for c in all_away_codes}
        
        invalid_codes = used_codes - valid_team_codes
        print(f"\n🔍 Team Code Consistency Check:")
        if not invalid_codes:
            print("   ✅ All used team codes exist in the teams table.")
        else:
            print(f"   ❌ Found {len(invalid_codes)} invalid team codes:")
            for code in sorted(invalid_codes):
                count = session.query(Game).filter((Game.home_team == code) | (Game.away_team == code)).count()
                print(f"     - '{code}': {count} games")

        # 4. Historical Check (Sample)
        # Check SK vs SSG transition in 2021
        sk_count = session.query(Game).filter(((Game.home_team == 'SK') | (Game.away_team == 'SK'))).count()
        ssg_count = session.query(Game).filter(((Game.home_team == 'SSG') | (Game.away_team == 'SSG'))).count()
        print(f"\n🔍 Historical Brand Check:")
        print(f"   - 'SK' games: {sk_count}")
        print(f"   - 'SSG' games: {ssg_count}")
        
        nx_count = session.query(Game).filter(((Game.home_team == 'NX') | (Game.away_team == 'NX'))).count()
        ki_count = session.query(Game).filter(((Game.home_team == 'KI') | (Game.away_team == 'KI'))).count()
        wo_count = session.query(Game).filter(((Game.home_team == 'WO') | (Game.away_team == 'WO'))).count()
        print(f"   - 'NX' (Nexen) games: {nx_count}")
        print(f"   - 'KI' (Kiwoom ID) games: {ki_count}")
        print(f"   - 'WO' (Heroes/Kiwoom) games: {wo_count}")

    finally:
        session.close()

if __name__ == "__main__":
    validate_schedules()
