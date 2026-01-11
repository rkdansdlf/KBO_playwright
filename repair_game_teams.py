
from src.db.engine import SessionLocal
from src.models.game import Game
from src.utils.team_codes import team_code_from_game_id_segment

def repair_game_teams():
    session = SessionLocal()
    try:
        games = session.query(Game).all()
        print(f"🛠️  Repairing teams for {len(games)} games...")
        
        updated_count = 0
        for game in games:
            # game_id format: YYYYMMDD AWAY HOME SEQ
            # Example: 20250712EAWE0 -> EA (Away), WE (Home)
            # Actually, per KBO standard: YYYYMMDD + AWAY + HOME + SEQ
            # But wait, let's look at a sample: 20250712EAWE0 -> WE (Home), EA (Away)?
            # In my validate script query: 20250712EAWE0|2025-07-12|WE|EA|20250
            # It seems home_team is WE, away_team is EA.
            # Segment format: [0:8] Date, [8:10] Away, [10:12] Home, [12] Seq
            
            gid = game.game_id
            if len(gid) < 12:
                continue
                
            season_year = int(gid[:4])
            away_seg = gid[8:10]
            home_seg = gid[10:12]
            
            new_away = team_code_from_game_id_segment(away_seg, season_year)
            new_home = team_code_from_game_id_segment(home_seg, season_year)
            
            changed = False
            if game.away_team != new_away:
                print(f"   [{gid}] Away: {game.away_team} -> {new_away}")
                game.away_team = new_away
                changed = True
            if game.home_team != new_home:
                print(f"   [{gid}] Home: {game.home_team} -> {new_home}")
                game.home_team = new_home
                changed = True
                
            if changed:
                updated_count += 1
                
        if updated_count > 0:
            session.commit()
            print(f"\n✅ Successfully updated {updated_count} games.")
        else:
            print("\n✨ No changes needed. All team codes are already correct.")
            
    except Exception as e:
        session.rollback()
        print(f"❌ Error during repair: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    repair_game_teams()
