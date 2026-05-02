"""
Reverse Sync: OCI (Production) -> SQLite (Local)
Purpose: Pull missing game records and basic metadata from OCI to local DB to align the baseline.
"""
import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.db.engine import SessionLocal as LocalSession
from src.models.game import Game, GameMetadata

load_dotenv()

def reverse_sync():
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        print("❌ OCI_DB_URL not found.")
        return

    print("🚀 Initializing Reverse Sync (OCI -> Local)...")
    oci_engine = create_engine(oci_url)
    OCI_Session = sessionmaker(bind=oci_engine)
    
    with OCI_Session() as oci_session, LocalSession() as local_session:
        # 1. Find Game IDs missing in Local
        print("🔍 Identifying missing games in Local DB...")
        local_game_ids = set(local_session.execute(text("SELECT game_id FROM game")).scalars().all())
        oci_games = oci_session.query(Game).all()
        
        missing_games = [g for g in oci_games if g.game_id not in local_game_ids]
        print(f"🎯 Found {len(missing_games)} missing games in OCI.")

        if not missing_games:
            print("✅ Local DB is already up to date with OCI game records.")
            return

        # 2. Sync Missing Games
        print(f"🚚 Syncing {len(missing_games)} game records...")
        count = 0
        for g in missing_games:
            # Create a new instance for local session
            new_game = Game(
                game_id=g.game_id,
                game_date=g.game_date,
                home_team=g.home_team,
                away_team=g.away_team,
                home_score=g.home_score,
                away_score=g.away_score,
                game_status=g.game_status,
                season_id=g.season_id,
                stadium=g.stadium,
                away_pitcher=g.away_pitcher,
                home_pitcher=g.home_pitcher,
                winning_team=g.winning_team,
                winning_score=g.winning_score,
                is_primary=g.is_primary,
                home_franchise_id=g.home_franchise_id,
                away_franchise_id=g.away_franchise_id,
                winning_franchise_id=g.winning_franchise_id
            )
            local_session.add(new_game)
            count += 1
            if count % 500 == 0:
                local_session.commit()
                print(f"   Synced {count}/{len(missing_games)} games...")
        
        local_session.commit()
        print(f"✅ Successfully synced {count} games to Local DB.")

        # 3. Sync Metadata for those games
        print("🔍 Syncing Metadata for new games...")
        new_game_ids = [g.game_id for g in missing_games]
        # Chunk metadata fetch to avoid huge IN clause
        meta_count = 0
        for i in range(0, len(new_game_ids), 500):
            chunk = new_game_ids[i:i+500]
            oci_meta = oci_session.query(GameMetadata).filter(GameMetadata.game_id.in_(chunk)).all()
            
            for m in oci_meta:
                new_meta = GameMetadata(
                    game_id=m.game_id,
                    stadium_code=m.stadium_code,
                    stadium_name=m.stadium_name,
                    attendance=m.attendance,
                    start_time=m.start_time,
                    end_time=m.end_time,
                    game_time_minutes=m.game_time_minutes,
                    weather=m.weather,
                    source_payload=m.source_payload
                )
                local_session.add(new_meta)
                meta_count += 1
            
            local_session.commit()
            print(f"   Synced {meta_count} metadata rows...")
        
        print(f"✅ Successfully synced {meta_count} metadata rows to Local DB.")

if __name__ == "__main__":
    reverse_sync()
