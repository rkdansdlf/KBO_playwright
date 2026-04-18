
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from sqlalchemy import text
from src.db.engine import SessionLocal
from src.models.player import Player, PlayerBasic
from dotenv import load_dotenv
from datetime import datetime

def repair_master():
    load_dotenv()
    session = SessionLocal()
    
    print("\n🛠️  Repairing Master Players Table (ORM Mode)...")
    print("-" * 50)
    
    # 1. Get all players from player_basic
    basics = session.query(PlayerBasic).all()
    
    # Active teams list including codes
    ACTIVE_TEAMS = (
        'SS', 'HT', 'LT', 'OB', 'HH', 'KT', 'NC', 'SK', 'WO', 'LG', 'KH',
        '삼성', 'KIA', '롯데', '두산', '한화', 'NC', 'SSG', '키움', 'LG'
    )
    
    upsert_count = 0
    new_inserts = 0
    
    for pb in basics:
        p_id_str = str(pb.player_id)
        
        # Heuristic for status: If team exists and is not 'None' or '해체', mark as ACTIVE
        calc_status = "RETIRED"
        if pb.team and any(t in pb.team for t in ACTIVE_TEAMS):
            calc_status = "ACTIVE"
        elif pb.status == 'active':
            calc_status = "ACTIVE"
            
        # Check if exists in players
        player = session.query(Player).filter(Player.kbo_person_id == p_id_str).first()
        
        if player:
            # Update existing master record
            player.status = calc_status
            if not player.photo_url and pb.photo_url:
                player.photo_url = pb.photo_url
            
            # Enrich with available detailed data
            if pb.salary_original: player.salary_original = pb.salary_original
            if pb.signing_bonus_original: player.signing_bonus_original = pb.signing_bonus_original
            if pb.draft_info: player.draft_info = pb.draft_info
            
            upsert_count += 1
        else:
            # Insert new master record
            new_player = Player(
                kbo_person_id=p_id_str,
                status=calc_status,
                photo_url=pb.photo_url,
                salary_original=pb.salary_original,
                signing_bonus_original=pb.signing_bonus_original,
                draft_info=pb.draft_info,
                is_foreign_player=False # Default
            )
            session.add(new_player)
            new_inserts += 1
            
        # Batch commit every 100 records
        if (new_inserts + upsert_count) % 100 == 0:
            session.commit()
            
    session.commit()
    print(f"✅ Successfully processed {len(basics)} players.")
    print(f"   - New master records created: {new_inserts}")
    print(f"   - Existing records updated: {upsert_count}")
    print("-" * 50)
    
    session.close()

if __name__ == "__main__":
    repair_master()
