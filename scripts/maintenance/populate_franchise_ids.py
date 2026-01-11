
import os
import sys

# Ensure project root is in path
sys.path.append(os.getcwd())

from sqlalchemy import select, update
from src.db.engine import SessionLocal
from src.models.franchise import Franchise
from src.models.team import Team
from src.utils.team_codes import GAME_ID_SEGMENT_TO_CODE

def populate_franchise_ids():
    print("🛠️  Populating franchise_id in teams table...")
    
    with SessionLocal() as session:
        # 1. Load all Franchises
        franchises = session.execute(select(Franchise)).scalars().all()
        # Map canonical_code -> franchise_id
        # franchise.current_code should match canonical codes
        franchise_map = {f.current_code: f.id for f in franchises}
        
        print(f"📖 Loaded {len(franchises)} franchises.")
        
        # 2. Iterate all Teams (or just iterate mapping keys)
        # Let's iterate GAME_ID_SEGMENT_TO_CODE to cover historical mappings
        
        updated_count = 0
        
        # Also include direct mapping for current codes if missing from segment map
        # e.g. "SS" -> "SS".
        
        # Get all team_ids present in DB
        db_teams = session.execute(select(Team)).scalars().all()
        db_team_ids = [t.team_id for t in db_teams]
        
        for team_id in db_team_ids:
            # Resolve canonical
            canonical = GAME_ID_SEGMENT_TO_CODE.get(team_id, team_id) # Default to self if not in map
            
            # Find franchise id
            f_id = franchise_map.get(canonical)
            
            if f_id:
                # Update
                stmt = update(Team).where(Team.team_id == team_id).values(franchise_id=f_id)
                session.execute(stmt)
                updated_count += 1
                # print(f"   Linked {team_id} -> Franchise {f_id} ({canonical})")
            else:
                print(f"⚠️  No Franchise found for team_id '{team_id}' (Canonical: '{canonical}')")
        
        session.commit()
        print(f"✅ Updated {updated_count} teams with franchise_id.")

if __name__ == "__main__":
    populate_franchise_ids()
