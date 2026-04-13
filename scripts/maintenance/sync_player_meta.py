"""
Syncs basic player info from 'player_basic' (populated from Search)
into the relational 'players' and 'player_identities' tables.
Ensures PlayerIdResolver can match players using relational links.
"""
from __future__ import annotations

import sys
import os

# Put project root in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from sqlalchemy import select
from src.db.engine import SessionLocal
from src.models.player import PlayerBasic, Player, PlayerIdentity
from src.utils.safe_print import safe_print as print

def sync_profiles():
    print("🚀 Starting Player Metadata Sync (Relational Mirroring)...")
    
    with SessionLocal() as session:
        # Get all players from basic table
        basics = session.execute(select(PlayerBasic)).scalars().all()
        print(f"📊 Found {len(basics)} players in 'player_basic'.")
        
        synced_count = 0
        new_players = 0
        new_identities = 0
        
        for basic in basics:
            # 1. Check if player exists in master 'players' table
            # We treat player_basic.player_id as the canonical kbo_person_id string
            kbo_id = str(basic.player_id)
            
            player = session.execute(
                select(Player).where(Player.kbo_person_id == kbo_id)
            ).scalar_one_or_none()
            
            if not player:
                # Create master player record
                player = Player(
                    kbo_person_id=kbo_id,
                    height_cm=basic.height_cm,
                    weight_kg=basic.weight_kg,
                    birth_date=basic.birth_date_date,
                    status="ACTIVE" if basic.status != "retired" else "RETIRED"
                )
                session.add(player)
                session.flush() # Get player.id
                new_players += 1
            
            # 2. Check/create Identity
            identity = session.execute(
                select(PlayerIdentity).where(
                    PlayerIdentity.player_id == player.id,
                    PlayerIdentity.name_kor == basic.name
                )
            ).scalar_one_or_none()
            
            if not identity:
                identity = PlayerIdentity(
                    player_id=player.id,
                    name_kor=basic.name,
                    is_primary=True
                )
                session.add(identity)
                new_identities += 1
            
            synced_count += 1
            if synced_count % 500 == 0:
                print(f"   Progress: {synced_count} processed...")
                session.commit() # Periodic commit

        session.commit()
        print(f"\n✅ Sync Complete!")
        print(f"   - Total processed: {synced_count}")
        print(f"   - New master players: {new_players}")
        print(f"   - New identities: {new_identities}")

if __name__ == "__main__":
    sync_profiles()
