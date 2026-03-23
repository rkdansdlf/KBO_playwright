import asyncio
import argparse
from sqlalchemy import select, update, delete
from sqlalchemy.orm import Session

from src.db.engine import SessionLocal
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching
from src.models.game import GameBattingStat, GamePitchingStat, GameLineup

def merge_player(session: Session, source_id: int, target_id: int):
    """Merges source_id into target_id for all game stat models, then deletes source_id."""
    print(f"Merging temporary ID {source_id} -> Official KBO ID {target_id}")
    
    # 1. Update Game Batting Stats
    b_updated = session.execute(
        update(GameBattingStat).where(GameBattingStat.player_id == source_id).values(player_id=target_id)
    ).rowcount
    
    # 2. Update Game Pitching Stats
    p_updated = session.execute(
        update(GamePitchingStat).where(GamePitchingStat.player_id == source_id).values(player_id=target_id)
    ).rowcount
    
    # 3. Update Game Lineups
    l_updated = session.execute(
        update(GameLineup).where(GameLineup.player_id == source_id).values(player_id=target_id)
    ).rowcount
    
    print(f"  - Batting Stats updated: {b_updated}")
    print(f"  - Pitching Stats updated: {p_updated}")
    print(f"  - Lineups updated: {l_updated}")
    
    # 4. Delete temporary PlayerBasic profile
    session.execute(delete(PlayerBasic).where(PlayerBasic.player_id == source_id))
    session.commit()
    print(f"✅ Deleted temporary profile {source_id}")


def run_merge(auto_confirm: bool = False):
    session = SessionLocal()
    try:
        # Find all pseudo-IDs (>= 900000)
        pseudos = session.execute(
            select(PlayerBasic).where(PlayerBasic.player_id >= 900000)
        ).scalars().all()
        
        if not pseudos:
            print("ℹ️ No temporary pseudo-IDs found in the database. Everything is clean!")
            return
            
        print(f"🔍 Found {len(pseudos)} temporary profiles.")
        
        for pseudo in pseudos:
            print(f"\nEvaluating Pseudo Profile: [{pseudo.player_id}] {pseudo.name} (Team: {pseudo.team})")
            
            # Find matching official profiles
            stmt = select(PlayerBasic).where(
                PlayerBasic.name == pseudo.name,
                PlayerBasic.team == pseudo.team,
                PlayerBasic.player_id < 900000
            )
            candidates = session.execute(stmt).scalars().all()
            
            if not candidates:
                print("  ⚠️ No official KBO matching profile found yet. Skipping.")
                continue
                
            if len(candidates) > 1:
                # Need to disambiguate by uniform_no or other metrics
                matched = [c for c in candidates if c.uniform_no == pseudo.uniform_no]
                if len(matched) == 1:
                    candidates = matched
                else:
                    print("  ⚠️ Ambiguous multiple official profiles found. Skipping.")
                    for c in candidates:
                        print(f"      - {c.player_id}: {c.name} (Uni: {c.uniform_no})")
                    continue
            
            target = candidates[0]
            print(f"  🎯 Found Official Match: [{target.player_id}] {target.name} (Uni: {target.uniform_no})")
            
            if not auto_confirm:
                ans = input(f"Proceed with merging {pseudo.player_id} into {target.player_id}? (y/n): ")
                if ans.lower() != 'y':
                    print("  Skipping.")
                    continue
            
            merge_player(session, pseudo.player_id, target.player_id)
            
    except Exception as e:
        session.rollback()
        print(f"❌ Fatal Error during merge: {e}")
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description="Merge pseudo player IDs to official KBO IDs")
    parser.add_argument("--yes", "-y", action="store_true", help="Auto-confirm all unique merges")
    args = parser.parse_args()
    
    run_merge(auto_confirm=args.yes)


if __name__ == "__main__":
    main()
