"""
Cleanup script to remove corrupted historical player records.
Targets records with unrealistic values (e.g., wins > 35, games > 165).
"""
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonPitching, PlayerSeasonBatting
from src.models.team import Team # Import to resolve FK constraints

def cleanup_corrupted_stats():
    with SessionLocal() as session:
        print("🧹 Starting precision cleaning of historical data...")
        
        # 1. Pitching Cleanup
        p_corrupted = session.query(PlayerSeasonPitching).filter(
            (PlayerSeasonPitching.wins > 35) | 
            (PlayerSeasonPitching.games > 165)
        ).all()
        
        print(f"   Found {len(p_corrupted)} corrupted pitching records.")
        for rec in p_corrupted:
            session.delete(rec)
            
        # 2. Batting Cleanup
        b_corrupted = session.query(PlayerSeasonBatting).filter(
            (PlayerSeasonBatting.home_runs > 65) | 
            (PlayerSeasonBatting.games > 165)
        ).all()
        
        print(f"   Found {len(b_corrupted)} corrupted batting records.")
        for rec in b_corrupted:
            session.delete(rec)
            
        session.commit()
        print("✅ Cleanup complete. Database is now purged of unrealistic outliers.")

if __name__ == "__main__":
    cleanup_corrupted_stats()
