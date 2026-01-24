"""
Batch Repair Script for Game Pitching Stats.
Fills NULL ERA, WHIP, K/9, etc. using PitchingStatCalculator.
"""
import sys
import os

# Adjust path
sys.path.append(os.getcwd())

from src.db.engine import SessionLocal
from src.models.game import GamePitchingStat
from src.services.stat_calculator import PitchingStatCalculator

def repair_stats():
    print("🚀 Starting Pitching Stat Repair...")
    
    with SessionLocal() as session:
        # Find rows with missing stats
        query = session.query(GamePitchingStat).filter(
            (GamePitchingStat.era.is_(None)) | (GamePitchingStat.era == 0.0)
        )
        
        total = query.count()
        if total == 0:
            print("✅ No missing stats found in game_pitching_stats.")
            return

        print(f"📊 Found {total} rows to repair.")
        
        updated_count = 0
        batch_size = 500
        
        for idx, stat in enumerate(query.all(), 1):
            # Map model to dict for calculator
            raw_data = {
                'innings_outs': stat.innings_outs,
                'earned_runs': stat.earned_runs,
                'hits_allowed': stat.hits_allowed,
                'walks_allowed': stat.walks_allowed,
                'strikeouts': stat.strikeouts,
                'home_runs_allowed': stat.home_runs_allowed,
                'hit_batters': stat.hit_batters,
                'batters_faced': stat.batters_faced,
            }
            
            ratios = PitchingStatCalculator.calculate_ratios(raw_data)
            
            # Update row
            stat.era = ratios['era']
            stat.whip = ratios['whip']
            stat.k_per_nine = ratios['k_per_nine']
            stat.bb_per_nine = ratios['bb_per_nine']
            stat.kbb = ratios['kbb']
            
            # FIP is not a column in the model by default, store in extra_stats if available
            if hasattr(stat, 'fip'):
                stat.fip = ratios['fip']
            elif hasattr(stat, 'extra_stats'):
                if stat.extra_stats is None:
                    stat.extra_stats = {}
                extras = dict(stat.extra_stats)
                extras['fip'] = ratios['fip']
                stat.extra_stats = extras
            
            updated_count += 1
            
            if idx % batch_size == 0:
                session.commit()
                print(f"   Progress: {idx}/{total} processed...")
        
        session.commit()
        print(f"✅ Finished! Updated {updated_count} rows.")

if __name__ == "__main__":
    repair_stats()
