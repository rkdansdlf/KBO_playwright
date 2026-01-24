"""
Batch Repair Script for Game Batting Stats.
Fills NULL/Zero AVG, OBP, etc. using BattingStatCalculator.
"""
import sys
import os

# Adjust path
sys.path.append(os.getcwd())

from src.db.engine import SessionLocal
from src.models.game import GameBattingStat
from src.services.stat_calculator import BattingStatCalculator

def repair_stats():
    print("🚀 Starting Batting Stat Repair...")
    
    with SessionLocal() as session:
        # Find rows with missing stats
        # We check 'avg' as a proxy for all missing rates
        query = session.query(GameBattingStat).filter(
            (GameBattingStat.avg.is_(None)) | (GameBattingStat.avg == 0.0)
        )
        
        total = query.count()
        if total == 0:
            print("✅ No missing stats found in game_batting_stats.")
            return

        print(f"📊 Found {total} rows to repair.")
        
        updated_count = 0
        batch_size = 500
        
        for idx, stat in enumerate(query.all(), 1):
            # Map model to dict for calculator
            raw_data = {
                'at_bats': stat.at_bats,
                'hits': stat.hits,
                'walks': stat.walks,
                'hbp': stat.hbp,
                'sacrifice_flies': stat.sacrifice_flies,
                'doubles': stat.doubles,
                'triples': stat.triples,
                'home_runs': stat.home_runs,
                'strikeouts': stat.strikeouts,
                'plate_appearances': stat.plate_appearances,
                'intentional_walks': stat.intentional_walks,
                'stolen_bases': stat.stolen_bases,
                'caught_stealing': stat.caught_stealing,
                'gdp': stat.gdp,
                'sacrifice_hits': stat.sacrifice_hits
            }
            
            ratios = BattingStatCalculator.calculate_ratios(raw_data)
            
            # Update row (always if it's currently NULL)
            stat.avg = ratios['avg']
            stat.obp = ratios['obp']
            stat.slg = ratios['slg']
            stat.ops = ratios['ops']
            stat.iso = ratios['iso']
            stat.babip = ratios['babip']
            
            # Handle extra_stats if it's null
            if stat.extra_stats is None:
                stat.extra_stats = {}
            
            # Update XR in extra_stats
            # Ensure it's a dict copy to trigger SQLAlchemy update tracking for MutableDict if needed, 
            # or just reassignment for JSON type.
            extras = dict(stat.extra_stats)
            extras['xr'] = ratios['xr']
            stat.extra_stats = extras
            
            updated_count += 1
            
            if idx % batch_size == 0:
                session.commit()
                print(f"   Progress: {idx}/{total} processed...")
        
        session.commit()
        print(f"✅ Finished! Updated {updated_count} rows.")

if __name__ == "__main__":
    repair_stats()
