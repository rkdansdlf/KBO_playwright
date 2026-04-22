from typing import List, Dict, Any
from sqlalchemy.orm import Session
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.services.stat_calculator import BattingStatCalculator, PitchingStatCalculator

def audit_batting_stats(session: Session, limit: int = 5000):
    print(f"--- Auditing Batting Stats (Seasons >= 2024, Sample: {limit}) ---")
    records = session.query(PlayerSeasonBatting).filter(PlayerSeasonBatting.season >= 2024).limit(limit).all()
    errors = 0
    passed = 0
    
    for rec in records:
        if not rec.at_bats: continue
        
        data = {
            'at_bats': rec.at_bats,
            'hits': rec.hits,
            'walks': rec.walks,
            'hbp': rec.hbp,
            'sacrifice_flies': rec.sacrifice_flies,
            'sacrifice_hits': rec.sacrifice_hits,
            'doubles': rec.doubles,
            'triples': rec.triples,
            'home_runs': rec.home_runs,
            'strikeouts': rec.strikeouts,
            'intentional_walks': rec.intentional_walks,
            'stolen_bases': rec.stolen_bases,
            'caught_stealing': rec.caught_stealing,
            'gdp': rec.gdp
        }
        
        recalc = BattingStatCalculator.calculate_ratios(data)
        
        # Check AVG, OBP, SLG
        checks = [('avg', rec.avg), ('obp', rec.obp), ('slg', rec.slg)]
        
        is_rec_valid = True
        for key, stored_val in checks:
            if stored_val is None: continue
            if abs(recalc[key] - stored_val) > 0.002:
                print(f"❌ Batting Mismatch [ID:{rec.id}, S:{rec.season}, P:{rec.player_id}]: {key} Stored={stored_val}, Recalc={recalc[key]}")
                errors += 1
                is_rec_valid = False
        
        if is_rec_valid:
            passed += 1
                
    print(f"Batting Audit Complete. Passed: {passed}, Fails: {errors}")

def audit_pitching_stats(session: Session, limit: int = 5000):
    print(f"\n--- Auditing Pitching Stats (Seasons >= 2024, Sample: {limit}) ---")
    records = session.query(PlayerSeasonPitching).filter(PlayerSeasonPitching.season >= 2024).limit(limit).all()
    errors = 0
    passed = 0
    
    for rec in records:
        innings_outs = rec.innings_outs
        if innings_outs is None and rec.extra_stats and 'innings_outs' in rec.extra_stats:
            innings_outs = int(rec.extra_stats['innings_outs'])
            
        if innings_outs is None and rec.innings_pitched is not None:
            ip = float(rec.innings_pitched)
            ip_str = f"{ip:.2f}"
            whole, frac = ip_str.split('.')
            if frac in ['10', '20']:
                innings_outs = int(whole) * 3 + int(frac[0])
            else:
                innings_outs = round(ip * 3)
        
        if not innings_outs: continue
        
        data = {
            'innings_outs': innings_outs,
            'earned_runs': rec.earned_runs,
            'hits_allowed': rec.hits_allowed,
            'walks_allowed': rec.walks_allowed,
            'strikeouts': rec.strikeouts,
            'home_runs_allowed': rec.home_runs_allowed,
            'hit_batters': rec.hit_batters
        }
        
        recalc = PitchingStatCalculator.calculate_ratios(data)
        
        checks = [('era', rec.era), ('whip', rec.whip)]
        
        is_rec_valid = True
        for key, stored_val in checks:
            if stored_val is None: continue
            if abs(recalc[key] - stored_val) > 0.02: 
                print(f"❌ Pitching Mismatch [ID:{rec.id}, S:{rec.season}, P:{rec.player_id}]: {key} Stored={stored_val}, Recalc={recalc[key]} (Outs={innings_outs})")
                errors += 1
                is_rec_valid = False
        
        if is_rec_valid:
            passed += 1

    print(f"Pitching Audit Complete. Passed: {passed}, Fails: {errors}")

if __name__ == "__main__":
    db = SessionLocal()
    try:
        audit_batting_stats(db, limit=5000)
        audit_pitching_stats(db, limit=5000)
    finally:
        db.close()
