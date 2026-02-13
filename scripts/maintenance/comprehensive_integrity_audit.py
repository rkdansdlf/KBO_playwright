
import sys
import os
import csv
import argparse
from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from collections import defaultdict
from datetime import datetime

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.db.engine import Engine
from src.models.game import Game, GameBattingStat, GamePitchingStat
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

# Benchmarks: Total Regular Season Games (League Type 0) per Year
# Based on historical team counts and games per team
GAME_COUNT_BENCHMARKS = {
    1982: 240, 1983: 300, 1984: 300, 1985: 330,
    1986: 378, 1987: 378, 1988: 378, 1989: 420, 1990: 420,
    1991: 504, 1992: 504, 1993: 504, 1994: 504, 1995: 504, 1996: 504, 1997: 504, 1998: 504,
    1999: 528, 2000: 532, 2001: 532, 2002: 532, 2003: 532, 2004: 532,
    2005: 504, 2006: 504, 2007: 504, 2008: 504,
    2009: 532, 2010: 532, 2011: 532, 2012: 532,
    2013: 576, 2014: 576,
    2015: 720, 2016: 720, 2017: 720, 2018: 720, 2019: 720, 2020: 720, 2021: 720, 2022: 720, 2023: 720, 2024: 720
}

def run_audit(target_db="local"):
    if target_db == "oci":
        url = os.getenv('OCI_DB_URL')
        if not url:
            print("❌ OCI_DB_URL not found in environment.")
            return
        engine = create_engine(url)
    else:
        engine = Engine

    Session = sessionmaker(bind=engine)
    session = Session()

    print(f"🚀 Initializing Comprehensive Audit on [{target_db.upper()}]...")
    
    discrepancies = []
    
    # 1. Volume Audit (Game Counts)
    print("📊 Part 1: Game Volume Verification...")
    game_counts = session.query(
        func.substr(Game.game_id, 1, 4).label('year'),
        func.count(Game.game_id).label('cnt')
    ).filter(text("season_id < 1000")) # Only official sequential IDs (Regular/Post)
    # Note: Benchmarks are for Regular Season (league_type_code 0)
    # But let's check total per year vs our benchmark
    
    results = game_counts.group_by('year').all()
    for year_str, count in results:
        yr = int(year_str)
        benchmark = GAME_COUNT_BENCHMARKS.get(yr)
        if benchmark:
            diff = count - benchmark
            if abs(diff) > 0: # Usually allow a few postseason games if not filtered exactly
                # But for Regular Season filter:
                reg_count = session.query(func.count(Game.game_id)).filter(
                    Game.game_id.like(f"{yr}%"),
                    text("season_id IN (SELECT season_id FROM kbo_seasons WHERE league_type_code = '0')")
                ).scalar()
                
                if reg_count != benchmark:
                    discrepancies.append({
                        'scope': 'SEASON',
                        'id': year_str,
                        'type': 'GAME_COUNT_MISMATCH',
                        'expected': benchmark,
                        'actual': reg_count,
                        'detail': f'Missing/Extra games: {reg_count - benchmark}'
                    })

    # 2. Score Consistency (Batting vs Pitching)
    print("⚾ Part 2: Score Consistency Analysis...")
    # Batting Runs vs Game Score
    batting_runs = session.query(
        GameBattingStat.game_id,
        GameBattingStat.team_side,
        func.sum(GameBattingStat.runs).label('total_runs')
    ).group_by(GameBattingStat.game_id, GameBattingStat.team_side).all()
    
    game_scores = {g.game_id: {'home': g.home_score, 'away': g.away_score} 
                   for g in session.query(Game.game_id, Game.home_score, Game.away_score).all()}
    
    for game_id, side, b_runs in batting_runs:
        if game_id not in game_scores: continue
        expected = game_scores[game_id][side]
        if b_runs != expected:
            discrepancies.append({
                'scope': 'GAME',
                'id': game_id,
                'type': 'BATTING_SCORE_MISMATCH',
                'expected': expected,
                'actual': b_runs,
                'detail': f'Team: {side}'
            })

    # Pitching Runs Allowed vs Opponent Batting Runs
    pitching_runs = session.query(
        GamePitchingStat.game_id,
        GamePitchingStat.team_side,
        func.sum(GamePitchingStat.runs_allowed).label('runs_allowed')
    ).group_by(GamePitchingStat.game_id, GamePitchingStat.team_side).all()
    
    for game_id, side, p_runs in pitching_runs:
        if game_id not in game_scores: continue
        # Opponent side
        opp_side = 'away' if side == 'home' else 'home'
        expected = game_scores[game_id][opp_side]
        if p_runs != expected:
             discrepancies.append({
                'scope': 'GAME',
                'id': game_id,
                'type': 'PITCHING_SCORE_MISMATCH',
                'expected': expected,
                'actual': p_runs,
                'detail': f'Pitching Team: {side} (allowed {p_runs}, but opponent scored {expected})'
            })

    # 3. Ghost Players (NULL player_id)
    print("👻 Part 3: Ghost Player Detection...")
    ghost_batters = session.query(GameBattingStat.game_id, GameBattingStat.player_name).filter(GameBattingStat.player_id == None).all()
    for gid, name in ghost_batters:
        discrepancies.append({
            'scope': 'PLAYER_ENTRY',
            'id': gid,
            'type': 'GHOST_BATTER',
            'expected': 'Valid Player ID',
            'actual': 'NULL',
            'detail': name
        })
        
    ghost_pitchers = session.query(GamePitchingStat.game_id, GamePitchingStat.player_name).filter(GamePitchingStat.player_id == None).all()
    for gid, name in ghost_pitchers:
        discrepancies.append({
            'scope': 'PLAYER_ENTRY',
            'id': gid,
            'type': 'GHOST_PITCHER',
            'expected': 'Valid Player ID',
            'actual': 'NULL',
            'detail': name
        })

    # 4. Historical Expansion (1982-2000)
    print("📈 Part 4: Historical Roster Audit...")
    for year in range(1982, 2001):
        batters = session.query(func.count(PlayerSeasonBatting.id)).filter(PlayerSeasonBatting.season == year).scalar()
        if batters < 80: # Benchmark: Expanded rosters usually have >100 players
            discrepancies.append({
                'scope': 'SEASON_STATS',
                'id': str(year),
                'type': 'INCOMPLETE_ROSTER',
                'expected': '>100',
                'actual': batters,
                'detail': 'Batting'
            })

    # Save results
    output_file = f"data/audit_comprehensive_{target_db}.csv"
    os.makedirs('data', exist_ok=True)
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['scope', 'id', 'type', 'expected', 'actual', 'detail'])
        writer.writeheader()
        writer.writerows(discrepancies)

    print(f"\n✅ Audit Finished. Result: {len(discrepancies)} issues found.")
    print(f"📄 Report saved to: {output_file}")
    
    # Summary by type
    summary = defaultdict(int)
    for d in discrepancies: summary[d['type']] += 1
    for k, v in summary.items():
        print(f"   - {k}: {v}")

    session.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", choices=["local", "oci"], default="local")
    args = parser.parse_args()
    
    run_audit(target_db=args.db)
