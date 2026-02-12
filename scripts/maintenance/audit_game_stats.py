
import sys
import os
import csv
import argparse
from sqlalchemy import create_engine, text, func, case
from sqlalchemy.orm import sessionmaker
from collections import defaultdict

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.db.engine import Engine
from src.models.game import Game, GameBattingStat

def audit_game_stats(year: int = None):
    engine = Engine
    Session = sessionmaker(bind=engine)
    session = Session()

    print(f"🔍 Starting Data Integrity Audit{' for ' + str(year) if year else ''}...")
    print("   Comparing Game Scores vs. Aggregated Batting Stats...")

    # 1. Fetch Game Scores
    games_query = session.query(
        Game.game_id, 
        Game.home_team, 
        Game.away_team, 
        Game.home_score, 
        Game.away_score
    )
    if year:
        # Use game_id prefix for more reliable historical filtering
        games_query = games_query.filter(Game.game_id.like(f'{year}%'))
    
    games_results = games_query.all()
    
    game_map = {
        g.game_id: {
            'home': g.home_team, 
            'away': g.away_team, 
            'home_score': g.home_score or 0, 
            'away_score': g.away_score or 0
        } 
        for g in games_results
    }
    
    print(f"   Loaded {len(games_results)} games.")

    # 2. Aggregate Batting Stats Key Metrics
    stats_query = session.query(
        GameBattingStat.game_id,
        GameBattingStat.team_side,
        func.sum(GameBattingStat.runs).label('total_runs'),
        func.sum(GameBattingStat.hits).label('total_hits'),
        func.count(GameBattingStat.id).label('player_count')
    )
    
    if year:
        stats_query = stats_query.filter(GameBattingStat.game_id.like(f'{year}%'))
    
    stats_results = stats_query.group_by(
        GameBattingStat.game_id,
        GameBattingStat.team_side
    ).all()

    discrepancies = []
    
    # 3. Compare
    processed_games = set()

    for row in stats_results:
        game_id = row.game_id
        team_side = row.team_side # 'home' or 'away'
        calc_runs = row.total_runs
        calc_hits = row.total_hits

        processed_games.add(game_id)
        
        if game_id not in game_map:
            continue

        game_info = game_map[game_id]
        expected_score = game_info[f'{team_side}_score']
        
        if calc_runs != expected_score:
            discrepancies.append({
                'game_id': game_id,
                'type': 'SCORE_MISMATCH',
                'team': game_info[team_side],
                'side': team_side,
                'expected': expected_score,
                'actual': calc_runs,
                'diff': expected_score - calc_runs
            })

    # 4. Ghost Players (NULL player_id)
    ghost_query = session.query(
        GameBattingStat.game_id,
        GameBattingStat.player_name,
        GameBattingStat.team_code
    ).filter(GameBattingStat.player_id == None)
    
    if year:
        ghost_query = ghost_query.filter(GameBattingStat.game_id.like(f'{year}%'))
        
    ghost_results = ghost_query.all()
    
    for row in ghost_results:
        discrepancies.append({
            'game_id': row.game_id,
            'type': 'GHOST_PLAYER',
            'team': row.team_code,
            'side': 'N/A',
            'expected': 'Valid ID',
            'actual': f'NULL ({row.player_name})',
            'diff': 0
        })
        
    # 5. Duplicate Lineups
    dupe_query = session.query(
        GameBattingStat.game_id,
        GameBattingStat.player_id,
        GameBattingStat.player_name,
        func.count('*').label('cnt')
    ).group_by(
        GameBattingStat.game_id,
        GameBattingStat.player_id,
        GameBattingStat.player_name
    ).having(func.count('*') > 1)
    
    if year:
        dupe_query = dupe_query.filter(GameBattingStat.game_id.like(f'{year}%'))
        
    dupe_results = dupe_query.all()
    
    for row in dupe_results:
        if row.player_id is None: continue 
        discrepancies.append({
            'game_id': row.game_id,
            'type': 'DUPLICATE_PLAYER',
            'team': 'N/A', 
            'side': 'N/A',
            'expected': '1 entry',
            'actual': f'{row.cnt} entries ({row.player_name})',
            'diff': row.cnt - 1
        })

    # Output to CSV
    output_path = f'data/audit_results_{year if year else "all"}.csv'
    os.makedirs('data', exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['game_id', 'type', 'team', 'side', 'expected', 'actual', 'diff'])
        writer.writeheader()
        writer.writerows(discrepancies)

    print(f"\n✅ Audit Complete.")
    print(f"   Analyzed {len(processed_games)} games having stats.")
    print(f"   Found {len(discrepancies)} issues.")
    print(f"   Report saved to: {output_path}")
    
    by_type = defaultdict(int)
    for d in discrepancies:
        by_type[d['type']] += 1
        
    print("\n📊 Issue Summary:")
    if not discrepancies:
        print("   ✨ No issues found!")
    for k, v in by_type.items():
        print(f"   - {k}: {v}")

    session.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Season year to audit")
    parser.add_argument("--start-year", type=int, help="Start year for range audit")
    parser.add_argument("--end-year", type=int, help="End year for range audit")
    args = parser.parse_args()
    
    if args.start_year and args.end_year:
        print(f"🔍 Running Audit for range {args.start_year}-{args.end_year}...")
        for y in range(args.start_year, args.end_year + 1):
            audit_game_stats(y)
    elif args.year:
        audit_game_stats(args.year)
    else:
        print("Please provide --year or --start-year/--end-year")
