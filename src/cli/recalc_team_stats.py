import argparse
from src.db.engine import SessionLocal
from src.aggregators.team_stat_aggregator import TeamStatAggregator
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching
from src.repositories.team_stats_repository import TeamSeasonBattingRepository, TeamSeasonPitchingRepository
from src.utils.team_mapping import get_team_mapping_for_year

def main():
    parser = argparse.ArgumentParser(description="Recalculate team-level season stats from transactional data.")
    parser.add_argument("--year", type=int, required=True, help="Season year")
    parser.add_argument("--league", type=str, default="regular", help="League key")
    parser.add_argument("--type", type=str, default="all", choices=["batting", "pitching", "all"])
    parser.add_argument("--save", action="store_true", help="Save results to local database")
    
    args = parser.parse_args()
    
    print(f"🚀 Starting Team Recalculation for {args.year} ({args.league})")
    
    team_mapping = get_team_mapping_for_year(args.year)
    reverse_mapping = {v: k for k, v in team_mapping.items()}
    
    # Valid columns for filtering
    batting_cols = {c.key for c in TeamSeasonBatting.__table__.columns}
    pitching_cols = {c.key for c in TeamSeasonPitching.__table__.columns}
    
    with SessionLocal() as session:
        if args.type in ["batting", "all"]:
            print(f"\n[BATTING] Processing {args.year}...")
            batting_data = TeamStatAggregator.aggregate_team_batting(session, args.year, args.league, source='MANUAL_RECALC')
            
            processed_batting = []
            for s in batting_data:
                s['team_name'] = reverse_mapping.get(s['team_id'], s['team_id'])
                # Filter valid columns
                processed_batting.append({k: v for k, v in s.items() if k in batting_cols})
            
            if args.save and processed_batting:
                repo = TeamSeasonBattingRepository()
                repo.upsert_many(processed_batting)
                print(f"   ✅ Saved {len(processed_batting)} team batting records.")
        
        if args.type in ["pitching", "all"]:
            print(f"\n[PITCHING] Processing {args.year}...")
            pitching_data = TeamStatAggregator.aggregate_team_pitching(session, args.year, args.league, source='MANUAL_RECALC')
            
            processed_pitching = []
            for s in pitching_data:
                s['team_name'] = reverse_mapping.get(s['team_id'], s['team_id'])
                # Filter valid columns
                processed_pitching.append({k: v for k, v in s.items() if k in pitching_cols})

            if args.save and processed_pitching:
                repo = TeamSeasonPitchingRepository()
                repo.upsert_many(processed_pitching)
                print(f"   ✅ Saved {len(processed_pitching)} team pitching records.")

    print("\n✅ Team recalculation task finished.")

if __name__ == "__main__":
    main()
