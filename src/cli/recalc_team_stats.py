import argparse
from src.db.engine import SessionLocal
from src.aggregators.team_stat_aggregator import TeamStatAggregator
from src.repositories.team_stats_repository import TeamSeasonBattingRepository, TeamSeasonPitchingRepository
from src.utils.team_mapping import get_team_mapping_for_year

def main():
    parser = argparse.ArgumentParser(description="Recalculate team-level season stats from transactional data.")
    parser.add_argument("--year", type=int, required=True, help="Season year")
    parser.add_argument("--league", type=str, default="regular", help="League key")
    parser.add_argument("--type", type=str, default="all", choices=["batting", "pitching", "all"])
    parser.add_argument("--save", action="store_true", help="Save results to local database")
    
    args = parser.parse_args()
    league_upper = args.league.upper()
    
    print(f"🚀 Starting Team Recalculation for {args.year} ({args.league})")
    
    team_mapping = get_team_mapping_for_year(args.year)
    reverse_mapping = {v: k for k, v in team_mapping.items()}
    
    with SessionLocal() as session:
        if args.type in ["batting", "all"]:
            print(f"\n[BATTING] Processing {args.year}...")
            batting_data = TeamStatAggregator.aggregate_team_batting(session, args.year, args.league)
            for s in batting_data:
                s['team_name'] = reverse_mapping.get(s['team_id'], s['team_id'])
            
            if args.save and batting_data:
                repo = TeamSeasonBattingRepository()
                repo.upsert_many(batting_data)
                print(f"   ✅ Saved {len(batting_data)} team batting records.")
        
        if args.type in ["pitching", "all"]:
            print(f"\n[PITCHING] Processing {args.year}...")
            pitching_data = TeamStatAggregator.aggregate_team_pitching(session, args.year, args.league)
            for s in pitching_data:
                s['team_name'] = reverse_mapping.get(s['team_id'], s['team_id'])

            if args.save and pitching_data:
                repo = TeamSeasonPitchingRepository()
                repo.upsert_many(pitching_data)
                print(f"   ✅ Saved {len(pitching_data)} team pitching records.")

    print("\n✅ Team recalculation task finished.")

if __name__ == "__main__":
    main()
