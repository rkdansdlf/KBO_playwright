import argparse
from src.repositories.safe_batting_repository import save_batting_stats_safe
from src.repositories.player_season_pitching_repository import save_pitching_stats_to_db
from src.crawlers.player_pitching_all_series_crawler import fallback_pitching_from_db
from src.crawlers.player_batting_all_series_crawler import fallback_batting_from_db

def main():
    parser = argparse.ArgumentParser(description="Recalculate season cumulative stats from transactional game details.")
    parser.add_argument("--year", type=int, required=True, help="Season year")
    parser.add_argument("--series", type=str, default="regular", 
                        choices=["regular", "wildcard", "semi_playoff", "playoff", "korean_series", "all"], 
                        help="Series key")
    parser.add_argument("--type", type=str, default="all", 
                        choices=["batting", "pitching", "all"], 
                        help="Stat type")
    parser.add_argument("--save", action="store_true", help="Save results to local database")
    
    args = parser.parse_args()
    
    if args.series == "all":
        series_list = ["regular", "wildcard", "semi_playoff", "playoff", "korean_series"]
    else:
        series_list = [args.series]
    
    print(f"🚀 Starting Recalculation for {args.year} (Type: {args.type}, Series: {args.series})")
    
    for series in series_list:
        if args.type in ["batting", "all"]:
            print(f"\n[BATTING] Processing {args.year} {series}...")
            batting_data = fallback_batting_from_db(args.year, series)
            if args.save and batting_data:
                save_batting_stats_safe(batting_data)
            elif not batting_data:
                print(f"   ℹ️ No batting transactional data found for {args.year} {series}.")
        
        if args.type in ["pitching", "all"]:
            print(f"\n[PITCHING] Processing {args.year} {series}...")
            pitching_data = fallback_pitching_from_db(args.year, series)
            if args.save and pitching_data:
                payloads = [stat.to_repository_payload() for stat in pitching_data]
                save_pitching_stats_to_db(payloads)
            elif not pitching_data:
                print(f"   ℹ️ No pitching transactional data found for {args.year} {series}.")

    print("\n✅ Recalculation task finished.")

if __name__ == "__main__":
    main()
