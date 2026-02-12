import argparse
from src.crawlers.legacy_batting_crawler import crawl_legacy_batting_stats
from src.crawlers.legacy_pitching_crawler import crawl_legacy_pitching_stats

def recover_legacy_season(year: int, save: bool = False, headless: bool = True):
    print(f"⚾ Starting legacy recovery for {year} season...")
    
    # 1. Recover Batting Stats
    print("\n[BATTING] Crawling legacy batting stats...")
    batting_data = crawl_legacy_batting_stats(
        year=year,
        series_key='regular',
        save_to_db=save,
        headless=headless
    )
    print(f"✅ Recovered {len(batting_data)} batting records.")

    # 2. Recover Pitching Stats
    print("\n[PITCHING] Crawling legacy pitching stats...")
    pitching_data = crawl_legacy_pitching_stats(
        year=year,
        series_key='regular',
        save_to_db=save,
        headless=headless
    )
    print(f"✅ Recovered {len(pitching_data)} pitching records.")

    print(f"\n✨ Legacy recovery for {year} complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Recover legacy season stats (1982-2000)")
    parser.add_argument("--year", type=int, required=True, help="Season year")
    parser.add_argument("--save", action="store_true", help="Save to database")
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="Run with browser visible")
    
    args = parser.parse_args()
    
    recover_legacy_season(args.year, args.save, args.headless)
