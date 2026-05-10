import argparse
from typing import List
from src.db.engine import SessionLocal
from src.services.matchup_engine import MatchupEngine

def batch_calculate_matchups(years: List[int], sync_oci: bool = False):
    """
    Runs the MatchupEngine for a range of years to compute BvP and Splits.
    """
    engine = MatchupEngine()
    
    for year in years:
        try:
            engine.execute_all(year)
        except Exception as e:
            print(f"⚠️ Failed to calculate matchups for {year}: {e}")

    if sync_oci:
        print("🚀 Syncing Matchups to OCI...")
        from src.cli.sync_oci import OCISync
        import os
        
        target_url = os.getenv('OCI_DB_URL')
        if target_url:
            with SessionLocal() as session:
                syncer = OCISync(target_url, session)
                syncer.sync_matchups()
            print("✅ Sync complete.")
        else:
            print("⚠️ OCI_DB_URL not set, skipping sync.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate Matchup and Split matrices.")
    parser.add_argument("--years", type=str, default="2020-2026")
    parser.add_argument("--sync", action="store_true", help="Sync results to OCI")
    
    args = parser.parse_args()
    
    if "-" in args.years:
        start, end = map(int, args.years.split("-"))
        target_years = list(range(start, end + 1))
    else:
        target_years = [int(args.years)]
        
    batch_calculate_matchups(target_years, sync_oci=args.sync)
