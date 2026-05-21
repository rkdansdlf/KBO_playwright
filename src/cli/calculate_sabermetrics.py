import argparse
import os
from typing import List
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching, PlayerBasic
from src.models.team import Team
from src.aggregators.sabermetrics_calculator import SabermetricsCalculator
from src.cli.sync_oci import OCISync

def batch_calculate_sabermetrics(years: List[int], sync_oci: bool = False):
    """
    Batches through years and updates all players with advanced Sabermetrics.
    """
    with SessionLocal() as session:
        for year in years:
            print(f"📈 Calculating Sabermetrics for {year}...")
            
            try:
                lg = SabermetricsCalculator.get_league_constants(session, year)
                print(f"   League Constants: wOBA={lg['lg_woba']:.3f}, FIP_C={lg['fip_constant']:.2f}, R/PA={lg['lg_r_per_pa']:.3f}")
            except Exception as e:
                print(f"   ⚠️ Could not calculate league constants for {year}: {e}")
                continue

            # 1. Update Batting Sabermetrics
            batters = session.query(PlayerSeasonBatting).filter(
                PlayerSeasonBatting.season == year,
                PlayerSeasonBatting.player_id >= 10000
            ).all()
            for bat in batters:
                metrics = SabermetricsCalculator.calculate_batting_metrics(bat, lg)
                
                # Update extra_stats JSON
                extra = bat.extra_stats or {}
                extra.update(metrics)
                bat.extra_stats = extra
            
            print(f"   ✅ Updated {len(batters)} batters.")

            # 2. Update Pitching Sabermetrics
            pitchers = session.query(PlayerSeasonPitching).filter(
                PlayerSeasonPitching.season == year,
                PlayerSeasonPitching.player_id >= 10000
            ).all()
            for pit in pitchers:
                metrics = SabermetricsCalculator.calculate_pitching_metrics(pit, lg)
                
                # Update FIP column and extra_stats
                pit.fip = metrics['fip_adj']
                extra = pit.extra_stats or {}
                extra.update({
                    'fip_adj': metrics['fip_adj'],
                    'war': metrics['war']
                })
                pit.extra_stats = extra
            
            print(f"   ✅ Updated {len(pitchers)} pitchers.")
            
            session.commit()

    if sync_oci:
        print("🚀 Syncing updated Sabermetrics to OCI...")
        target_url = os.getenv('OCI_DB_URL')
        if target_url:
            with SessionLocal() as session:
                syncer = OCISync(target_url, session)
                syncer.sync_player_season_batting()
                syncer.sync_player_season_pitching()
            print("✅ Sync complete.")
        else:
            print("⚠️ OCI_DB_URL not set, skipping sync.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate Sabermetrics for players.")
    parser.add_argument("--years", type=str, default="2020-2026")
    parser.add_argument("--sync", action="store_true", help="Sync results to OCI")
    
    args = parser.parse_args()
    
    if "-" in args.years:
        start, end = map(int, args.years.split("-"))
        target_years = list(range(start, end + 1))
    else:
        target_years = [int(args.years)]
        
    batch_calculate_sabermetrics(target_years, sync_oci=args.sync)
