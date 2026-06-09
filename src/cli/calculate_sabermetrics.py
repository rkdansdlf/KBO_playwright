import argparse
import logging
import os
from collections.abc import Sequence

from src.aggregators.sabermetrics_calculator import SabermetricsCalculator
from src.cli.sync_oci import OCISync
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

logger = logging.getLogger(__name__)


def batch_calculate_sabermetrics(years: list[int], sync_oci: bool = False) -> None:
    """
    Batches through years and updates all players with advanced Sabermetrics.
    """
    with SessionLocal() as session:
        for year in years:
            logger.info(f"📈 Calculating Sabermetrics for {year}...")

            try:
                lg = SabermetricsCalculator.get_league_constants(session, year)
                logger.info(
                    "   League Constants: wOBA=%.3f, FIP_C=%.2f, R/PA=%.3f",
                    lg["lg_woba"],
                    lg["fip_constant"],
                    lg["lg_r_per_pa"],
                )
            except Exception:
                logger.exception(f"   ⚠️ Could not calculate league constants for {year}")
                continue

            # 1. Update Batting Sabermetrics
            batters = (
                session.query(PlayerSeasonBatting)
                .filter(PlayerSeasonBatting.season == year, PlayerSeasonBatting.player_id >= 10000)
                .all()
            )
            for bat in batters:
                metrics = SabermetricsCalculator.calculate_batting_metrics(bat, lg)

                # Update extra_stats JSON
                extra = bat.extra_stats or {}
                extra.update(metrics)
                bat.extra_stats = extra

            logger.info(f"   ✅ Updated {len(batters)} batters.")

            # 2. Update Pitching Sabermetrics
            pitchers = (
                session.query(PlayerSeasonPitching)
                .filter(PlayerSeasonPitching.season == year, PlayerSeasonPitching.player_id >= 10000)
                .all()
            )
            for pit in pitchers:
                metrics = SabermetricsCalculator.calculate_pitching_metrics(pit, lg)

                # Update FIP column and extra_stats
                pit.fip = metrics["fip_adj"]
                extra = pit.extra_stats or {}
                extra.update({"fip_adj": metrics["fip_adj"], "lob_pct": metrics.get("lob_pct"), "war": metrics["war"]})
                pit.extra_stats = extra

            logger.info(f"   ✅ Updated {len(pitchers)} pitchers.")

            session.commit()

    if sync_oci:
        logger.info("🚀 Syncing updated Sabermetrics to OCI...")
        target_url = os.getenv("OCI_DB_URL")
        if target_url:
            with SessionLocal() as session:
                syncer = OCISync(target_url, session)
                syncer.sync_player_season_batting()
                syncer.sync_player_season_pitching()
            logger.info("✅ Sync complete.")
        else:
            logger.warning("⚠️ OCI_DB_URL not set, skipping sync.")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Calculate Sabermetrics for players.")
    parser.add_argument("--years", type=str, default="2020-2026")
    parser.add_argument("--sync", action="store_true", help="Sync results to OCI")
    args = parser.parse_args(argv)

    if "-" in args.years:
        start, end = map(int, args.years.split("-"))
        target_years = list(range(start, end + 1))
    else:
        target_years = [int(args.years)]

    batch_calculate_sabermetrics(target_years, sync_oci=args.sync)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
