"""
KBO Advanced Daily Data Update Orchestrator.
Fetches fielding, baserunning, and team-level cumulative stats.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from src.crawlers.baserunning_stats_crawler import crawl_baserunning_stats
from src.crawlers.fielding_stats_crawler import crawl_all_fielding_stats
from src.crawlers.team_batting_stats_crawler import TeamBattingStatsCrawler
from src.crawlers.team_pitching_stats_crawler import TeamPitchingStatsCrawler
from src.db.engine import SessionLocal
from src.repositories.player_stats_repository import PlayerSeasonBaserunningRepository, PlayerSeasonFieldingRepository
from src.sync.oci_sync import OCISync

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")


async def run_advanced_update(
    year: int,
    sync: bool = False,
    headless: bool = True,
) -> None:
    logger.info(f"\n{'=' * 60}")  # noqa: G004
    logger.info("🚀 KBO Advanced Daily Sync Started for Year: %s", year)
    logger.info(f"{'=' * 60}")  # noqa: G004

    any_error = False

    # 1. Fielding Stats
    logger.info("\n🛡️ Step 1: Crawling Fielding Stats...")
    try:
        fielding_records = await asyncio.to_thread(crawl_all_fielding_stats, year)
        if fielding_records:
            from src.models.player import PlayerSeasonFielding

            valid_cols = {c.key for c in PlayerSeasonFielding.__table__.columns}

            # Adapt and filter records
            processed = []
            for r in fielding_records:
                if not r.get("player_id"):
                    continue
                # Map and filter
                payload = {k: v for k, v in r.items() if k in valid_cols}
                processed.append(payload)

            fielding_repo = PlayerSeasonFieldingRepository()
            saved = fielding_repo.upsert_many(processed)
            logger.info("   ✅ Saved %s fielding records", saved)
    except Exception:
        logger.exception("   ❌ Error crawling fielding stats")
        any_error = True

    # 2. Baserunning Stats
    logger.info("\n🏃 Step 2: Crawling Baserunning Stats...")
    try:
        baserunning_records = await asyncio.to_thread(crawl_baserunning_stats, year)
        if baserunning_records:
            from src.models.player import PlayerSeasonBaserunning

            valid_cols = {c.key for c in PlayerSeasonBaserunning.__table__.columns}

            processed = []
            for r in baserunning_records:
                if not r.get("player_id"):
                    continue
                payload = {k: v for k, v in r.items() if k in valid_cols}
                processed.append(payload)

            baserunning_repo = PlayerSeasonBaserunningRepository()
            saved = baserunning_repo.upsert_many(processed)
            logger.info("   ✅ Saved %s baserunning records", saved)
    except Exception:
        logger.exception("   ❌ Error crawling baserunning stats")
        any_error = True

    # 3. Team Batting Stats
    logger.info("\n🏏 Step 3: Crawling Team Batting Stats...")
    try:
        batting_crawler = TeamBattingStatsCrawler()
        batting_stats = await asyncio.to_thread(batting_crawler.crawl, year, persist=True, headless=headless)
        logger.info("   ✅ Saved %s team batting records", len(batting_stats))
    except Exception:
        logger.exception("   ❌ Error crawling team batting stats")
        any_error = True

    # 4. Team Pitching Stats
    logger.info("\n⚾ Step 4: Crawling Team Pitching Stats...")
    try:
        pitching_crawler = TeamPitchingStatsCrawler()
        pitching_stats = await asyncio.to_thread(pitching_crawler.crawl, year, persist=True, headless=headless)
        logger.info("   ✅ Saved %s team pitching records", len(pitching_stats))
    except Exception:
        logger.exception("   ❌ Error crawling team pitching stats")
        any_error = True

    # 5. Team Defense Aggregation (from player-level data)
    logger.info("\n🏰 Step 5: Aggregating Team Fielding & Baserunning...")
    try:
        from src.aggregators.team_fielding_aggregator import TeamFieldingAggregator
        from src.models.team import Team

        with SessionLocal() as session:
            active_teams = [t.team_id for t in session.query(Team.team_id).filter(Team.is_active).all()]
            agg = TeamFieldingAggregator(session)
            agg.run_all(year, active_teams)
        logger.info("   ✅ Team defense aggregated for %s teams", len(active_teams))
    except Exception:
        logger.exception("   ❌ Error aggregating team defense stats")
        any_error = True

    # 6. Recalculate Rankings
    logger.info("\n🏷️ Step 6: Recalculating Stat Rankings...")
    try:
        from src.cli.calculate_rankings import rebuild_rankings

        saved_rankings = await asyncio.to_thread(rebuild_rankings, year)
        logger.info("   ✅ Recalculated %s ranking records", saved_rankings)
    except Exception:
        logger.exception("   ❌ Error recalculating rankings")
        any_error = True

    if sync:
        logger.info("\n☁️ Step 7: Synchronizing to OCI...")
        oci_url = os.getenv("OCI_DB_URL")
        if not oci_url:
            logger.warning("   ⚠️ OCI_DB_URL not set, skipping sync")
        else:
            with SessionLocal() as session:
                syncer = OCISync(oci_url, session)
                try:
                    syncer.sync_fielding_stats(year)
                    syncer.sync_baserunning_stats(year)
                    syncer.sync_team_season_batting(year)
                    syncer.sync_team_season_pitching(year)
                    syncer.sync_team_season_fielding(year)
                    syncer.sync_team_season_baserunning(year)
                    syncer.sync_stat_rankings(year)
                    logger.info("   ✅ OCI synchronization completed")
                except Exception:
                    logger.exception("   ❌ OCI sync error")
                    any_error = True
                finally:
                    syncer.close()

    logger.info(f"\n{'=' * 60}")  # noqa: G004
    logger.info("🏁 Advanced Daily Sync Finished for %s", year)
    logger.info(f"{'=' * 60}\n")  # noqa: G004

    if any_error:
        raise RuntimeError(f"Advanced Daily Sync finished with errors for {year}")


def main() -> int:
    parser = argparse.ArgumentParser(description="KBO Advanced Daily Data Orchestrator")
    parser.add_argument("--year", type=int, help="Target year. Defaults to current year.")
    parser.add_argument("--sync", action="store_true", help="Sync to OCI")
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="Run with browser UI")

    args = parser.parse_args()

    year = args.year or datetime.now(KST).year
    asyncio.run(run_advanced_update(year, sync=args.sync, headless=args.headless))


if __name__ == "__main__":
    main()
