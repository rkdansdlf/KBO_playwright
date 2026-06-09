"""
Fix Player Names - Re-crawl all players from KBO website

This script:
1. Clears bad data from player_basic table
2. Re-crawls all players from KBO website
3. Saves correct player names to database
4. Optionally syncs to OCI

Usage:
    python fix_player_names.py --crawl --save
    python fix_player_names.py --crawl --save --sync-oci
"""

import logging

logger = logging.getLogger(__name__)

import argparse
import asyncio
import os

from sqlalchemy import text

from src.crawlers.player_search_crawler import crawl_all_players, player_row_to_dict
from src.db.engine import SessionLocal, init_db
from src.repositories.player_basic_repository import PlayerBasicRepository


async def main():
    parser = argparse.ArgumentParser(description="Fix player names by re-crawling")
    parser.add_argument("--crawl", action="store_true", help="Crawl players from website")
    parser.add_argument("--save", action="store_true", help="Save to SQLite database")
    parser.add_argument("--sync-oci", action="store_true", help="Sync to OCI after crawl")
    parser.add_argument("--max-pages", type=int, help="Limit number of pages (for testing)")
    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("🔧 FIX PLAYER NAMES - Re-crawl from KBO Website")
    logger.info("=" * 70)

    if not args.crawl:
        logger.warning("\n⚠️  Use --crawl flag to start crawling")
        logger.info("   Example: python fix_player_names.py --crawl --save")
        return

    # Initialize database
    logger.info("\n📦 Initializing database...")
    init_db()

    # Crawl players
    logger.info("\n🕷️  Crawling players from KBO website...")
    if args.max_pages:
        logger.info(f"   (Limited to {args.max_pages} pages for testing)")

    players = await crawl_all_players(max_pages=args.max_pages, headless=True, request_delay=1.5)

    logger.info(f"\n✅ Crawled {len(players)} players")

    if not players:
        logger.error("❌ No players collected!")
        return

    # Validate names
    logger.info("\n🔍 Validating player names...")
    valid_players = []
    invalid_players = []

    for p in players:
        if p.name and p.name.strip() and p.name not in ["Unknown Player", "Unknown", "-", "N/A"]:
            valid_players.append(p)
        else:
            invalid_players.append(p)

    logger.info(f"   ✅ Valid: {len(valid_players)}")
    logger.error(f"   ❌ Invalid: {len(invalid_players)}")

    if invalid_players:
        logger.warning("\n⚠️  Invalid player names found:")
        for p in invalid_players[:10]:
            logger.info(f"   - player_id={p.player_id}, name='{p.name}'")

    # Show sample
    logger.info("\n📋 Sample (first 10 valid players):")
    for p in valid_players[:10]:
        logger.info(f"   - {p.name} (ID: {p.player_id}, #{p.uniform_no}, {p.team}/{p.position})")

    if args.save:
        # Convert to dicts
        logger.info("\n🔄 Converting to database format...")
        player_dicts = [player_row_to_dict(p) for p in valid_players]

        # Save to database
        logger.info(f"\n💾 Saving {len(player_dicts)} players to SQLite...")
        repo = PlayerBasicRepository()

        try:
            saved_count = repo.upsert_players(player_dicts)
            logger.info(f"✅ Saved {saved_count} players to SQLite")

            # Verify
            logger.info("\n🔍 Verifying database...")
            total = repo.count()
            logger.info(f"   Total players in database: {total}")

            # Check for any remaining "Unknown Player" entries
            with SessionLocal() as session:
                unknown_count = session.execute(
                    "SELECT COUNT(*) FROM player_basic WHERE name = 'Unknown Player'"
                ).scalar()
                if unknown_count > 0:
                    logger.warning(f"   ⚠️  Still have {unknown_count} 'Unknown Player' entries!")
                else:
                    logger.info("   ✅ No 'Unknown Player' entries found")

                # Show sample from database
                sample = session.execute(
                    "SELECT player_id, name, team, position FROM player_basic ORDER BY player_id LIMIT 10"
                ).fetchall()

                logger.info("\n📋 Sample from database:")
                for row in sample:
                    logger.info(f"   - {row[1]} (ID: {row[0]}, {row[2]}/{row[3]})")

        except Exception as e:  # noqa: BLE001
            logger.error(f"❌ Error saving to database: {e}")
            import traceback

            traceback.print_exc()
            return
    else:
        logger.warning("\n⚠️  Skipping save (use --save flag to save to database)")

    if args.sync_oci:
        oci_url = os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")

        if not oci_url:
            logger.error("\n❌ OCI_DB_URL not set; cannot sync to OCI")
            return

        logger.info("\n🔄 Syncing to OCI...")
        from src.sync.oci_sync import OCISync

        with SessionLocal() as sqlite_session:
            sync = OCISync(oci_url, sqlite_session)
            try:
                if not sync.test_connection():
                    logger.error("❌ OCI connection failed")
                    return

                synced = sync.sync_player_basic()
                logger.info(f"✅ Synced {synced} players to OCI")

                # Verify OCI
                logger.info("\n🔍 Verifying OCI...")
                unknown_count = sync.target_session.execute(
                    text("SELECT COUNT(*) FROM player_basic WHERE name = 'Unknown Player'")
                ).scalar()

                if unknown_count > 0:
                    logger.warning(f"   ⚠️  OCI still has {unknown_count} 'Unknown Player' entries!")
                else:
                    logger.info("   ✅ No 'Unknown Player' entries in OCI")

            finally:
                sync.close()
    else:
        logger.info("\nℹ️  Skipping OCI sync (use --sync-oci flag to sync)")

    logger.info("\n" + "=" * 70)
    logger.info("✅ Complete!")
    logger.info("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
