"""CLI: Fix player names by re-crawling from KBO website.

Usage:
    python3 -m src.cli.fix_player_names --crawl --save
    python3 -m src.cli.fix_player_names --crawl --save --sync-oci
    python3 -m src.cli.fix_player_names --crawl --save --max-pages 1
"""

import argparse
import asyncio
import logging
import os

from src.crawlers.player_search_crawler import crawl_all_players, player_row_to_dict
from src.db.engine import SessionLocal, get_oci_url, init_db
from src.repositories.player_basic_repository import PlayerBasicRepository
from src.utils.player_validation import filter_valid_player_payloads

logger = logging.getLogger(__name__)


async def fix_player_names(
    max_pages: int | None = None,
    save: bool = False,
    sync_oci: bool = False,
) -> None:
    logger.info("=" * 60)
    logger.info("Fix Player Names - Re-crawl from KBO Website")
    logger.info("=" * 60)

    init_db()

    logger.info("Crawling players from KBO website...")
    if max_pages:
        logger.info("  (limited to %d pages)", max_pages)

    players = await crawl_all_players(max_pages=max_pages)

    if not players:
        logger.info("No players collected from website")
        return

    player_dicts_raw = [player_row_to_dict(p) for p in players]
    logger.info("Collected %d raw player records", len(player_dicts_raw))

    valid_dicts, filter_counts = filter_valid_player_payloads(player_dicts_raw)
    if filter_counts:
        for reason, count in filter_counts.most_common():
            logger.warning("  filtered: %s x%d", reason, count)
    logger.info("Valid players: %d / %d", len(valid_dicts), len(player_dicts_raw))

    if not valid_dicts:
        logger.info("No valid players to save")
        return

    logger.info("Sample (first 5):")
    for d in valid_dicts[:5]:
        logger.info("  %s (ID: %s, %s/%s)", d["name"], d["player_id"], d.get("team"), d.get("position"))

    if save:
        logger.info("Saving %d players to SQLite...", len(valid_dicts))
        repo = PlayerBasicRepository()
        saved = repo.upsert_players(valid_dicts)
        logger.info("Saved %d players", saved)
    else:
        logger.info("Skipping save (use --save flag)")

    if not sync_oci:
        oci_url = get_oci_url()
        if oci_url:
            sync_oci = True

    if sync_oci:
        oci_url = get_oci_url()
        if not oci_url:
            logger.info("OCI_DB_URL not set; cannot sync to OCI")
            return

        logger.info("Syncing player_basic to OCI...")
        from src.sync.oci_sync import OCISync

        with SessionLocal() as sqlite_session:
            sync = OCISync(oci_url, sqlite_session)
            try:
                if not sync.test_connection():
                    logger.info("OCI connection failed")
                    return
                synced = sync.sync_player_basic()
                logger.info("Synced %d players to OCI", synced)
            finally:
                sync.close()

    logger.info("=" * 60)
    logger.info("Complete")
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Fix player names by re-crawling from KBO website")
    parser.add_argument("--crawl", action="store_true", help="Crawl players from website")
    parser.add_argument("--save", action="store_true", help="Save to SQLite database")
    parser.add_argument("--sync-oci", action="store_true", help="Sync to OCI after crawl")
    parser.add_argument("--max-pages", type=int, default=None, help="Limit number of pages (for testing)")
    args = parser.parse_args()

    if not args.crawl:
        logger.info("Use --crawl flag to start crawling")
        logger.info("  Example: python3 -m src.cli.fix_player_names --crawl --save")
        return

    asyncio.run(fix_player_names(
        max_pages=args.max_pages,
        save=args.save,
        sync_oci=args.sync_oci,
    ))


if __name__ == "__main__":
    main()
