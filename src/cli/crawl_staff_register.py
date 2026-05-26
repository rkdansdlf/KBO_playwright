"""
KBO Staff Register CLI
Crawls the current day's manager and coaching staff registered on KBO Register.aspx,
upserts them to the local SQLite DB (player_basic table), and optionally synchronizes to OCI.
"""
from __future__ import annotations

import logging
import argparse
import asyncio
import os
import sys
from typing import Sequence

from src.crawlers.staff_register_crawler import StaffRegisterCrawler, KBO_TEAM_MAP
from src.utils.safe_print import safe_print as print

logger = logging.getLogger(__name__)


async def run_crawler(args: argparse.Namespace) -> int:
    # 1. Determine team codes to crawl
    if args.all_teams:
        team_codes = list(KBO_TEAM_MAP.keys())
    elif args.team:
        team_upper = args.team.upper()
        if team_upper not in KBO_TEAM_MAP:
            print(f"❌ Invalid team code: {args.team}. Must be one of: {list(KBO_TEAM_MAP.keys())}")
            return 1
        team_codes = [team_upper]
    else:
        print("❌ Please specify either --team <TEAM_CODE> or --all-teams.")
        return 1

    print(f"🚀 Starting KBO Staff Register Crawler for teams: {team_codes}")
    print(f"   Dry run: {args.dry_run}")
    
    # 2. Instantiate and run crawler
    crawler = StaffRegisterCrawler(headless=True)
    records = await crawler.crawl_all_teams(team_codes=team_codes)
    
    print(f"📊 Crawled {len(records)} staff records.")

    # 3. Save to local SQLite
    upserted_count = crawler.save_to_db(records, dry_run=args.dry_run)

    # 4. Optional OCI Synchronization
    if args.sync_oci and not args.dry_run:
        oci_url = os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
        if not oci_url:
            print("⚠️ sync-oci requested, but OCI_DB_URL/TARGET_DATABASE_URL env var not found. Skipping OCI sync.")
        else:
            player_ids = [r["player_id"] for r in records if r.get("player_id")]
            if player_ids:
                print(f"🔄 Synchronizing {len(player_ids)} staff records to OCI...")
                from src.db.engine import SessionLocal
                from src.sync.oci_sync import OCISync
                with SessionLocal() as session:
                    syncer = OCISync(oci_url, session)
                    try:
                        synced_count = syncer.sync_player_basic_by_ids(player_ids)
                        print(f"✅ Successfully synchronized {synced_count} player_basic records to OCI.")
                    except Exception:
                        logger.exception("❌ Failed to sync player basic records to OCI")
                    finally:
                        syncer.close()
            else:
                print("ℹ️ No valid player IDs found to sync to OCI.")

    print("🏁 Roster crawling completed.")
    return 0


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Crawl KBO Manager & Coach roster registration")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--team",
        type=str,
        help="Specific KBO team code to crawl (e.g. LG, KT, WO, NC, LT, OB, SS, HT, SK, HH)",
    )
    group.add_argument(
        "--all-teams",
        action="store_true",
        help="Crawl all 10 KBO teams",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Crawl and output statistics without writing to database",
    )
    parser.add_argument(
        "--sync-oci",
        action="store_true",
        help="Synchronize crawled and updated player_basic records to OCI",
    )

    args = parser.parse_args(argv)
    
    # Run async main loop
    status = asyncio.run(run_crawler(args))
    sys.exit(status)


if __name__ == "__main__":
    main()
