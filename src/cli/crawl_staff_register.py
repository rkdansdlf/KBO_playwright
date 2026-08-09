"""KBO Staff Register CLI.

Crawl the current day's manager and coaching staff registered on KBO Register.aspx
and upsert them to the configured database.

"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import TYPE_CHECKING

from src.crawlers.staff_register_crawler import KBO_TEAM_MAP, StaffRegisterCrawler

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

async def run_crawler(args: argparse.Namespace) -> int:
    # 1. Determine team codes to crawl
    """Run crawler.

    Args:
        args: Positional arguments to pass through.
        args: Args.

    Returns:
        Integer result.

    """
    if args.all_teams:
        team_codes = list(KBO_TEAM_MAP.keys())
    elif args.team:
        team_upper = args.team.upper()
        if team_upper not in KBO_TEAM_MAP:
            logger.error("❌ Invalid team code: %s. Must be one of: %s", args.team, list(KBO_TEAM_MAP.keys()))
            return 1
        team_codes = [team_upper]
    else:
        logger.error("❌ Please specify either --team <TEAM_CODE> or --all-teams.")
        return 1

    logger.info("🚀 Starting KBO Staff Register Crawler for teams: %s", team_codes)
    logger.info("   Dry run: %s", args.dry_run)

    # 2. Instantiate and run crawler
    crawler = StaffRegisterCrawler(headless=True)
    records = await crawler.crawl_all_teams(team_codes=team_codes)

    logger.info("📊 Crawled %s staff records.", len(records))

    # 3. Save to local SQLite
    crawler.save_to_db(records, dry_run=args.dry_run)

    logger.info("🏁 Roster crawling completed.")
    return 0


def main(argv: Sequence[str] | None = None) -> None:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
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
    args = parser.parse_args(argv)

    # Run async main loop
    status = asyncio.run(run_crawler(args))
    sys.exit(status)


if __name__ == "__main__":
    main()
