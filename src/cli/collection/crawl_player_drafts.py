"""CLI entrypoint for crawling KBO Rookie Draft history."""

from __future__ import annotations

import argparse
import asyncio
import logging

from src.crawlers.draft_history_crawler import DraftHistoryCrawler
from src.db.engine import get_db_session
from src.repositories.player_draft_repository import PlayerDraftRepository

logger = logging.getLogger(__name__)


async def run(args: argparse.Namespace) -> None:
    """Run draft crawler CLI."""
    crawler = DraftHistoryCrawler()
    results = await crawler.crawl_draft_history(season=args.season)

    logger.info("Crawled %d draft records for season %d.", len(results), args.season)

    if args.save:
        with get_db_session() as session:
            repo = PlayerDraftRepository(session)
            for item in results:
                repo.save_draft_entry(item)
            logger.info("Saved %d draft records to DB.", len(results))


def main() -> None:
    """Parse CLI args and execute."""
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Crawl KBO Rookie Draft History")
    parser.add_argument("--season", type=int, default=2026, help="Target draft season year")
    parser.add_argument("--save", action="store_true", help="Save records to database")

    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
