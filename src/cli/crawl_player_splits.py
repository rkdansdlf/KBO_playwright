"""CLI entrypoint for crawling player situational split statistics and backfilling."""

from __future__ import annotations

import argparse
import asyncio
import logging

from src.crawlers.player_splits_crawler import PlayerSplitsCrawler
from src.db.engine import get_db_session
from src.repositories.player_splits_repository import PlayerSplitsRepository
from src.services.rag_indexer import RagKnowledgeIndexer

logger = logging.getLogger(__name__)


async def run(args: argparse.Namespace) -> None:
    """Run player splits crawler CLI."""
    crawler = PlayerSplitsCrawler()

    if args.all_types:
        results = await crawler.crawl_all_splits(season=args.season)
    else:
        results = await crawler.crawl_player_splits(
            season=args.season,
            split_type=args.type,
        )

    logger.info("Crawled %d player split records for season %d.", len(results), args.season)

    if args.save:
        with get_db_session() as session:
            repo = PlayerSplitsRepository(session)
            for item in results:
                repo.save_splits_entry(item)
            logger.info("Saved %d player split records to DB.", len(results))

            if args.reindex_rag:
                indexer = RagKnowledgeIndexer(session)
                count = indexer.index_player_splits(season=args.season)
                logger.info("Reindexed %d player splits RAG chunks.", count)


def main() -> None:
    """Parse CLI args and execute."""
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Crawl KBO Player Situational Splits Stats")
    parser.add_argument("--season", type=int, default=2026, help="Target season year")
    parser.add_argument(
        "--type",
        type=str,
        default="scoring_position",
        help="Split type (scoring_position, vs_pitcher_type, runner_on_base)",
    )
    parser.add_argument("--all-types", action="store_true", help="Crawl all split types across categories for backfill")
    parser.add_argument("--save", action="store_true", help="Save records to database")
    parser.add_argument("--reindex-rag", action="store_true", help="Reindex player splits RAG chunks after saving")

    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
