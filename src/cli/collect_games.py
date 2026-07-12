"""Unified Game Data Collector (Details + optional direct relay fallback)."""

from __future__ import annotations

import argparse
import asyncio
import logging
from typing import TYPE_CHECKING

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.naver_relay_crawler import NaverRelayCrawler
from src.db.engine import SessionLocal
from src.services.game_collection_service import (
    GameCollectionConfig,
    crawl_and_save_game_details,
    load_game_targets_by_ids,
    load_game_targets_from_db,
)
from src.utils.team_codes import normalize_kbo_game_id

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def _parse_game_ids(value: str | None) -> list[str]:
    if not value:
        return []
    return [normalize_kbo_game_id(t.strip()) for t in value.split(",") if t.strip()]


async def collect_games(
    year: int,
    month: int | None = None,
    game_ids: list[str] | None = None,
    *,
    force: bool = False,
    concurrency: int | None = None,
) -> None:
    """Handle the collect games operation.

    Args:
        year: Season year.
        month: Month.
        game_ids: Game Ids.
        force: If True, forces the operation even if data already exists.
        concurrency: Maximum number of concurrent requests.
        year: Season year.
        month: Month number (1-12).
        game_ids: Game Ids.

    """
    targets = load_game_targets_by_ids(game_ids) if game_ids else load_game_targets_from_db(year, month)

    logger.info(
        "Target: %s games%s",
        len(targets),
        f" for {year}" + (f"-{month}" if month else "") if not game_ids else "",
    )

    session = SessionLocal()
    try:
        from src.services.player_id_resolver import PlayerIdResolver

        resolver = PlayerIdResolver(
            session,
            strict_game_resolution=True,
            allow_auto_register=False,
        )
        resolver.preload_season_index(year)

        detail_crawler = GameDetailCrawler(request_delay=1.0, resolver=resolver)
        relay_crawler = NaverRelayCrawler()

        result = await crawl_and_save_game_details(
            targets,
            detail_crawler=detail_crawler,
            config=GameCollectionConfig(
                relay_crawler=relay_crawler,
                force=force,
                concurrency=concurrency,
                pause_every=10,
                pause_seconds=2.0,
                log=logger.info,
            ),
        )
        logger.info(
            "[FINISH] detail_saved=%s detail_failed=%s detail_skipped=%s relay_games=%s relay_rows=%s relay_skipped=%s",
            result.detail_saved,
            result.detail_failed,
            result.detail_skipped_existing,
            result.relay_saved_games,
            result.relay_rows_saved,
            result.relay_skipped_existing,
        )
    finally:
        session.close()


def main(argv: Sequence[str] | None = None) -> None:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = argparse.ArgumentParser(
        description=(
            "Collect game details and direct Naver relay fallback rows. "
            "For completed-game relay recovery, prefer scripts/fetch_kbo_pbp.py."
        ),
    )
    parser.add_argument("--year", type=int, required=True, help="Target Year (e.g. 2024)")
    parser.add_argument("--month", type=int, help="Target Month (Optional)")
    parser.add_argument("--game-ids", type=str, help="Specific game IDs to crawl, comma separated")
    parser.add_argument("--concurrency", type=int, default=None, help="Max concurrent game detail crawls")
    parser.add_argument("--force", action="store_true", help="Recrawl and overwrite existing detail/relay rows")
    args = parser.parse_args(argv)

    game_ids = _parse_game_ids(args.game_ids)
    asyncio.run(
        collect_games(args.year, month=args.month, game_ids=game_ids, force=args.force, concurrency=args.concurrency),
    )


if __name__ == "__main__":
    main()
