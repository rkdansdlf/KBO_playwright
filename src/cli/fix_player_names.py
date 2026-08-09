"""CLI: Fix player names by re-crawling from KBO website.

Usage:
    python3 -m src.cli.fix_player_names --crawl --save
    python3 -m src.cli.fix_player_names --crawl --save --max-pages 1

"""

from __future__ import annotations

import argparse
import asyncio
import logging

from src.crawlers.player_search_crawler import crawl_all_players, player_row_to_dict
from src.db.engine import init_db
from src.repositories.player_basic_repository import PlayerBasicRepository
from src.utils.player_validation import filter_valid_player_payloads

logger = logging.getLogger(__name__)


def _configure_cli_logging() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")


async def _crawl_player_payloads(max_pages: int | None) -> list[dict]:
    logger.info("Crawling players from KBO website...")
    if max_pages:
        logger.info("  (limited to %d pages)", max_pages)
    players = await crawl_all_players(max_pages=max_pages)
    return [player_row_to_dict(player) for player in players]


def _filter_valid_players(player_dicts_raw: list[dict]) -> list[dict]:
    logger.info("Collected %d raw player records", len(player_dicts_raw))
    valid_dicts, filter_counts = filter_valid_player_payloads(player_dicts_raw)
    if filter_counts:
        for reason, count in filter_counts.most_common():
            logger.warning("  filtered: %s x%d", reason, count)
    logger.info("Valid players: %d / %d", len(valid_dicts), len(player_dicts_raw))
    return valid_dicts


def _log_player_sample(valid_dicts: list[dict]) -> None:
    logger.info("Sample (first 5):")
    for player in valid_dicts[:5]:
        logger.info(
            "  %s (ID: %s, %s/%s)",
            player["name"],
            player["player_id"],
            player.get("team"),
            player.get("position"),
        )


def _save_players_if_requested(valid_dicts: list[dict], *, save: bool) -> None:
    if not save:
        logger.info("Skipping save (use --save flag)")
        return
    logger.info("Saving %d players...", len(valid_dicts))
    saved = PlayerBasicRepository().upsert_players(valid_dicts)
    logger.info("Saved %d players", saved)


async def fix_player_names(
    max_pages: int | None = None,
    *,
    save: bool = False,
) -> None:
    """Fix player names.

    Args:
        max_pages: Max Pages.
        save: Whether to persist the results.
        max_pages: Max Pages.

    """
    logger.info("=" * 60)

    logger.info("Fix Player Names - Re-crawl from KBO Website")
    logger.info("=" * 60)

    init_db()

    player_dicts_raw = await _crawl_player_payloads(max_pages)
    if not player_dicts_raw:
        logger.info("No players collected from website")
        return

    valid_dicts = _filter_valid_players(player_dicts_raw)
    if not valid_dicts:
        logger.info("No valid players to save")
        return

    _log_player_sample(valid_dicts)
    _save_players_if_requested(valid_dicts, save=save)
    logger.info("=" * 60)
    logger.info("Complete")
    logger.info("=" * 60)


def main() -> int:
    """Run the main entry point for this CLI command."""
    _configure_cli_logging()
    parser = argparse.ArgumentParser(description="Fix player names by re-crawling from KBO website")
    parser.add_argument("--crawl", action="store_true", help="Crawl players from website")
    parser.add_argument("--save", action="store_true", help="Save to SQLite database")
    parser.add_argument("--max-pages", type=int, default=None, help="Limit number of pages (for testing)")
    args = parser.parse_args()

    if not args.crawl:
        logger.info("Use --crawl flag to start crawling")
        logger.info("  Example: python3 -m src.cli.fix_player_names --crawl --save")
        return 0

    asyncio.run(
        fix_player_names(
            max_pages=args.max_pages,
            save=args.save,
        ),
    )
    return 0


if __name__ == "__main__":
    main()
