"""
CLI entrypoint to execute Phase 1 supplementary crawlers.

broadcast, game MVP, injury/IL, foreign player, manager change, fan culture.

"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

logger = logging.getLogger(__name__)


async def run_broadcast(*, save: bool = False) -> None:
    """
    Run broadcast.

    Args:
        save: Whether to persist the results.
        save: Whether to persist the results.

    """
    from src.crawlers.broadcast_crawler import BroadcastCrawler

    crawler = BroadcastCrawler()
    await crawler.run(save=save)


async def run_game_mvp(game_ids: list[str] | None = None, *, save: bool = False) -> None:
    """
    Run game mvp.

    Args:
        game_ids: Game Ids.
        save: Whether to persist the results.
        game_ids: Game Ids.
        save: Whether to persist the results.
        game_ids: Game Ids.

    """
    from src.crawlers.game_mvp_crawler import GameMvpCrawler

    crawler = GameMvpCrawler()
    await crawler.run(game_ids=game_ids, save=save)


async def run_injury(*, save: bool = False) -> None:
    """
    Run injury.

    Args:
        save: Whether to persist the results.
        save: Whether to persist the results.

    """
    from src.crawlers.injury_crawler import InjuryCrawler

    crawler = InjuryCrawler()
    await crawler.run(save=save)


async def run_foreign_player(*, save: bool = False) -> None:
    """
    Run foreign player.

    Args:
        save: Whether to persist the results.
        save: Whether to persist the results.

    """
    from src.crawlers.foreign_player_crawler import ForeignPlayerCrawler

    crawler = ForeignPlayerCrawler()
    await crawler.run(save=save)


async def run_manager_change(*, save: bool = False) -> None:
    """
    Run manager change.

    Args:
        save: Whether to persist the results.
        save: Whether to persist the results.

    """
    from src.crawlers.manager_change_crawler import ManagerChangeCrawler

    crawler = ManagerChangeCrawler()
    await crawler.run(save=save)


async def run_fan_culture(*, save: bool = False) -> None:
    """
    Run fan culture.

    Args:
        save: Whether to persist the results.
        save: Whether to persist the results.

    """
    from src.crawlers.fan_culture_crawler import FanCultureCrawler

    crawler = FanCultureCrawler()
    await crawler.run(save=save)


def seed_stadium_info() -> None:
    """Seed stadium info."""
    from scripts.seed_stadium_info import seed_stadium_info

    seed_stadium_info()


async def run_all_crawlers(*, save: bool = False) -> None:
    """
    Run all news-based crawlers (broadcast, mvp, injury, foreign, manager).

    Args:
        save: Whether to persist the results.
        save: Whether to persist the results.

    """
    logger.info("Starting Phase 1 news-based crawlers...")

    await run_broadcast(save=save)
    await run_game_mvp(save=save)
    await run_injury(save=save)
    await run_foreign_player(save=save)
    await run_manager_change(save=save)
    logger.info("Phase 1 news-based crawlers complete.")


def run_all_seeds() -> None:
    """Run all seed data (stadium info, fan culture)."""
    logger.info("Starting Phase 1 seed data...")
    seed_stadium_info()
    logger.info("Phase 1 seed data complete.")


async def run_all(*, save: bool = False) -> None:
    """
    Run all Phase 1 crawlers and seeds (legacy compat).

    Args:
        save: Whether to persist the results.
        save: Whether to persist the results.

    """
    await run_all_crawlers(save=save)

    run_all_seeds()


def main() -> int:
    """Run the main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="Phase 1 Supplementary Crawlers")
    parser.add_argument(
        "--type",
        choices=["broadcast", "mvp", "injury", "foreign", "manager", "fan_culture", "seed_stadium", "crawlers", "all"],
        help="Crawler type to run",
    )
    parser.add_argument("--save", action="store_true", help="Save results to database")
    parser.add_argument("--game-ids", nargs="+", help="Specific game IDs (for mvp)")

    args = parser.parse_args()
    if not args.type:
        parser.print_help()
        sys.exit(1)

    runner_map = {
        "broadcast": lambda: run_broadcast(save=args.save),
        "mvp": lambda: run_game_mvp(game_ids=args.game_ids, save=args.save),
        "injury": lambda: run_injury(save=args.save),
        "foreign": lambda: run_foreign_player(save=args.save),
        "manager": lambda: run_manager_change(save=args.save),
        "fan_culture": lambda: run_fan_culture(save=args.save),
        "seed_stadium": seed_stadium_info,
        "crawlers": lambda: run_all_crawlers(save=args.save),
        "all": lambda: run_all(save=args.save),
    }

    runner = runner_map.get(args.type)
    if runner:
        result = runner()
        if asyncio.iscoroutine(result):
            asyncio.run(result)


if __name__ == "__main__":
    main()
