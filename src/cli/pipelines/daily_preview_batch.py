"""Daily Preview Batch Script.

Fetch pre-game context and persists both preview JSON and core pregame tables.

"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime
from typing import TYPE_CHECKING

from src.constants import KST
from src.crawlers.preview_crawler import PreviewCrawler
from src.services.pregame_context_writer import save_preview_contexts
from src.utils.refresh_manifest import write_refresh_manifest

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def _write_pregame_manifest(target_date: str, game_ids: list[str]) -> str:
    return write_refresh_manifest(  # type: ignore[return-value]
        phase="pregame",
        target_date=target_date,
        game_ids=game_ids,
        datasets=["game", "game_metadata", "game_lineups", "game_summary"],
    )


async def run_preview_batch(target_date: str) -> list[str]:
    """Run preview batch.

    Args:
        target_date: Target date for the operation.

    Returns:
        List of results.

    """
    logger.info("🚀 Starting Preview Data Batch for %s...", target_date)

    crawler = PreviewCrawler(request_delay=1.0)
    previews = await crawler.run(target_date)
    if not previews:
        manifest_path = _write_pregame_manifest(target_date, [])
        logger.info("[info] No preview data found. manifest=%s", manifest_path)
        return []

    saved_ids = save_preview_contexts(previews, target_date)

    manifest_path = _write_pregame_manifest(target_date, saved_ids)
    logger.info("✅ Pregame batch finished. saved=%s manifest=%s", len(saved_ids), manifest_path)
    return saved_ids


def main(argv: Sequence[str] | None = None) -> int:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = argparse.ArgumentParser(description="KBO Daily Preview Crawler")

    parser.add_argument("--date", type=str, help="Target date (YYYYMMDD). Defaults to today.", default=None)
    args = parser.parse_args(argv)

    target = args.date or datetime.now(KST).strftime("%Y%m%d")
    asyncio.run(run_preview_batch(target))
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
