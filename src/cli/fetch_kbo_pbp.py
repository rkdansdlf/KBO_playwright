from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import TYPE_CHECKING

from src.services.relay_recovery_service import (
    load_relay_recovery_targets,
    recover_relay_data,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


async def run_fetcher(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Deprecated KBO play-by-play fetcher alias. Prefer "
            "`python scripts/fetch_kbo_pbp.py ...` for completed-game relay recovery."
        ),
    )
    parser.add_argument("--season", type=int, help="Season year to fetch (e.g. 2024)")
    parser.add_argument("--month", type=int, help="Optional month to filter games")
    parser.add_argument("--game-id", type=str, help="Specific Game ID to fetch (e.g. 20240323SSHH0)")
    parser.add_argument("--concurrency", type=int, default=1, help="Deprecated compatibility option")
    parser.add_argument("--force", action="store_true", help="Process games even if relay rows already exist")
    parser.add_argument(
        "--validate-final-score",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Validate final event score against game boxscore (defaults to True)",
    )
    parser.add_argument(
        "--validate-inning-continuity",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Validate inning continuity (defaults to True)",
    )
    args = parser.parse_args(argv)

    logger.info(
        "[DEPRECATED] src.cli.fetch_kbo_pbp is a compatibility alias. "
        "Prefer `python scripts/fetch_kbo_pbp.py` for completed-game relay recovery.",
    )
    if args.concurrency != 1:
        logger.info("[WARN] --concurrency is ignored by the shared relay recovery service.")

    if not args.season and not args.game_id:
        logger.info("[ERROR] Must provide --season or --game-id")
        return 1

    try:
        targets = load_relay_recovery_targets(
            season=args.season,
            month=args.month,
            game_ids=[args.game_id] if args.game_id else None,
            missing_only=not args.force,
            log=logger.info,
        )
    except (FileNotFoundError, ValueError):
        logger.exception("[ERROR] Failed to fetch KBO PBP")
        return 1

    if not targets:
        logger.info("[INFO] No games found to process.")
        return 0

    await recover_relay_data(
        targets,
        validate_final_score=args.validate_final_score,
        validate_inning_continuity=args.validate_inning_continuity,
        log=logger.info,
    )
    logger.info("\n[INFO] Relay recovery run completed.")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(run_fetcher(argv))


if __name__ == "__main__":
    raise SystemExit(main())
