from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Sequence

from src.services.relay_recovery_service import (
    load_relay_recovery_targets,
    recover_relay_data,
)
from src.utils.safe_print import safe_print as print


async def run_fetcher(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Deprecated KBO play-by-play fetcher alias. Prefer "
            "`python scripts/fetch_kbo_pbp.py ...` for completed-game relay recovery."
        )
    )
    parser.add_argument("--season", type=int, help="Season year to fetch (e.g. 2024)")
    parser.add_argument("--month", type=int, help="Optional month to filter games")
    parser.add_argument("--game-id", type=str, help="Specific Game ID to fetch (e.g. 20240323SSHH0)")
    parser.add_argument("--concurrency", type=int, default=1, help="Deprecated compatibility option")
    parser.add_argument("--force", action="store_true", help="Process games even if relay rows already exist")
    args = parser.parse_args(argv)

    print(
        "[DEPRECATED] src.cli.fetch_kbo_pbp is a compatibility alias. "
        "Prefer `python scripts/fetch_kbo_pbp.py` for completed-game relay recovery."
    )
    if args.concurrency != 1:
        print("[WARN] --concurrency is ignored by the shared relay recovery service.")

    if not args.season and not args.game_id:
        print("[ERROR] Must provide --season or --game-id")
        return 1

    try:
        targets = load_relay_recovery_targets(
            season=args.season,
            month=args.month,
            game_ids=[args.game_id] if args.game_id else None,
            missing_only=not args.force,
            log=print,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"[ERROR] {exc}")
        return 1

    if not targets:
        print("[INFO] No games found to process.")
        return 0

    await recover_relay_data(targets, log=print)
    print("\n[INFO] Relay recovery run completed.")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(run_fetcher(argv))


if __name__ == "__main__":
    raise SystemExit(main())
