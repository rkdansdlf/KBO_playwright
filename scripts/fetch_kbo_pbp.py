import argparse
import asyncio
import os
import sys
from pathlib import Path

# Adjust sys.path to run from scripts/ folder easily.
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.services.relay_recovery_service import (
    DEFAULT_MANIFEST_PATH,
    load_relay_recovery_targets,
    parse_source_order,
    recover_relay_data,
)
from src.utils.safe_print import safe_print as print


async def run_fetcher(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KBO relay recovery fetcher")
    parser.add_argument("--season", type=int, help="Season year to fetch (e.g. 2024)")
    parser.add_argument("--date", type=str, help="Target date to fetch (e.g. 20240924)")
    parser.add_argument("--limit", type=int, help="Limit maximum games to process")
    parser.add_argument("--game-ids", type=str, help="Specific Game IDs to fetch, comma separated")
    parser.add_argument("--game-ids-file", type=str, help="Path to a file containing Game IDs, one per line")
    parser.add_argument("--dry-run", action="store_true", help="Parse relay data but do not save to DB")
    parser.add_argument(
        "--missing-only",
        action="store_true",
        default=True,
        help="Only process games missing events or play-by-play (default: true)",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite existing relay data")
    parser.add_argument("--source-order", type=str, help="Comma separated source order override")
    parser.add_argument(
        "--import-manifest",
        type=str,
        default=str(DEFAULT_MANIFEST_PATH),
        help="Manifest CSV path, or comma separated manifest CSV paths",
    )
    parser.add_argument("--bucket", type=str, help="Force one bucket ID for all selected games")
    parser.add_argument(
        "--source-timeout",
        type=float,
        default=30.0,
        help="Per-source timeout in seconds for probe/fetch attempts",
    )
    parser.add_argument(
        "--allow-derived-pbp",
        action="store_true",
        default=False,
        help="Allow game_play_by_play backfill from existing game_events",
    )
    parser.add_argument("--report-out", type=str, help="CSV report output path")
    args = parser.parse_args(argv)

    if not args.season and not args.date and not args.game_ids and not args.game_ids_file:
        print("[ERROR] Must provide --season, --date, --game-ids, or --game-ids-file")
        return 1

    if args.force:
        args.missing_only = False

    try:
        targets = load_relay_recovery_targets(
            season=args.season,
            date=args.date,
            game_ids=_parse_game_ids(args.game_ids),
            game_ids_file=args.game_ids_file,
            bucket=args.bucket,
            missing_only=args.missing_only,
            log=print,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"[ERROR] {exc}")
        return 1

    if not targets:
        print("[INFO] No games found to process.")
        return 0

    if args.limit:
        targets = targets[: args.limit]

    await recover_relay_data(
        targets,
        dry_run=args.dry_run,
        source_order_override=parse_source_order(args.source_order),
        import_manifest=args.import_manifest,
        source_timeout=args.source_timeout,
        allow_derived_pbp=args.allow_derived_pbp,
        report_out=Path(args.report_out) if args.report_out else None,
        log=print,
    )
    print("\n[INFO] Relay recovery run completed.")
    return 0


def _parse_game_ids(value: str | None) -> list[str]:
    if not value:
        return []
    return [token.strip() for token in value.split(",") if token.strip()]


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run_fetcher()))
