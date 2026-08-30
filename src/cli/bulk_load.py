"""CLI command for high-throughput KBO parallel chunk loading and checkpoint management."""

from __future__ import annotations

import argparse
import json
import sys

from src.services.bulk_loader import BulkChunkLoader, CheckpointManager


def build_parser() -> argparse.ArgumentParser:
    """Build argument parser for bulk_load CLI."""
    parser = argparse.ArgumentParser(
        description="High-throughput parallel chunk loader with atomic checkpoints for KBO data.",
    )
    parser.add_argument(
        "--category",
        type=str,
        default="pbp",
        choices=["pbp", "boxscore", "season_stats", "schedule", "all"],
        help="Target data category for bulk loading (default: pbp).",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2020,
        help="Start season year (default: 2020).",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=2024,
        help="End season year (default: 2024).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Number of concurrent worker threads (default: 4).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1,
        help="Number of years grouped in a single chunk (default: 1).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume execution from existing checkpoint manifest if available.",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Delete existing checkpoint before starting.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output result as JSON instead of terminal ASCII summary.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Execute main parallel bulk loader CLI workflow."""
    parser = build_parser()
    args = parser.parse_args(argv)

    chk_mgr = CheckpointManager()
    job_id = f"{args.category}_{args.start_year}_{args.end_year}"

    if args.reset_checkpoint:
        chk_mgr.delete_manifest(job_id)

    loader = BulkChunkLoader(
        checkpoint_manager=chk_mgr,
        concurrency=args.concurrency,
    )

    manifest = loader.run_bulk_load(
        category=args.category,
        start_year=args.start_year,
        end_year=args.end_year,
        chunk_size=args.chunk_size,
        resume=args.resume,
    )

    if args.json:
        print(json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2))  # noqa: T201
    else:
        print(manifest.to_ascii_summary())  # noqa: T201

    return 1 if manifest.failed_partitions > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
