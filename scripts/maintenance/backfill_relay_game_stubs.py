from __future__ import annotations

import argparse
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.repositories.game_repository import backfill_missing_game_stubs_for_relays


def main():
    parser = argparse.ArgumentParser(description="Backfill missing parent game rows for relay-bearing game_ids")
    parser.add_argument(
        "--seasons",
        type=str,
        help="Comma separated season years to limit the backfill (e.g. 2024,2025,2026)",
    )
    parser.add_argument(
        "--sync-to-oci",
        action="store_true",
        help="Trigger AUTO_SYNC_OCI-compatible per-game sync after inserting stubs",
    )
    args = parser.parse_args()

    seasons = None
    if args.seasons:
        seasons = [int(token.strip()) for token in args.seasons.split(",") if token.strip()]

    inserted = backfill_missing_game_stubs_for_relays(
        seasons=seasons,
        sync_to_oci=args.sync_to_oci,
    )
    print(f"Inserted {inserted} missing game stubs")


if __name__ == "__main__":
    main()
