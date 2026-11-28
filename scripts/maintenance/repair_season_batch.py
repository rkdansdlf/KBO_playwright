"""Batch repair historical season boxscore discrepancies."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from sqlalchemy import text

from scripts.maintenance.merge_duplicate_game_row import merge
from src.db.engine import Engine, SessionLocal
from src.services.boxscore_repair_service import (
    discover_candidates_from_integrity,
    repair_games_async,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("repair_season_batch")


def auto_merge_aliases(candidates_season: list[str]) -> None:
    """Merge duplicate alias games into their canonical rows."""
    with Engine.connect() as conn:
        for gid in candidates_season:
            row = conn.execute(
                text(
                    "SELECT alias_game_id, canonical_game_id FROM game_id_aliases "
                    "WHERE alias_game_id = :gid AND alias_game_id != canonical_game_id"
                ),
                {"gid": gid},
            ).fetchone()
            if not row:
                continue
            alias_gid, canonical_gid = row[0], row[1]
            has_alias = (
                conn.execute(
                    text("SELECT 1 FROM game WHERE game_id = :gid"),
                    {"gid": alias_gid},
                ).scalar_one_or_none()
                is not None
            )
            has_canonical = (
                conn.execute(
                    text("SELECT 1 FROM game WHERE game_id = :gid"),
                    {"gid": canonical_gid},
                ).scalar_one_or_none()
                is not None
            )
            if has_alias and has_canonical:
                print(f"🔄 [AUTO-MERGE] Merging duplicate {alias_gid} -> {canonical_gid}...")
                merge(alias_gid, canonical_gid, apply=True)


async def run_repair(season: str, concurrency: int) -> int:
    with SessionLocal() as session:
        all_candidates = discover_candidates_from_integrity(session)
        season_candidates = [c for c in all_candidates if c.game_date.startswith(season)]

    print("\n" + "=" * 60)
    print(f" Starting Batch Repair for {season} Season ({len(season_candidates)} games)")
    print("=" * 60)

    if not season_candidates:
        print(f"No candidates found for season {season}.")
        return 0

    report = await repair_games_async(season_candidates, dry_run=False, concurrency=concurrency)

    print("\n" + "=" * 60)
    print(f" {season} Season Batch Repair Report")
    print("=" * 60)
    print(f" Total candidates     : {report.total_candidates}")
    print(f" Repaired successfully: {report.repaired}")
    print(f" Failed crawls/saves  : {report.failed}")
    print(f" Discrepancies left   : {report.discrepancies}")
    print("-" * 60)
    for o in report.outcomes:
        symbol = "✅" if o.status == "repaired" else "❌"
        print(f" {symbol} {o.game_id} ({o.game_date}): {o.status}")
        for err in o.errors:
            print(f"    - {err}")
    print("=" * 60 + "\n")

    # Check for alias duplicate cleanup
    candidate_ids = [c.game_id for c in season_candidates]
    auto_merge_aliases(candidate_ids)

    # Re-verify remaining discrepancies for this season
    with SessionLocal() as session:
        remaining = [c for c in discover_candidates_from_integrity(session) if c.game_date.startswith(season)]
        print(f"🎯 Final Remaining Candidates for {season}: {len(remaining)}")
        for r in remaining:
            print(f"   - {r.game_id}: {r.reasons}")

    return 0 if len(remaining) == 0 else 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", required=True, help="Season year (e.g. 2012)")
    parser.add_argument("--concurrency", type=int, default=3, help="Async concurrency (default: 3)")
    args = parser.parse_args()

    exit_code = asyncio.run(run_repair(args.season, args.concurrency))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
