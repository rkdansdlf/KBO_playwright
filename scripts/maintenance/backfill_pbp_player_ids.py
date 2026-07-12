#!/usr/bin/env python3
"""Batch-resolve NULL player_ids in game_play_by_play table.

Uses PlayerIdResolver and game_relay helper logic to resolve player IDs.

Usage:
    python scripts/maintenance/backfill_pbp_player_ids.py                # all years
    python scripts/maintenance/backfill_pbp_player_ids.py --year 2025    # single year
    python scripts/maintenance/backfill_pbp_player_ids.py --dry-run      # preview only
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path.cwd()))

from src.db.engine import SessionLocal
from src.repositories.game_relay import _relay_resolution_context, _resolve_pbp_player

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PBP_RESOLUTION_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, LookupError, OSError)


def _write_updates(session, updates: list[tuple]) -> None:
    if not updates:
        logger.info("   No rows were resolved.")
        return
    logger.info("   Writing %s resolved player_ids to DB...", len(updates))
    for i in range(0, len(updates), 500):
        for pid, conf, reason, unresolved, rid in updates[i : i + 500]:
            session.execute(
                text("""
                    UPDATE game_play_by_play
                    SET player_id = :pid, resolver_confidence = :conf, resolver_reason = :reason,
                        unresolved_player_name = :unresolved, updated_at = datetime('now')
                    WHERE id = :rid
                """),
                {"pid": pid, "conf": conf, "reason": reason, "unresolved": unresolved, "rid": rid},
            )
    session.commit()
    logger.info("   Successfully updated %s rows.", len(updates))


def backfill_year(session, year: int, dry_run: bool = False) -> None:
    """Resolve NULL player_ids for game_play_by_play in a single season year."""
    logger.info("📅 Resolving PBP player IDs for year %s...", year)
    rows = session.execute(
        text("""
            SELECT id, game_id, inning, inning_half, pitcher_name, batter_name, play_description
            FROM game_play_by_play
            WHERE player_id IS NULL AND game_id LIKE :prefix
              AND (batter_name IS NOT NULL OR pitcher_name IS NOT NULL)
            ORDER BY game_id, id
        """),
        {"prefix": f"{year}%"},
    ).fetchall()
    if not rows:
        logger.info("   No NULL player_id rows found for %s.", year)
        return
    logger.info("   Found %s NULL player_id rows.", len(rows))
    current_game_id: str | None = None
    resolution = None
    resolved_count = 0
    updates: list[tuple[int | None, str | None, str | None, str | None, int]] = []
    for row in rows:
        rid, game_id, inning, inning_half, pitcher_name, batter_name, play_description = row
        if game_id != current_game_id:
            current_game_id = game_id
            try:
                resolution = _relay_resolution_context(session, game_id)
            except PBP_RESOLUTION_EXCEPTIONS as e:
                logger.warning("   Failed to create resolution context for game %s: %s", game_id, e)
                resolution = None
        if resolution is None:
            continue
        pbp_dict = {
            "inning": inning,
            "inning_half": inning_half,
            "pitcher_name": pitcher_name,
            "batter_name": batter_name,
            "play_description": play_description,
        }
        try:
            player_id, confidence, reason, unresolved_name = _resolve_pbp_player(pbp_dict, resolution)
            if player_id:
                updates.append((player_id, confidence, reason, unresolved_name, rid))
                resolved_count += 1
                if dry_run and resolved_count <= 20:
                    logger.info(
                        "   [DRY-RUN MATCH] %s | Inn: %s %s | B: %s / P: %s -> ID: %s (%s)",
                        game_id,
                        inning,
                        inning_half,
                        batter_name,
                        pitcher_name,
                        player_id,
                        reason,
                    )
        except PBP_RESOLUTION_EXCEPTIONS as e:
            logger.debug("Failed to resolve row %s: %s", rid, e)
    if dry_run:
        logger.info("   [DRY-RUN SUMMARY] Would resolve %s/%s rows.", resolved_count, len(rows))
        return
    _write_updates(session, updates)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill NULL player_ids in game_play_by_play")
    parser.add_argument("--year", type=int, help="Single year")
    parser.add_argument("--start", type=int, default=2001, help="Start year")
    parser.add_argument("--end", type=int, default=2026, help="End year")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    args = parser.parse_args()

    years = [args.year] if args.year else list(range(args.start, args.end + 1))

    logger.info("🚀 Starting PBP player_id backfill for years: %s-%s", years[0], years[-1])
    if args.dry_run:
        logger.info("   ⚠️  DRY RUN — no DB writes")

    with SessionLocal() as session:
        for year in years:
            backfill_year(session, year, args.dry_run)


if __name__ == "__main__":
    main()
