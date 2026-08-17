"""Backfill script to populate home_runs_allowed, walks_allowed, and era in game_pitching_stats from extra_stats JSON.

Usage:
    python3 -m scripts.maintenance.backfill_pitching_hr_bb --dry-run
    python3 -m scripts.maintenance.backfill_pitching_hr_bb --apply
    python3 -m scripts.maintenance.backfill_pitching_hr_bb --year 2025 --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

from src.db.engine import SessionLocal

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 5000


def _safe_int(val: Any) -> int | None:
    if val in (None, "", "-"):
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val: Any) -> float | None:
    if val in (None, "", "-"):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _extract_pitching_updates(
    current_hr: int | None,
    current_bb: int | None,
    current_era: float | None,
    extra: dict[str, Any],
) -> tuple[int | None, int | None, float | None, bool, bool, bool, bool]:
    """Extract updated HR, BB, ERA values from extra dict.

    Returns:
        (new_hr, new_bb, new_era, needs_update, hr_updated, bb_updated, era_updated)

    """
    needs_update = False
    hr_updated = False
    bb_updated = False
    era_updated = False

    # Extract Home Runs Allowed
    hr_val = _safe_int(extra.get("홈런") or extra.get("피홈런") or extra.get("HR"))
    if hr_val is not None and (current_hr is None or current_hr == 0):
        new_hr = hr_val
        if hr_val > 0 or current_hr is None:
            needs_update = True
            hr_updated = True
    else:
        new_hr = current_hr

    # Extract Walks Allowed
    bb_val = _safe_int(extra.get("4사구") or extra.get("사사구") or extra.get("볼넷") or extra.get("BB"))
    if bb_val is not None and (current_bb is None or current_bb == 0):
        new_bb = bb_val
        if bb_val > 0 or current_bb is None:
            needs_update = True
            bb_updated = True
    else:
        new_bb = current_bb

    # Extract ERA
    era_val = _safe_float(extra.get("평균자책점") or extra.get("ERA"))
    if era_val is not None and current_era is None:
        new_era = era_val
        needs_update = True
        era_updated = True
    else:
        new_era = current_era

    return new_hr, new_bb, new_era, needs_update, hr_updated, bb_updated, era_updated


def _commit_batch(session: Session, updates: list[dict[str, Any]]) -> None:
    """Execute and commit batch update."""
    if not updates:
        return
    session.execute(
        text("""
            UPDATE game_pitching_stats
            SET home_runs_allowed = :b_hr,
                walks_allowed = :b_bb,
                era = :b_era
            WHERE id = :b_id
        """),
        updates,
    )
    session.commit()
    logger.info("  Committed batch of %d updates...", len(updates))
    updates.clear()


def backfill_pitching_stats(
    year: int | None = None,
    *,
    apply: bool = False,
) -> dict[str, int]:
    """Extract HR, BB, and ERA from extra_stats JSON and update game_pitching_stats columns."""
    stats = {
        "scanned": 0,
        "updated": 0,
        "hr_backfilled": 0,
        "bb_backfilled": 0,
        "era_backfilled": 0,
        "total_hr": 0,
        "total_bb": 0,
    }

    with SessionLocal() as session:
        where_clause = "WHERE extra_stats IS NOT NULL AND extra_stats != '' AND extra_stats != '{}'"
        params: dict[str, Any] = {}
        if year:
            where_clause += " AND substr(game_id, 1, 4) = :year"
            params["year"] = str(year)

        query = text(f"""
            SELECT id, home_runs_allowed, walks_allowed, era, extra_stats
            FROM game_pitching_stats
            {where_clause}
            ORDER BY id
        """)

        rows = session.execute(query, params).fetchall()
        stats["scanned"] = len(rows)
        logger.info("Found %d rows with extra_stats to inspect (year=%s)", stats["scanned"], year or "ALL")

        pending_updates: list[dict[str, Any]] = []

        for row in rows:
            row_id, cur_hr, cur_bb, cur_era, extra_raw = row[0], row[1], row[2], row[3], row[4]
            try:
                extra = json.loads(extra_raw)
            except (json.JSONDecodeError, TypeError):
                continue

            if not isinstance(extra, dict):
                continue

            new_hr, new_bb, new_era, needs_up, hr_up, bb_up, era_up = _extract_pitching_updates(
                cur_hr, cur_bb, cur_era, extra
            )

            if not needs_up:
                continue

            stats["updated"] += 1
            stats["hr_backfilled"] += int(hr_up)
            stats["bb_backfilled"] += int(bb_up)
            stats["era_backfilled"] += int(era_up)
            stats["total_hr"] += new_hr or 0
            stats["total_bb"] += new_bb or 0
            pending_updates.append(
                {
                    "b_id": row_id,
                    "b_hr": new_hr if new_hr is not None else (cur_hr or 0),
                    "b_bb": new_bb if new_bb is not None else (cur_bb or 0),
                    "b_era": new_era if new_era is not None else cur_era,
                }
            )

            if apply and len(pending_updates) >= BATCH_SIZE:
                _commit_batch(session, pending_updates)

        if apply and pending_updates:
            _commit_batch(session, pending_updates)

    mode_label = "APPLIED" if apply else "DRY-RUN"
    logger.info(
        "[%s] Scanned: %d | Updated: %d | HR Backfilled: %d | BB Backfilled: %d | ERA Backfilled: %d | Total HR: %d | Total BB: %d",
        mode_label,
        stats["scanned"],
        stats["updated"],
        stats["hr_backfilled"],
        stats["bb_backfilled"],
        stats["era_backfilled"],
        stats["total_hr"],
        stats["total_bb"],
    )
    return stats


def main(argv: Sequence[str] | None = None) -> int:
    """Run CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Backfill home_runs_allowed, walks_allowed, and era from extra_stats JSON"
    )
    parser.add_argument("--year", type=int, default=None, help="Target year (default: all years)")
    parser.add_argument("--apply", action="store_true", help="Apply updates to database (default: dry-run)")
    args = parser.parse_args(argv)

    stats = backfill_pitching_stats(year=args.year, apply=args.apply)
    return 0 if stats["scanned"] > 0 or args.year is None else 1


if __name__ == "__main__":
    sys.exit(main())
