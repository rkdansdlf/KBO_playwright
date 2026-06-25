#!/usr/bin/env python3
"""Audit script to detect anomalies in game_status and associated data.
Checks for:
1. Future games not marked as SCHEDULED (or CANCELLED/POSTPONED)
2. LIVE games without any progress evidence (inning scores, events, pbp)
3. COMPLETED games without scores
4. Past games still marked as LIVE
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


import argparse
import sys
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal
from src.utils.game_status import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DRAW,
    GAME_STATUS_LIVE,
    GAME_STATUS_POSTPONED,
    GAME_STATUS_SCHEDULED,
)


def audit_game_status() -> list[dict[str, Any]]:
    today = date.today()
    current_year_start = date(today.year, 1, 1)
    violations = []

    with SessionLocal() as session:
        # 1. Future games check
        future_violations = (
            session.execute(
                text("""
                SELECT game_id, game_date, game_status
                FROM game
                WHERE game_date > :today
                  AND game_status NOT IN (:scheduled, :cancelled, :postponed)
            """),
                {
                    "today": today,
                    "scheduled": GAME_STATUS_SCHEDULED,
                    "cancelled": GAME_STATUS_CANCELLED,
                    "postponed": GAME_STATUS_POSTPONED,
                },
            )
            .mappings()
            .all()
        )

        violations.extend(
            {
                "game_id": row["game_id"],
                "game_date": row["game_date"],
                "status": row["game_status"],
                "reason": "Future game with non-scheduled status",
            }
            for row in future_violations
        )

        # 2. LIVE games without evidence
        live_no_evidence = (
            session.execute(
                text("""
                SELECT g.game_id, g.game_date, g.game_status
                FROM game g
                WHERE g.game_status = :live
                  AND g.game_date >= :start_date
                  AND NOT EXISTS (SELECT 1 FROM game_inning_scores gis WHERE gis.game_id = g.game_id)
                  AND NOT EXISTS (SELECT 1 FROM game_events ge WHERE ge.game_id = g.game_id)
                  AND NOT EXISTS (SELECT 1 FROM game_play_by_play pbp WHERE pbp.game_id = g.game_id)
            """),
                {"live": GAME_STATUS_LIVE, "start_date": current_year_start},
            )
            .mappings()
            .all()
        )

        violations.extend(
            {
                "game_id": row["game_id"],
                "game_date": row["game_date"],
                "status": row["game_status"],
                "reason": "LIVE game without progress evidence (inning scores, events, pbp)",
            }
            for row in live_no_evidence
        )

        # 3. COMPLETED games without scores
        completed_no_scores = (
            session.execute(
                text("""
                SELECT game_id, game_date, game_status
                FROM game
                WHERE game_status IN (:completed, :draw)
                  AND game_date >= :start_date
                  AND (home_score IS NULL OR away_score IS NULL)
            """),
                {"completed": GAME_STATUS_COMPLETED, "draw": GAME_STATUS_DRAW, "start_date": current_year_start},
            )
            .mappings()
            .all()
        )

        violations.extend(
            {
                "game_id": row["game_id"],
                "game_date": row["game_date"],
                "status": row["game_status"],
                "reason": "COMPLETED/DRAW game missing scores",
            }
            for row in completed_no_scores
        )

        # 4. Past games still marked as LIVE
        past_live = (
            session.execute(
                text("""
                SELECT game_id, game_date, game_status
                FROM game
                WHERE game_date < :today
                  AND game_date >= :start_date
                  AND game_status = :live
            """),
                {"today": today, "live": GAME_STATUS_LIVE, "start_date": current_year_start},
            )
            .mappings()
            .all()
        )

        violations.extend(
            {
                "game_id": row["game_id"],
                "game_date": row["game_date"],
                "status": row["game_status"],
                "reason": "Past game still marked as LIVE",
            }
            for row in past_live
        )

    return violations


def main():
    parser = argparse.ArgumentParser(description="Audit game status integrity")
    parser.add_argument("--fail", action="store_true", help="Exit with non-zero code if violations found")
    args = parser.parse_args()

    logger.info(f"🔍 Starting Game Status Integrity Audit (Today: {date.today()})...")
    violations = audit_game_status()

    if not violations:
        logger.info("✅ No integrity violations found.")
        sys.exit(0)

    logger.error(f"❌ Found {len(violations)} integrity violations:")
    for v in violations:
        logger.info(f"  - [{v['game_id']}] {v['game_date']} | Status: {v['status']} | Reason: {v['reason']}")

    if args.fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
