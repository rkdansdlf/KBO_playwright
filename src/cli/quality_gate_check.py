"""
CLI tool for running statistical quality gate checks.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Sequence

from src.db.engine import SessionLocal
from src.validators.quality_gate import run_quality_gate

logger = logging.getLogger(__name__)

CATEGORY_LABELS = {
    "batting": "Batting",
    "pitching": "Pitching",
    "pa_formula": "PA Formula",
    "team_batting": "Team Batting",
    "team_pitching": "Team Pitching",
}


def _print_category(category: str, result: dict) -> None:
    status = "PASSED" if result.get("ok") else "FAILED"
    label = CATEGORY_LABELS.get(category, category.capitalize())
    logger.info("%s: %s", label, status)

    is_team = category.startswith("team_")
    checked_label = "Checked Teams" if is_team else "Checked Players"
    logger.info("  %s: %s", checked_label, result.get("checked_players", 0))

    if result.get("error"):
        logger.error("  Error: %s", result["error"])

    mismatches = result.get("mismatches") or []
    if mismatches:
        logger.warning("  Mismatches: %d", len(mismatches))
        for mismatch in mismatches[:5]:
            entity = mismatch.get("player_id") or mismatch.get("team_id") or "?"
            logger.warning("    - %s: %s", entity, mismatch.get("issue"))
            diffs = mismatch.get("diffs")
            if diffs:
                for d in diffs[:3]:
                    logger.warning("      %s", d)
                if len(diffs) > 3:
                    logger.warning("      ... and %d more diff entries", len(diffs) - 3)
            for key in ("expected_pa", "actual_pa", "difference"):
                if key in mismatch:
                    logger.warning("      %s: %s", key, mismatch[key])
        if len(mismatches) > 5:
            logger.warning("    - ... and %d more", len(mismatches) - 5)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KBO Data Statistical Quality Gate")
    parser.add_argument("--year", type=int, help="Season year to check")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args(argv)

    if not args.year:
        from datetime import datetime

        year = datetime.now().year
    else:
        year = args.year

    with SessionLocal() as session:
        result = run_quality_gate(session, year)

    if args.json:
        logger.info(json.dumps(result, indent=2))
    else:
        logger.info("Statistical Quality Gate for %d", year)
        logger.info("----------------------------------------")

        for category in ["batting", "pitching", "pa_formula", "team_batting", "team_pitching"]:
            _print_category(category, result[category])

        logger.info("----------------------------------------")
        if result["ok"]:
            logger.info("Overall Status: SUCCESS")
        else:
            logger.warning("Overall Status: FAILURE (Statistical inconsistencies detected)")

    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
