"""
CLI tool for running statistical quality gate checks.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from src.db.engine import SessionLocal
from src.validators.quality_gate import run_quality_gate

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
    print(f"{label}: {status}")

    is_team = category.startswith("team_")
    checked_label = "Checked Teams" if is_team else "Checked Players"
    print(f"  {checked_label}: {result.get('checked_players', 0)}")

    if result.get("error"):
        print(f"  Error: {result['error']}")

    mismatches = result.get("mismatches") or []
    if mismatches:
        print(f"  Mismatches: {len(mismatches)}")
        for mismatch in mismatches[:5]:
            entity = mismatch.get("player_id") or mismatch.get("team_id") or "?"
            print(f"    - {entity}: {mismatch.get('issue')}")
            diffs = mismatch.get("diffs")
            if diffs:
                for d in diffs[:3]:
                    print(f"      {d}")
                if len(diffs) > 3:
                    print(f"      ... and {len(diffs) - 3} more diff entries")
            for key in ("expected_pa", "actual_pa", "difference"):
                if key in mismatch:
                    print(f"      {key}: {mismatch[key]}")
        if len(mismatches) > 5:
            print(f"    - ... and {len(mismatches) - 5} more")


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
        print(json.dumps(result, indent=2))
    else:
        print(f"Statistical Quality Gate for {year}")
        print("----------------------------------------")

        for category in ["batting", "pitching", "pa_formula", "team_batting", "team_pitching"]:
            _print_category(category, result[category])

        print("----------------------------------------")
        if result["ok"]:
            print("Overall Status: SUCCESS")
        else:
            print("Overall Status: FAILURE (Statistical inconsistencies detected)")

    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
