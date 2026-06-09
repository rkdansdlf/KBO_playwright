"""
Monthly Team Stats Consistency Audit for Scheduler.

Compares TeamSeasonBatting/Pitching with PlayerSeasonBatting/Pitching
aggregated by team. Reports mismatches without modifying data.

Imported by scripts/scheduler.py as the crawl_monthly_team_audit_job target.
Runs on the 1st of every month at 04:00 KST via APScheduler.
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from src.db.engine import SessionLocal
from src.validators.quality_gate import run_quality_gate

logger = logging.getLogger(__name__)


def run_monthly_team_audit(year: int) -> dict[str, Any]:
    """Run team stats consistency check and return results."""
    with SessionLocal() as session:
        gate = run_quality_gate(session, year)
        team_batting = gate.get("team_batting", {})
        team_pitching = gate.get("team_pitching", {})

        result = {
            "year": year,
            "generated_at": datetime.now().isoformat(),
            "batting": {
                "ok": team_batting.get("ok", True),
                "checked_teams": team_batting.get("checked_players", 0),
                "mismatches": team_batting.get("mismatches", []),
            },
            "pitching": {
                "ok": team_pitching.get("ok", True),
                "checked_teams": team_pitching.get("checked_players", 0),
                "mismatches": team_pitching.get("mismatches", []),
            },
        }
        return result


def crawl_monthly_team_audit_job():
    """Scheduled job entry point — logs results, saves report, raises on failure."""
    KST = ZoneInfo("Asia/Seoul")
    current_year = datetime.now(KST).year
    target_year = current_year - 1

    if target_year < 2020:
        logger.info("Skipping team audit for year %s (before 2020)", target_year)
        return

    logger.info("Starting monthly team stats audit for year %s", target_year)
    result = run_monthly_team_audit(target_year)

    log_dir = Path("logs/team_audit")
    log_dir.mkdir(parents=True, exist_ok=True)
    report_path = log_dir / f"team_audit_{target_year}.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    bat_ok = result["batting"]["ok"]
    pit_ok = result["pitching"]["ok"]
    bat_miss = len(result["batting"]["mismatches"])
    pit_miss = len(result["pitching"]["mismatches"])

    logger.info(
        "Team audit for %s: batting_ok=%s (%d mismatches), pitching_ok=%s (%d mismatches)",
        target_year,
        bat_ok,
        bat_miss,
        pit_ok,
        pit_miss,
    )

    if not bat_ok or not pit_ok:
        raise RuntimeError(
            f"Team stats audit failed for {target_year}: batting={bat_miss} mismatches, pitching={pit_miss} mismatches"
        )

    logger.info("Monthly team stats audit completed for %s", target_year)


def main() -> int:
    """CLI entry point for direct invocation (e.g., from GitHub Actions)."""
    import argparse

    parser = argparse.ArgumentParser(description="Monthly Team Stats Consistency Audit")
    parser.add_argument("--year", type=int, help="Target year (defaults to previous year)")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    args = parser.parse_args()

    KST = ZoneInfo("Asia/Seoul")
    current_year = datetime.now(KST).year
    target_year = args.year if args.year else current_year - 1

    if target_year < 2020:
        logger.info(f"Skipping team audit for year {target_year} (before 2020)")
        return

    logger.info(f"Running team stats audit for year {target_year}...")
    result = run_monthly_team_audit(target_year)

    if args.json:
        logger.info(json.dumps(result, indent=2, ensure_ascii=False))
        return

    bat_ok = result["batting"]["ok"]
    pit_ok = result["pitching"]["ok"]
    bat_miss = len(result["batting"]["mismatches"])
    pit_miss = len(result["pitching"]["mismatches"])

    logger.info(f"\nTeam Batting: {'PASS' if bat_ok else 'FAIL'} ({bat_miss} mismatches)")
    for m in result["batting"]["mismatches"]:
        logger.info(f"  - [{m['team_id']}] {m['issue']}")
        for d in (m.get("diffs") or [])[:3]:
            logger.info(f"    {d}")

    logger.info(f"\nTeam Pitching: {'PASS' if pit_ok else 'FAIL'} ({pit_miss} mismatches)")
    for m in result["pitching"]["mismatches"]:
        logger.info(f"  - [{m['team_id']}] {m['issue']}")
        for d in (m.get("diffs") or [])[:3]:
            logger.info(f"    {d}")

    if not bat_ok or not pit_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
