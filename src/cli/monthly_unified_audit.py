"""
Monthly Unified Audit Job for Scheduler.

Runs both PA formula audit (with fix) and team stats consistency check.

PA formula audit: applies ratio-based SH/SF fix for the target year.
Team stats audit: compares TeamSeasonBatting/Pitching with
PlayerSeasonBatting/Pitching aggregates (read-only).

Imported by scripts/scheduler.py as the crawl_monthly_unified_audit_job target.
Runs on the 1st of every month at 03:00 KST via APScheduler.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.exc import SQLAlchemyError

from scripts.maintenance.audit_pa_formula import audit_year, fix_year_formula
from src.cli.monthly_team_audit import run_monthly_team_audit

logger = logging.getLogger(__name__)

PA_AUDIT_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)


def run_pa_fix(year: int, *, dry_run: bool = False) -> dict[str, Any]:
    """Apply PA formula fix for a given year, returning result dict."""
    try:
        fixed_rows = fix_year_formula(year, dry_run=dry_run)
    except PA_AUDIT_EXCEPTIONS as exc:
        logger.exception("PA formula fix failed for %s", year)
        return {"ok": False, "error": str(exc), "fixed_rows": 0}

    logger.info("PA formula fix completed for %s (dry_run=%s fixed_rows=%s)", year, dry_run, fixed_rows)
    return {
        "ok": True,
        "fixed_rows": fixed_rows,
        "message": f"PA formula fix completed for {year} (fixed_rows={fixed_rows}, dry_run={dry_run})",
    }


def run_pa_audit(year: int) -> dict[str, Any]:
    """Run PA formula audit (read-only) and return JSON result."""
    try:
        data = audit_year(year)
    except PA_AUDIT_EXCEPTIONS as exc:
        logger.exception("PA formula audit failed for %s", year)
        return {"year": year, "ok": False, "error": str(exc), "violation_count": 0, "violations": []}

    return {
        "year": year,
        "ok": data.get("violation_rows", 0) == 0,
        "violation_count": data.get("violation_rows", 0),
        "violations": data,
    }


def crawl_monthly_unified_audit_job() -> None:
    """Scheduled job entry point — runs PA fix + both audits, saves reports."""
    kst = ZoneInfo("Asia/Seoul")
    current_year = datetime.now(kst).year
    target_year = current_year - 1

    if target_year < 2020:
        logger.info("Skipping unified audit for year %s (before 2020)", target_year)
        return

    logger.info("Starting unified audit for year %s", target_year)

    # Phase 1: Apply PA formula fix
    fix_result = run_pa_fix(target_year, dry_run=False)
    if not fix_result["ok"]:
        msg = f"PA formula fix failed for {target_year}: {fix_result.get('error')}"
        raise RuntimeError(msg)

    # Phase 2: Run PA formula audit (post-fix) to verify
    pa_result = run_pa_audit(target_year)

    # Phase 3: Run team stats audit
    team_result = run_monthly_team_audit(target_year)

    # Save reports
    log_dir = Path("logs/unified_audit")
    log_dir.mkdir(parents=True, exist_ok=True)

    pa_report_path = log_dir / f"pa_audit_{target_year}.json"
    with pa_report_path.open("w", encoding="utf-8") as f:
        json.dump(pa_result, f, indent=2, ensure_ascii=False)

    team_report_path = log_dir / f"team_audit_{target_year}.json"
    with team_report_path.open("w", encoding="utf-8") as f:
        json.dump(team_result, f, indent=2, ensure_ascii=False)

    # Log summary
    pa_ok = pa_result.get("ok", False)
    pa_violations = pa_result.get("violation_count", 0)
    team_bat_ok = team_result["batting"]["ok"]
    team_pit_ok = team_result["pitching"]["ok"]
    team_bat_miss = len(team_result["batting"]["mismatches"])
    team_pit_miss = len(team_result["pitching"]["mismatches"])

    logger.info(
        "Unified audit for %s: PA formula=%s (%d violations), "
        "Team batting=%s (%d mismatches), Team pitching=%s (%d mismatches)",
        target_year,
        "OK" if pa_ok else "FAIL",
        pa_violations,
        "OK" if team_bat_ok else "FAIL",
        team_bat_miss,
        "OK" if team_pit_ok else "FAIL",
        team_pit_miss,
    )

    if not pa_ok or not team_bat_ok or not team_pit_ok:
        msg = (
            f"Unified audit failed for {target_year}: "
            f"PA formula={'OK' if pa_ok else 'FAIL (' + str(pa_violations) + ' violations)'}, "
            f"Team batting={'OK' if team_bat_ok else 'FAIL (' + str(team_bat_miss) + ' mismatches)'}, "
            f"Team pitching={'OK' if team_pit_ok else 'FAIL (' + str(team_pit_miss) + ' mismatches)'}"
        )
        raise RuntimeError(msg)

    logger.info("Unified audit completed for %s", target_year)


def _target_year_from_args(year: int | None) -> int:
    kst = ZoneInfo("Asia/Seoul")
    return year or datetime.now(kst).year - 1


def _run_team_only(target_year: int, *, json_output: bool) -> None:
    team_result = run_monthly_team_audit(target_year)
    if json_output:
        logger.info(json.dumps(team_result, indent=2, ensure_ascii=False))
        return
    bat_ok = team_result["batting"]["ok"]
    pit_ok = team_result["pitching"]["ok"]
    logger.info(
        "Team Batting: %s (%s mismatches)",
        "PASS" if bat_ok else "FAIL",
        len(team_result["batting"]["mismatches"]),
    )
    logger.info(
        "Team Pitching: %s (%s mismatches)",
        "PASS" if pit_ok else "FAIL",
        len(team_result["pitching"]["mismatches"]),
    )
    if not bat_ok or not pit_ok:
        sys.exit(1)


def _run_pa_audit_for_cli(target_year: int, *, dry_run: bool) -> dict[str, Any]:
    fix_result = run_pa_fix(target_year, dry_run=dry_run)
    return run_pa_audit(target_year) if not dry_run else {"ok": fix_result["ok"], "violation_count": 0}


def _log_team_mismatches(label: str, result: dict[str, Any]) -> bool:
    ok = result["ok"]
    mismatches = result["mismatches"]
    logger.info("Team %s: %s (%s mismatches)", label, "PASS" if ok else "FAIL", len(mismatches))
    for mismatch in mismatches[:3]:
        logger.info("  - [%s] %s", mismatch["team_id"], mismatch["issue"])
        for diff in (mismatch.get("diffs") or [])[:2]:
            logger.info("    %s", diff)
    return ok


def _emit_unified_cli_output(
    pa_result: dict[str, Any],
    team_result: dict[str, Any] | None,
    *,
    pa_only: bool,
    json_output: bool,
) -> None:
    if json_output:
        output = {"pa_formula": pa_result}
        if not pa_only:
            output["team_stats"] = team_result
        logger.info(json.dumps(output, indent=2, ensure_ascii=False))
        return

    pa_ok = pa_result.get("ok", False)
    pa_violations = pa_result.get("violation_count", 0)
    logger.info("\nPA Formula Audit: %s", "PASS" if pa_ok else "FAIL (" + str(pa_violations) + " violations)")
    if pa_only:
        if not pa_ok:
            sys.exit(1)
        return

    team_bat_ok = _log_team_mismatches("Batting", team_result["batting"])
    team_pit_ok = _log_team_mismatches("Pitching", team_result["pitching"])
    if not pa_ok or not team_bat_ok or not team_pit_ok:
        sys.exit(1)


def main() -> int:
    """CLI entry point for direct invocation (e.g., from GitHub Actions)."""
    parser = argparse.ArgumentParser(description="Monthly Unified Audit (PA Formula + Team Stats)")
    parser.add_argument("--year", type=int, help="Target year (defaults to previous year)")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no changes applied")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    parser.add_argument("--pa-only", action="store_true", help="Run only PA formula audit (incl. fix)")
    parser.add_argument("--team-only", action="store_true", help="Run only team stats audit")
    args = parser.parse_args()

    target_year = _target_year_from_args(args.year)

    if target_year < 2020:
        logger.info("Skipping unified audit for year %s (before 2020)", target_year)
        return

    logger.info("Running unified audit for year %s...", target_year)

    if args.team_only:
        _run_team_only(target_year, json_output=args.json)
        return

    pa_result = _run_pa_audit_for_cli(target_year, dry_run=args.dry_run)
    team_result = None if args.pa_only else run_monthly_team_audit(target_year)
    _emit_unified_cli_output(pa_result, team_result, pa_only=args.pa_only, json_output=args.json)


if __name__ == "__main__":
    main()
