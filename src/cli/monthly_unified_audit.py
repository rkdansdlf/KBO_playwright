"""
Monthly Unified Audit Job for Scheduler.
Runs both PA formula audit (with fix) and team stats consistency check.

PA formula audit: applies ratio-based SH/SF fix for the target year.
Team stats audit: compares TeamSeasonBatting/Pitching with
PlayerSeasonBatting/Pitching aggregates (read-only).

Imported by scripts/scheduler.py as the crawl_monthly_unified_audit_job target.
Runs on the 1st of every month at 03:00 KST via APScheduler.
"""

import json
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src.cli.monthly_team_audit import run_monthly_team_audit

logger = logging.getLogger(__name__)


def run_pa_fix(year: int, dry_run: bool = False) -> dict:
    """Apply PA formula fix for a given year, returning result dict."""
    cmd = [
        sys.executable,
        "-m",
        "scripts.legacy.maintenance.audit_pa_formula",
        "--fix-year",
        str(year),
    ]
    if dry_run:
        cmd.append("--dry-run")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logger.error("PA formula fix failed for %s: %s", year, result.stderr.strip())
            return {"ok": False, "error": result.stderr.strip(), "fixed_rows": 0}
        logger.info("PA formula fix completed for %s (dry_run=%s)", year, dry_run)
        return {"ok": True, "fixed_rows": 0, "message": result.stdout.strip()}
    except subprocess.TimeoutExpired:
        logger.error("PA formula fix timed out for %s", year)
        return {"ok": False, "error": "Timeout after 300s", "fixed_rows": 0}


def run_pa_audit(year: int) -> dict:
    """Run PA formula audit (read-only) and return JSON result."""
    cmd = [
        sys.executable,
        "-m",
        "scripts.legacy.maintenance.audit_pa_formula",
        "--year",
        str(year),
        "--json",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logger.error("PA formula audit failed for %s: %s", year, result.stderr.strip())
            return {"year": year, "ok": False, "violation_count": 0, "violations": []}
        try:
            data = json.loads(result.stdout.strip())
            if isinstance(data, list):
                data = data[0] if data else {}
            return {
                "year": year,
                "ok": data.get("violation_rows", 0) == 0,
                "violation_count": data.get("violation_rows", 0),
                "violations": data,
            }
        except json.JSONDecodeError:
            logger.error("Failed to parse PA audit JSON for %s", year)
            return {"year": year, "ok": False, "violation_count": 0, "violations": []}
    except subprocess.TimeoutExpired:
        logger.error("PA formula audit timed out for %s", year)
        return {"year": year, "ok": False, "error": "Timeout after 300s", "violation_count": 0, "violations": []}


def crawl_monthly_unified_audit_job():
    """Scheduled job entry point — runs PA fix + both audits, saves reports."""
    KST = ZoneInfo("Asia/Seoul")
    current_year = datetime.now(KST).year
    target_year = current_year - 1

    if target_year < 2020:
        logger.info("Skipping unified audit for year %s (before 2020)", target_year)
        return

    logger.info("Starting unified audit for year %s", target_year)

    # Phase 1: Apply PA formula fix
    fix_result = run_pa_fix(target_year, dry_run=False)
    if not fix_result["ok"]:
        raise RuntimeError(f"PA formula fix failed for {target_year}: {fix_result.get('error')}")

    # Phase 2: Run PA formula audit (post-fix) to verify
    pa_result = run_pa_audit(target_year)

    # Phase 3: Run team stats audit
    team_result = run_monthly_team_audit(target_year)

    # Save reports
    log_dir = Path("logs/unified_audit")
    log_dir.mkdir(parents=True, exist_ok=True)

    pa_report_path = log_dir / f"pa_audit_{target_year}.json"
    with open(pa_report_path, "w", encoding="utf-8") as f:
        json.dump(pa_result, f, indent=2, ensure_ascii=False)

    team_report_path = log_dir / f"team_audit_{target_year}.json"
    with open(team_report_path, "w", encoding="utf-8") as f:
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
        raise RuntimeError(
            f"Unified audit failed for {target_year}: "
            f"PA formula={'OK' if pa_ok else 'FAIL (' + str(pa_violations) + ' violations)'}, "
            f"Team batting={'OK' if team_bat_ok else 'FAIL (' + str(team_bat_miss) + ' mismatches)'}, "
            f"Team pitching={'OK' if team_pit_ok else 'FAIL (' + str(team_pit_miss) + ' mismatches)'}"
        )

    logger.info("Unified audit completed for %s", target_year)


def main():
    """CLI entry point for direct invocation (e.g., from GitHub Actions)."""
    import argparse

    parser = argparse.ArgumentParser(description="Monthly Unified Audit (PA Formula + Team Stats)")
    parser.add_argument("--year", type=int, help="Target year (defaults to previous year)")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no changes applied")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    parser.add_argument("--pa-only", action="store_true", help="Run only PA formula audit (incl. fix)")
    parser.add_argument("--team-only", action="store_true", help="Run only team stats audit")
    args = parser.parse_args()

    KST = ZoneInfo("Asia/Seoul")
    current_year = datetime.now(KST).year
    target_year = args.year if args.year else current_year - 1

    if target_year < 2020:
        print(f"Skipping unified audit for year {target_year} (before 2020)")
        return

    print(f"Running unified audit for year {target_year}...")

    if args.team_only:
        team_result = run_monthly_team_audit(target_year)
        if args.json:
            print(json.dumps(team_result, indent=2, ensure_ascii=False))
            return
        bat_ok = team_result["batting"]["ok"]
        pit_ok = team_result["pitching"]["ok"]
        bat_miss = len(team_result["batting"]["mismatches"])
        pit_miss = len(team_result["pitching"]["mismatches"])
        print(f"Team Batting: {'PASS' if bat_ok else 'FAIL'} ({bat_miss} mismatches)")
        print(f"Team Pitching: {'PASS' if pit_ok else 'FAIL'} ({pit_miss} mismatches)")
        if not bat_ok or not pit_ok:
            sys.exit(1)
        return

    # Phase 1: Apply PA formula fix (skip if --dry-run or --pa-only)
    if args.pa_only:
        fix_result = run_pa_fix(target_year, dry_run=args.dry_run)
        pa_result = run_pa_audit(target_year) if not args.dry_run else {"ok": fix_result["ok"], "violation_count": 0}
    else:
        fix_result = run_pa_fix(target_year, dry_run=args.dry_run)
        pa_result = run_pa_audit(target_year) if not args.dry_run else {"ok": fix_result["ok"], "violation_count": 0}
        team_result = run_monthly_team_audit(target_year)

    if args.json:
        output = {"pa_formula": pa_result}
        if not args.pa_only:
            output["team_stats"] = team_result
        print(json.dumps(output, indent=2, ensure_ascii=False))
        return

    # Human-readable output
    pa_ok = pa_result.get("ok", False)
    pa_violations = pa_result.get("violation_count", 0)
    print(f"\nPA Formula Audit: {'PASS' if pa_ok else 'FAIL (' + str(pa_violations) + ' violations)'}")

    if args.pa_only:
        if not pa_ok:
            sys.exit(1)
        return

    team_bat_ok = team_result["batting"]["ok"]
    team_pit_ok = team_result["pitching"]["ok"]
    bat_miss = len(team_result["batting"]["mismatches"])
    pit_miss = len(team_result["pitching"]["mismatches"])

    print(f"Team Batting: {'PASS' if team_bat_ok else 'FAIL'} ({bat_miss} mismatches)")
    for m in team_result["batting"]["mismatches"][:3]:
        print(f"  - [{m['team_id']}] {m['issue']}")
        for d in (m.get("diffs") or [])[:2]:
            print(f"    {d}")

    print(f"Team Pitching: {'PASS' if team_pit_ok else 'FAIL'} ({pit_miss} mismatches)")
    for m in team_result["pitching"]["mismatches"][:3]:
        print(f"  - [{m['team_id']}] {m['issue']}")
        for d in (m.get("diffs") or [])[:2]:
            print(f"    {d}")

    if not pa_ok or not team_bat_ok or not team_pit_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
