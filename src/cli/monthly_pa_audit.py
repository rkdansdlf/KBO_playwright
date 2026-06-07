"""
Monthly PA Formula Audit Job for Scheduler.

Imported by scripts/scheduler.py as the crawl_monthly_pa_audit_job target.
Runs on the 1st of every month at 03:00 KST via APScheduler.
"""

import logging
import sys
from datetime import datetime
from typing import Sequence
from zoneinfo import ZoneInfo

from scripts.legacy.maintenance.audit_pa_formula import fix_year_formula

logger = logging.getLogger(__name__)


def run_monthly_pa_audit(target_year: int) -> int:
    """Apply the monthly PA formula fix for a target season."""
    logger.info("Starting PA formula audit for year %s", target_year)
    try:
        fixed_rows = fix_year_formula(target_year, dry_run=False)
    except Exception as exc:
        logger.exception("PA formula audit failed for %s", target_year)
        raise RuntimeError(f"PA formula audit failed for {target_year}: {exc}") from exc

    logger.info("PA formula audit completed for %s (fixed_rows=%s)", target_year, fixed_rows)
    return fixed_rows


def crawl_monthly_pa_audit_job():
    """
    Audit PA formula for the previous year and apply ratio-based fix.
    """
    KST = ZoneInfo("Asia/Seoul")
    current_year = datetime.now(KST).year
    target_year = current_year - 1

    if target_year < 2020:
        logger.info("Skipping PA formula audit for year %s (before 2020)", target_year)
        return

    run_monthly_pa_audit(target_year)


def main(argv: Sequence[str] | None = None):
    """CLI entry point for direct invocation (e.g., from GitHub Actions)."""
    import argparse

    parser = argparse.ArgumentParser(description="Monthly PA Formula Audit")
    parser.add_argument("--year", type=int, help="Target year (defaults to previous year)")
    args = parser.parse_args(argv)

    KST = ZoneInfo("Asia/Seoul")
    current_year = datetime.now(KST).year
    target_year = args.year if args.year else current_year - 1

    if target_year < 2020:
        print(f"Skipping PA formula audit for year {target_year} (before 2020)")
        return

    try:
        print(f"Starting PA formula audit for year {target_year}")
        fixed_rows = run_monthly_pa_audit(target_year)
    except Exception:
        logger.exception("PA formula audit failed")
        sys.exit(1)

    print(f"PA formula audit completed for {target_year}")
    print(f"Fixed rows: {fixed_rows}")


if __name__ == "__main__":
    main()
