"""
Monthly PA Formula Audit Job for Scheduler.

Imported by scripts/scheduler.py as the crawl_monthly_pa_audit_job target.
Runs on the 1st of every month at 03:00 KST via APScheduler.
"""

import logging
import subprocess
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


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

    logger.info("Starting PA formula audit for year %s", target_year)
    result = subprocess.run(
        [sys.executable, "-m", "scripts.maintenance.audit_pa_formula", "--fix-year", str(target_year)],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        raise RuntimeError(f"PA formula audit failed: {result.stderr}")

    logger.info("PA formula audit completed for %s", target_year)
    if result.stdout.strip():
        logger.debug("Audit output: %s", result.stdout.strip())
