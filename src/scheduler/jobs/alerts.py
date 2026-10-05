"""Notification dispatch jobs: pregame alerts and the daily milestone summary.

Both were invoked only from the manually dispatched ``daily-extras`` job and had no
scheduler entry, so the scheduled pipeline never sent them.

``--season`` defaults to a literal year in both CLIs, so the current season is passed
explicitly; otherwise the dispatch would silently keep reporting a stale season.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from datetime import datetime

from src.scheduler.config import KST
from src.scheduler.locks import (
    MAINTENANCE_LOCK,
    _scheduler_job_lock,
    _with_db_fail_fast_guard,
    _with_lock_skip_guard,
)

logger = logging.getLogger("src.scheduler.jobs.alerts")

#: Child-process launch failures. Delivery errors are the CLI's own concern and are
#: surfaced through the notification ledger rather than an exception here.
ALERT_DISPATCH_EXCEPTIONS = (subprocess.CalledProcessError, OSError, RuntimeError)


def _dispatch(module: str, *args: str) -> None:
    """Run a notification CLI in a child process so it owns its own ``argv``."""
    subprocess.run(  # noqa: S603
        [sys.executable, "-m", module, *args],
        check=True,
    )


def _dispatch_args() -> list[str]:
    """Arguments shared by both notification CLIs."""
    return ["--season", str(datetime.now(KST).year), "--channels", "telegram"]


@_with_db_fail_fast_guard
@_with_lock_skip_guard
def send_pregame_alerts_job() -> None:
    """Send the day's pregame alerts. Runs daily at 16:00 KST."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Pregame Alert Dispatch ===")
        try:
            _dispatch("src.cli.send_today_pregame_alerts", *_dispatch_args())
            logger.info("=== Pregame Alert Dispatch Completed ===")
        except ALERT_DISPATCH_EXCEPTIONS:
            logger.exception("Pregame alert dispatch failed")


@_with_db_fail_fast_guard
@_with_lock_skip_guard
def send_milestone_summary_job() -> None:
    """Send the daily milestone summary. Runs daily at 08:30 KST."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Milestone Summary Dispatch ===")
        try:
            _dispatch("src.cli.send_milestone_daily_summary", *_dispatch_args())
            logger.info("=== Milestone Summary Dispatch Completed ===")
        except ALERT_DISPATCH_EXCEPTIONS:
            logger.exception("Milestone summary dispatch failed")
