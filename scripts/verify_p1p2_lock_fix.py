"""Immediate in-process verification of the crawl_p1p2_data_job lock fix.

Reproduces the 2026-07 contention scenario WITHOUT touching operational data
or sending alerts:

1. Simulate ``crawl_daily_games`` holding ``DAILY_LOCK`` (as the real 03:00 job
   does via ``run_daily_update_main(acquire_lock=False)``).
2. Call ``crawl_p1p2_data_job()`` while that lock is held. Before the fix this
   raised ``LockAcquisitionError`` (nested ``daily_update`` lock collision) and
   crashed the scheduler. After the fix ``DAILY_LOCK`` is a ``ForceProcessLock``
   with stale auto-clear and a bounded timeout that yields ``_LockSkipped``
   instead of crashing.

The P1/P2 crawler ``main`` functions are monkeypatched to no-ops so no
operational DB writes or Telegram alerts occur. We only assert the lock path
does not raise ``LockAcquisitionError`` / crash the process.
"""

from __future__ import annotations

import contextlib
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

logger = logging.getLogger("verify_p1p2_lock_fix")

import scripts.scheduler as scheduler


def _noop_main(argv=None):
    return 0


def run_contention_scenario(acquire_lock, release_lock, run_job) -> int:
    """Core contention check, injectable for unit testing.

    Holds the daily lock (via ``acquire_lock``), runs ``run_job`` while held,
    and reports whether it crashed with ``LockAcquisitionError``.

    Returns:
        0 - job ran without ``LockAcquisitionError`` (fix verified)
        1 - job raised ``LockAcquisitionError`` (pre-fix behavior)
        2 - could not acquire the lock for the simulation (skip)

    """
    acquired = acquire_lock()
    if not acquired:
        print("[SKIP] Could not acquire DAILY_LOCK for the simulation (lock busy?).")
        return 2
    print("[OK] Simulated crawl_daily_games holding DAILY_LOCK.")
    crashed = False
    try:
        run_job()
        print("[OK] crawl_p1p2_data_job ran without LockAcquisitionError.")
    except scheduler.LockAcquisitionError:
        crashed = True
        print("[FAIL] LockAcquisitionError raised — pre-fix behavior reproduced.")
    except Exception:
        logger.exception("Unexpected exception during crawl_p1p2_data_job lock test")
        print("[FAIL] Unexpected exception during lock test (see log).")
        crashed = True
    finally:
        with contextlib.suppress(Exception):
            release_lock()
    if crashed:
        return 1
    print("[PASS] Lock fix verified: no LockAcquisitionError under DAILY_LOCK contention.")
    return 0


def main() -> int:
    """Run the contention scenario and report pass/fail.

    Monkeypatches the heavy P1/P2 crawlers to no-ops so no operational DB
    writes or Telegram alerts occur, then reproduces the 2026-07 lock
    contention (``crawl_daily_games`` holding ``DAILY_LOCK``) and asserts
    ``crawl_p1p2_data_job`` completes without ``LockAcquisitionError``.
    """
    # Monkeypatch the heavy crawlers so no operational writes/alerts happen.
    import src.cli.crawl_parking as cp
    import src.cli.crawl_seat_sections as cs
    import src.cli.crawl_stadium_food as cf

    saved = (cp.main, cs.main, cf.main)
    cp.main = _noop_main
    cs.main = _noop_main
    cf.main = _noop_main

    lock = scheduler.DAILY_LOCK
    try:
        return run_contention_scenario(
            lambda: lock.acquire(blocking=True, timeout=5),
            lock.release,
            scheduler.crawl_p1p2_data_job,
        )
    finally:
        cp.main, cs.main, cf.main = saved


if __name__ == "__main__":
    raise SystemExit(main())
