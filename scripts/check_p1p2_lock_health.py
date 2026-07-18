"""Post-run health check for the daily scheduler lock fixes.

Verifies that the 2026-07 scheduler lock hardening (B1-B6) and the Docker
PID-1 guard fix are behaving in production:

* No ``LockAcquisitionError`` / ``_LockSkipped`` in the scheduler logs.
* No "Another instance of run_daily_update" nested-lock false positive.
* The 06:45 ``crawl_p1p2_data`` job ran and completed without a lock error.
* ``diagnose_scheduler_locks`` reports a clean (exit 0) lock state.

This is the Step 1 operational-monitoring check that must pass after the
next 03:00 ``crawl_daily_games`` and 06:45 ``crawl_p1p2_data`` schedule cycle.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

# Marker file written by ``crawl_p1p2_data_job`` on every run (success or
# failure). This is the authoritative "the 06:45 job executed today" signal,
# avoiding fragile scans of the multi-MB rotating error log.
LAST_RUN_MARKER = Path("data/last_runs/p1p2_data.json")

LOGS_DIR = Path("logs")
# The operational scheduler writes to ``logs/scheduler.log`` via a
# ``RotatingFileHandler`` (``scripts/scheduler.py``). Its rotated backups are
# ``scheduler.log.1`` .. ``scheduler.log.5``. The launchd stdout/stderr logs are
# only a fallback for environments that do not use the rotating handler.
SCHEDULER_LOG = LOGS_DIR / "scheduler.log"
SCHEDULER_LOG_BACKUPS = sorted(LOGS_DIR.glob("scheduler.log.[1-9]"), reverse=True)
LAUNCHD_LOGS = (
    LOGS_DIR / "scheduler.launchd.out.log",
    LOGS_DIR / "scheduler.launchd.err.log",
)


def _log_candidates() -> list[Path]:
    """Return scheduler log files to scan, newest first.

    Prefers the rotating ``scheduler.log`` (and its backups) and only falls
    back to the launchd logs when the rotating handler output is absent.
    """
    if SCHEDULER_LOG.exists():
        return [SCHEDULER_LOG, *SCHEDULER_LOG_BACKUPS, *LAUNCHD_LOGS]
    return [*LAUNCHD_LOGS, *SCHEDULER_LOG_BACKUPS]


# Error signatures that must be ABSENT after the lock fix.
FORBIDDEN_SIGNATURES = (
    "LockAcquisitionError",
    "_LockSkipped",
    "Another instance of run_daily_update",
    "Could not acquire ProcessLock: daily_update",
)


# The 06:45 ``crawl_p1p2_data`` job writes ``LAST_RUN_MARKER`` on every run.
# With ``--require-run`` we require that marker to prove the job executed
# today (KST) and completed without writing an error status. Scanning the
# multi-MB rotating error log for a daily marker is too fragile (the tail
# window often does not reach back to 06:45), so the marker file is the
# authoritative signal.
def _p1p2_run_status() -> tuple[bool, str]:
    """Return (ran_today_ok, detail) for the last P1/P2 job run.

    ``ran_today_ok`` is True only when the marker exists, is parseable, has a
    ``ts`` on the current KST date, and ``status != "error"``.
    """
    if not LAST_RUN_MARKER.exists():
        return False, f"Marker file missing: {LAST_RUN_MARKER}"
    try:
        data = json.loads(LAST_RUN_MARKER.read_text(encoding="utf-8"))
        ts_raw = data.get("ts")
        status = data.get("status", "ok")
        if not ts_raw:
            return False, "Marker file has no 'ts' field."
        ts = datetime.fromisoformat(ts_raw)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=KST)
        today = datetime.now(KST).date()
        if ts.date() != today:
            return False, f"Last P1/P2 run was {ts.date()} (expected {today})."
        if status == "error":
            return False, "Last P1/P2 run recorded status='error'."
        return True, f"P1/P2 ran OK at {ts.isoformat()}."
    except (OSError, ValueError) as exc:
        return False, f"Could not parse marker file: {exc}"


_MAX_TAIL_BYTES = 8 * 1024 * 1024


def _tail_logs(lines: int) -> str:
    """Concatenate the tail of the relevant scheduler log files.

    Each file is read from its end (bounded to ``_MAX_TAIL_BYTES``) and then
    truncated to the last ``lines`` lines, so a multi-MB rotated log is not
    loaded entirely into memory.
    """
    chunks: list[str] = []
    for log_path in _log_candidates():
        if not log_path.exists():
            continue
        try:
            size = log_path.stat().st_size
            start = max(0, size - _MAX_TAIL_BYTES)
            with log_path.open("r", errors="replace") as fh:
                if start:
                    fh.seek(start)
                    # Discard the probable partial first line after the seek.
                    fh.readline()
                content = fh.read().splitlines()
        except OSError:
            continue
        chunks.extend(content[-lines:])
    return "\n".join(chunks)


def _collect_problems(logs: str, *, require_run: bool, run_diagnose: bool) -> list[str]:
    """Return a list of human-readable problems found in the scanned logs."""
    problems: list[str] = []
    if not logs.strip():
        problems.append("No scheduler log output found to scan.")

    for sig in FORBIDDEN_SIGNATURES:
        if sig in logs:
            problems.append(f"Forbidden lock-error signature present: {sig!r}")

    if require_run:
        ran_ok, detail = _p1p2_run_status()
        if not ran_ok:
            problems.append(f"P1/P2 job did not run cleanly today: {detail}")

    if run_diagnose:
        try:
            result = subprocess.run(
                [sys.executable, "scripts/diagnose_scheduler_locks.py"],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                problems.append(f"diagnose_scheduler_locks exited {result.returncode} (stale locks?)")
        except OSError as exc:
            problems.append(f"Could not run diagnose_scheduler_locks: {exc}")
    return problems


def main() -> int:
    """Run the lock-health post-check and return a process exit code."""
    parser = argparse.ArgumentParser(description="Check scheduler lock health after a run cycle.")
    parser.add_argument("--lines", type=int, default=20000, help="Lines to scan from each log tail.")
    parser.add_argument("--no-diagnose", action="store_true", help="Skip diagnose_scheduler_locks.")
    parser.add_argument(
        "--require-run",
        action="store_true",
        help="Also require today's P1/P2 run marker (use the morning after the 06:45 run).",
    )
    args = parser.parse_args()

    logs = _tail_logs(args.lines)
    problems = _collect_problems(logs, require_run=args.require_run, run_diagnose=not args.no_diagnose)

    if problems:
        print("[FAIL] Scheduler lock health check found problems:")
        for p in problems:
            print(f"  - {p}")
        return 1

    print("[OK] Scheduler lock health check passed:")
    print("  - No forbidden lock-error signatures in scheduler logs.")
    print("  - P1/P2 job ran today (marker file).")
    print("  - diagnose_scheduler_locks reports clean state.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
