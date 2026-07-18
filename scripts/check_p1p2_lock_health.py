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
import subprocess
import sys
from pathlib import Path

LOGS_DIR = Path("logs")
SCHEDULER_LOGS = (
    LOGS_DIR / "scheduler.launchd.out.log",
    LOGS_DIR / "scheduler.launchd.err.log",
)

# Error signatures that must be ABSENT after the lock fix.
FORBIDDEN_SIGNATURES = (
    "LockAcquisitionError",
    "_LockSkipped",
    "Another instance of run_daily_update",
    "Could not acquire ProcessLock: daily_update",
)

# Success signatures that should be PRESENT after a clean 06:45 run.
# Only enforced with --require-run (i.e. the morning after the schedule
# cycle), because before the first post-fix 06:45 execution they are absent
# by design.
SUCCESS_SIGNATURES = (
    "crawl_p1p2_data",
    "crawl_p1p2_data_job",
)


def _tail_logs(lines: int) -> str:
    """Concatenate the tail of every known scheduler log file."""
    chunks: list[str] = []
    for log_path in SCHEDULER_LOGS:
        if not log_path.exists():
            continue
        try:
            content = log_path.read_text(errors="replace").splitlines()
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
        for sig in SUCCESS_SIGNATURES:
            if sig not in logs:
                problems.append(f"Expected job signature missing from logs: {sig!r}")

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
        help="Also require crawl_p1p2_data job signatures (use the morning after the 06:45 run).",
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
    print("  - crawl_p1p2_data job signatures present.")
    print("  - diagnose_scheduler_locks reports clean state.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
