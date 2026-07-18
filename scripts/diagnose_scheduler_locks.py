"""Diagnose KBO scheduler lock state on the local SQLite runtime.

This is a read-only operational helper used to confirm the root cause of
``LockAcquisitionError`` / ``_LockSkipped`` events without touching any data:

* Stale ``data/locks/*.lock`` files left behind by crashed jobs.
* A live duplicate scheduler process (the most common cause of immediate
  ``daily_update`` lock contention).
* The scheduler PID-file guard state (``data/locks/scheduler.pid``).

Exit codes:
    0  clean (no stale locks, at most one scheduler process)
    1  at least one problem found (stale lock or duplicate scheduler)
    2  usage / environment error
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

LOCK_DIR = Path(__file__).resolve().parents[1] / "data" / "locks"
SCHEDULER_PID_FILE = LOCK_DIR / "scheduler.pid"

# Lock names owned by the scheduler / CLI entrypoints.
KNOWN_LOCKS = (
    "daily_update",
    "live_refresh",
    "maintenance",
    "realtime_oci_sync",
    "sqlite_writer",
)


def _pid_alive(pid: int) -> bool:
    """Return True if a process with ``pid`` is currently running."""
    try:
        os.kill(pid, 0)
    except (OSError, ProcessLookupError):
        return False
    return True


def _read_pid_file(path: Path) -> int | None:
    """Read the leading PID integer from a lock/PID file, or None."""
    try:
        raw = path.read_text().strip().split("\n")[0]
    except OSError:
        return None
    return int(raw) if raw.isdigit() else None


def _find_scheduler_processes() -> list[int]:
    """Return PIDs of processes running the scheduler (best-effort, macOS/Linux)."""
    pids: list[int] = []
    try:
        import subprocess

        out = subprocess.run(
            ["pgrep", "-f", "scripts/scheduler.py"],
            capture_output=True,
            text=True,
            check=False,
        )
        for line in out.stdout.splitlines():
            line = line.strip()
            if line.isdigit():
                pids.append(int(line))
    except (OSError, subprocess.SubprocessError):
        pass
    return pids


def _check_tier_locks(*, verbose: bool, problems: list[str]) -> None:
    """Inspect each known tier lock file for staleness."""
    for name in KNOWN_LOCKS:
        lock_file = LOCK_DIR / f"{name}.lock"
        if not lock_file.exists():
            if verbose:
                print(f"  [ok]   {name:18s} no lock file")
            continue
        pid = _read_pid_file(lock_file)
        if pid is None:
            problems.append(f"{name}: lock file exists but no valid PID ({lock_file})")
            print(f"  [WARN] {name:18s} lock file present, unreadable PID")
            continue
        if _pid_alive(pid):
            print(f"  [held] {name:18s} PID {pid} (alive)")
        else:
            problems.append(f"{name}: STALE lock file owned by dead PID {pid} ({lock_file})")
            print(f"  [STALE] {name:18s} PID {pid} (dead) -> should be cleared on next run")


def _check_scheduler_pid(problems: list[str]) -> None:
    """Inspect the scheduler PID-file guard state."""
    if SCHEDULER_PID_FILE.exists():
        pid = _read_pid_file(SCHEDULER_PID_FILE)
        if pid is None:
            problems.append(f"scheduler.pid present but unreadable ({SCHEDULER_PID_FILE})")
            print("  [WARN] scheduler.pid present, unreadable")
        elif _pid_alive(pid):
            print(f"  [ok]   scheduler.pid -> PID {pid} (alive)")
        else:
            problems.append(f"scheduler.pid STALE (dead PID {pid}) -> single-instance guard will clear it")
            print(f"  [STALE] scheduler.pid -> PID {pid} (dead)")
    else:
        print("  [ok]   no scheduler.pid (scheduler not running or guard not yet initialized)")


def _check_duplicate_schedulers(problems: list[str]) -> None:
    """Report duplicate scheduler processes (the real cause of contention)."""
    live_schedulers = sorted(set(_find_scheduler_processes()))
    if not live_schedulers:
        print("  [ok]   no scheduler processes running")
    elif len(live_schedulers) == 1:
        print(f"  [ok]   single scheduler process: PID {live_schedulers[0]}")
    else:
        problems.append(f"DUPLICATE scheduler processes detected: {live_schedulers}")
        print(f"  [FAIL] {len(live_schedulers)} scheduler processes running: {live_schedulers}")


def diagnose(*, verbose: bool = False) -> int:
    """Run the diagnostic and return the process exit code."""
    problems: list[str] = []

    if not LOCK_DIR.exists():
        print(f"[info] lock directory missing: {LOCK_DIR}")
        return 0

    print(f"Lock directory: {LOCK_DIR}")
    print("-" * 60)

    _check_tier_locks(verbose=verbose, problems=problems)
    print("-" * 60)
    _check_scheduler_pid(problems)
    print("-" * 60)
    _check_duplicate_schedulers(problems)
    print("-" * 60)

    if problems:
        print(f"RESULT: {len(problems)} problem(s) found:")
        for p in problems:
            print(f"  - {p}")
        return 1
    print("RESULT: clean")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose local scheduler lock state.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show OK states for every lock.")
    args = parser.parse_args(argv)
    try:
        return diagnose(verbose=args.verbose)
    except KeyboardInterrupt:
        return 2


if __name__ == "__main__":
    sys.exit(main())
