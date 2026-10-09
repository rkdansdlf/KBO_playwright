"""Resync bega_dev from bega_prod (prod is the source of truth).

Usage:
    python scripts/resync_bega_dev.py [--yes]
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def run(cmd: list[str]) -> None:
    """Run a docker command, raising on failure."""
    proc = subprocess.run(cmd, capture_output=True, text=True, env=dict(os.environ), cwd=REPO_ROOT, check=False)
    if proc.returncode != 0:
        if "already exists in network" in (proc.stderr or ""):
            return
        print(proc.stdout[-2000:])
        print(proc.stderr[-2000:])
        msg = "command failed: " + " ".join(cmd[:4])
        raise SystemExit(msg)


def main(argv: list[str] | None = None) -> int:
    """Recreate bega_dev.kbo from bega_prod."""
    parser = argparse.ArgumentParser(description="Resync bega_dev from bega_prod")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation")
    args = parser.parse_args(argv)

    if not args.yes:
        answer = input("Recreate bega_dev.kbo from bega_prod (ALL dev data lost)? [y/N] ")
        if answer.strip().lower() != "y":
            print("aborted")
            return 1

    run(["docker", "network", "connect", "kbo_playwright_default", "bega_dev"])
    run(["docker", "exec", "bega_dev", "psql", "-U", "postgres", "-c", "DROP DATABASE kbo;"])
    run(["docker", "exec", "bega_dev", "psql", "-U", "postgres", "-c", "CREATE DATABASE kbo;"])
    dump = subprocess.Popen(
        ["docker", "exec", "kbo_postgres", "sh", "-c", "pg_dump -U postgres bega_prod"],
        stdout=subprocess.PIPE,
        cwd=REPO_ROOT,
    )
    restore = subprocess.run(
        ["docker", "exec", "-i", "bega_dev", "psql", "-U", "postgres", "-d", "kbo", "-q"],
        stdin=dump.stdout,
        capture_output=True,
        cwd=REPO_ROOT,
        check=False,
    )
    dump.wait()
    if dump.returncode != 0 or restore.returncode != 0:
        print((restore.stderr or b"")[-1000:].decode("utf-8", "replace"))
        raise SystemExit("resync failed")
    print("bega_dev.kbo resynced from bega_prod")
    return 0


if __name__ == "__main__":
    sys.exit(main())
