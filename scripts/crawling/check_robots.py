#!/usr/bin/env python
"""Fetch and validate koreabaseball.com robots.txt before running crawlers.
"""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime
from pathlib import Path
from urllib.robotparser import RobotFileParser

import httpx

logger = logging.getLogger(__name__)
BASE_URL = "https://www.koreabaseball.com"
ROBOTS_URL = f"{BASE_URL}/robots.txt"
DEFAULT_PATHS = [
    "/Record/Player/",
    "/Record/Team/",
    "/Schedule/",
]
SNAPSHOT_DIR = Path("Docs/robots")


def fetch_robots(timeout: float = 10.0) -> str:
    with httpx.Client(timeout=timeout) as client:
        response = client.get(ROBOTS_URL)
        response.raise_for_status()
        return response.text


def save_snapshot(content: str) -> Path:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    snapshot_path = SNAPSHOT_DIR / f"robots_{timestamp}.txt"
    snapshot_path.write_text(content, encoding="utf-8")
    return snapshot_path


def validate_paths(content: str, paths: list[str]) -> list[str]:
    parser = RobotFileParser()
    parser.parse(content.splitlines())

    blocked = []
    for rel_path in paths:
        url = BASE_URL + rel_path
        if not parser.can_fetch("*", url):
            blocked.append(rel_path)
    return blocked


def main():
    parser = argparse.ArgumentParser(description="Check koreabaseball.com robots.txt before crawling.")
    parser.add_argument(
        "--paths",
        nargs="+",
        default=DEFAULT_PATHS,
        help="Relative paths to verify (default: key Record/* endpoints).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="HTTP timeout in seconds (default: 10)",
    )
    args = parser.parse_args()

    logger.info(f"[INFO] Fetching robots.txt from {ROBOTS_URL}")
    content = fetch_robots(timeout=args.timeout)
    snapshot_path = save_snapshot(content)
    logger.info(f"[INFO] Saved robots snapshot to {snapshot_path}")

    blocked = validate_paths(content, args.paths)
    inspected_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info(f"[INFO] Robots inspected at {inspected_at}")

    if blocked:
        logger.info("[ERROR] Crawling blocked for the following paths:")
        for rel_path in blocked:
            logger.info(f"  - {rel_path}")
        raise SystemExit(1)

    logger.info("[OK] All monitored paths are allowed.")


if __name__ == "__main__":
    main()
