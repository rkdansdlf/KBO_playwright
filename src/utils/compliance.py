"""Robots.txt compliance checker for KBO Data Crawler.

Ensure that we follow Disallow rules from koreabaseball.com.

"""

from __future__ import annotations

import asyncio
import logging
import time
import urllib.robotparser
from datetime import datetime
from http import HTTPStatus
from pathlib import Path

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore[assignment]

import httpx

from src.constants import KST

logger = logging.getLogger(__name__)


def _save_robots_snapshot(robots_url: str, content: str) -> Path:
    snapshot_dir = Path("docs/robots")
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    snapshot_path = snapshot_dir / f"robots_{timestamp}.txt"
    with snapshot_path.open("w", encoding="utf-8") as f:
        f.write(f"# Source: {robots_url}\n")
        f.write(f"# Fetched at: {datetime.now(KST).isoformat()}\n\n")
        f.write(content)
    return snapshot_path


class ComplianceChecker:
    """Fetch and parses robots.txt to check crawling permissions."""

    _instance: ComplianceChecker | None = None

    def __init__(self, robots_url: str = "https://www.koreabaseball.com/robots.txt") -> None:
        """Initialize a new instance.

        Args:
            robots_url: Robots URL.
            robots_url: Robots URL.

        """
        self.robots_url = robots_url

        self.parser = urllib.robotparser.RobotFileParser()
        self.last_fetch_time = 0.0
        self.fetch_interval = 86400  # 24 hours
        self._lock = asyncio.Lock()

    @classmethod
    def get_instance(cls) -> ComplianceChecker:
        """Get instance.

        Returns:
            ComplianceChecker instance.

        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def _ensure_loaded(self) -> None:
        """Fetch and parse robots.txt if needed."""
        now = time.time()
        if now - self.last_fetch_time > self.fetch_interval:
            async with self._lock:
                # Double check inside lock
                if now - self.last_fetch_time > self.fetch_interval:
                    logger.info("[COMPLIANCE] Fetching robots.txt from %s", self.robots_url)
                    try:
                        async with httpx.AsyncClient() as client:
                            response = await client.get(self.robots_url, timeout=10.0)
                            if response.status_code == HTTPStatus.OK:
                                content = response.text
                                self.parser.parse(content.splitlines())
                                self.last_fetch_time = now

                                # Save snapshot
                                try:
                                    snapshot_path = await asyncio.to_thread(
                                        _save_robots_snapshot, self.robots_url, content
                                    )
                                    logger.info("[COMPLIANCE] robots.txt snapshot saved to %s", snapshot_path)
                                except OSError:
                                    logger.exception("[COMPLIANCE] Failed to save snapshot")

                                logger.info("[COMPLIANCE] robots.txt loaded successfully.")
                            else:
                                logger.warning(
                                    "Failed to fetch robots.txt (Status %s). Using fallback.",
                                    response.status_code,
                                )
                                # Fallback: assume everything is allowed or use a default
                                self.parser.parse(["User-agent: *", "Disallow:"])
                                self.last_fetch_time = now
                    except httpx.HTTPError:
                        logger.exception("[COMPLIANCE] Error fetching robots.txt")
                        # Fallback on error
                        self.parser.parse(["User-agent: *", "Disallow:"])
                        self.last_fetch_time = now

    async def is_allowed(self, url: str, user_agent: str = "*") -> bool:
        """Check if the given URL is allowed for crawling.

        Args:
            url: Url.
            user_agent: User Agent.
            url: Url.
            user_agent: User Agent.

        """
        await self._ensure_loaded()

        allowed = self.parser.can_fetch(user_agent, url)
        if not allowed:
            logger.info("[COMPLIANCE] BLOCKED: %s is DISALLOWED by robots.txt", url)
        return allowed

    def is_allowed_sync(self, url: str, user_agent: str = "*") -> bool:
        """Provide synchronous version of is_allowed (Wait if not loaded).

        Args:
            url: Url.
            user_agent: User Agent.
            url: Url.
            user_agent: User Agent.

        """
        # If not loaded, we perform a sync fetch (minimal blocking)

        now = time.time()
        if now - self.last_fetch_time > self.fetch_interval:
            logger.info("[COMPLIANCE] Sync fetching robots.txt from %s", self.robots_url)
            try:
                if httpx is None:
                    logger.warning("[COMPLIANCE] httpx not installed, skipping robots.txt fetch")
                    time.sleep(self.fetch_interval)
                    return False

                response = httpx.get(self.robots_url, timeout=10.0)
                if response.status_code == HTTPStatus.OK:
                    self.parser.parse(response.text.splitlines())
                    self.last_fetch_time = now
                else:
                    self.parser.parse(["User-agent: *", "Disallow:"])
                    self.last_fetch_time = now
            except httpx.HTTPError:
                logger.exception("[COMPLIANCE] Error sync fetching robots.txt")
                self.parser.parse(["User-agent: *", "Disallow:"])
                self.last_fetch_time = now

        allowed = self.parser.can_fetch(user_agent, url)
        if not allowed:
            logger.info("[COMPLIANCE] BLOCKED (sync): %s is DISALLOWED by robots.txt", url)
        return allowed


# Global instance
compliance = ComplianceChecker.get_instance()
