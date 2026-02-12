"""
Robots.txt compliance checker for KBO Data Crawler.
Ensures that we follow Disallow rules from koreabaseball.com.
"""
import urllib.robotparser
import asyncio
import time
from typing import Optional
import httpx

class ComplianceChecker:
    """
    Fetches and parses robots.txt to check crawling permissions.
    """
    _instance: Optional["ComplianceChecker"] = None
    
    def __init__(self, robots_url: str = "https://www.koreabaseball.com/robots.txt"):
        self.robots_url = robots_url
        self.parser = urllib.robotparser.RobotFileParser()
        self.last_fetch_time = 0
        self.fetch_interval = 86400  # 24 hours
        self._lock = asyncio.Lock()

    @classmethod
    def get_instance(cls) -> "ComplianceChecker":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def _ensure_loaded(self):
        """Fetch and parse robots.txt if needed."""
        now = time.time()
        if now - self.last_fetch_time > self.fetch_interval:
            async with self._lock:
                # Double check inside lock
                if now - self.last_fetch_time > self.fetch_interval:
                    print(f"[COMPLIANCE] Fetching robots.txt from {self.robots_url}")
                    try:
                        async with httpx.AsyncClient() as client:
                            response = await client.get(self.robots_url, timeout=10.0)
                            if response.status_code == 200:
                                content = response.text
                                self.parser.parse(content.splitlines())
                                self.last_fetch_time = now
                                print("[COMPLIANCE] robots.txt loaded successfully.")
                            else:
                                print(f"[COMPLIANCE] Failed to fetch robots.txt (Status {response.status_code}). Using fallback.")
                                # Fallback: assume everything is allowed or use a default
                                self.parser.parse(["User-agent: *", "Disallow:"])
                                self.last_fetch_time = now
                    except Exception as e:
                        print(f"[COMPLIANCE] Error fetching robots.txt: {e}")
                        # Fallback on error
                        self.parser.parse(["User-agent: *", "Disallow:"])
                        self.last_fetch_time = now

    async def is_allowed(self, url: str, user_agent: str = "*") -> bool:
        """Check if the given URL is allowed for crawling."""
        await self._ensure_loaded()
        allowed = self.parser.can_fetch(user_agent, url)
        if not allowed:
            print(f"[COMPLIANCE] BLOCKED: {url} is DISALLOWED by robots.txt")
        return allowed

    def is_allowed_sync(self, url: str, user_agent: str = "*") -> bool:
        """Synchronous version of is_allowed (Wait if not loaded)."""
        # If not loaded, we perform a sync fetch (minimal blocking)
        now = time.time()
        if now - self.last_fetch_time > self.fetch_interval:
            print(f"[COMPLIANCE] Sync fetching robots.txt from {self.robots_url}")
            try:
                import httpx
                response = httpx.get(self.robots_url, timeout=10.0)
                if response.status_code == 200:
                    self.parser.parse(response.text.splitlines())
                    self.last_fetch_time = now
                else:
                    self.parser.parse(["User-agent: *", "Disallow:"])
                    self.last_fetch_time = now
            except Exception as e:
                print(f"[COMPLIANCE] Error sync fetching robots.txt: {e}")
                self.parser.parse(["User-agent: *", "Disallow:"])
                self.last_fetch_time = now

        allowed = self.parser.can_fetch(user_agent, url)
        if not allowed:
            print(f"[COMPLIANCE] BLOCKED (sync): {url} is DISALLOWED by robots.txt")
        return allowed

# Global instance
compliance = ComplianceChecker.get_instance()
