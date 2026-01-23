from __future__ import annotations

import asyncio
from typing import Dict, List, Optional

from playwright.async_api import Page

from src.utils.status_parser import parse_status_from_text
from src.utils.playwright_pool import AsyncPlaywrightPool


class PlayerStatusConfirmer:
    """Confirm suspicious player statuses via profile pages."""

    def __init__(
        self,
        *,
        request_delay: float = 1.5,
        max_confirmations: int = 200,
        headless: bool = True,
        pool: Optional[AsyncPlaywrightPool] = None,
    ):
        self.base_url = "https://www.koreabaseball.com/Record/Player/HitterDetail/Basic.aspx"
        self.request_delay = request_delay
        self.max_confirmations = max_confirmations
        self.headless = headless
        self.pool = pool

    async def confirm_entries(self, entries: List[Dict[str, object]]) -> Dict[str, int]:
        """Mutates entries in-place when profile confirmation succeeds."""
        suspects = [entry for entry in entries if entry.get("status") in {"retired", "staff"}]
        attempts = 0
        confirmed = 0
        if not suspects:
            return {"attempted": 0, "confirmed": 0}

        pool = self.pool or AsyncPlaywrightPool(max_pages=1, headless=self.headless)
        owns_pool = self.pool is None
        await pool.start()
        try:
            page = await pool.acquire()
            try:
                for entry in suspects:
                    if attempts >= self.max_confirmations:
                        break
                    player_id = entry.get("player_id")
                    if not player_id:
                        continue
                    result = await self._confirm_single(page, str(player_id))
                    attempts += 1
                    if result:
                        entry.update(result)
                        confirmed += 1
            finally:
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()
        return {"attempted": attempts, "confirmed": confirmed}

    async def _confirm_single(self, page: Page, player_id: str) -> Optional[Dict[str, str]]:
        url = f"{self.base_url}?playerId={player_id}"
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(self.request_delay)
        text = await page.inner_text("body")
        parsed = parse_status_from_text(text)
        if parsed:
            status, staff_role = parsed
            return {"status": status, "staff_role": staff_role, "status_source": "profile"}
        return None
