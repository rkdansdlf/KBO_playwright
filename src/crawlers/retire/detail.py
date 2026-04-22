"""
Fetch retired/inactive player detail pages (hitter & pitcher).
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from playwright.async_api import Page

from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.throttle import throttle
from src.utils.compliance import compliance


class RetiredPlayerDetailCrawler:
    """Download retired player detail pages and extract table payloads."""

    hitter_url = "https://www.koreabaseball.com/Record/Retire/Hitter.aspx"
    pitcher_url = "https://www.koreabaseball.com/Record/Retire/Pitcher.aspx"

    def __init__(self, request_delay: float = 1.5, pool: Optional[AsyncPlaywrightPool] = None):
        self.request_delay = request_delay
        self.pool = pool

    async def fetch_player(self, player_id: str, retries: int = 2) -> Dict[str, Any]:
        """
        Fetch hitter & pitcher pages for the given player ID.
        """
        pool = self.pool or AsyncPlaywrightPool(max_pages=1)
        owns_pool = self.pool is None
        await pool.start()
        try:
            for attempt in range(retries + 1):
                try:
                    page = await pool.acquire()
                    try:
                        hitter_payload = await self._fetch_page(page, self.hitter_url, player_id)
                        pitcher_payload = await self._fetch_page(page, self.pitcher_url, player_id)
                        return {
                            "player_id": player_id,
                            "hitter": hitter_payload,
                            "pitcher": pitcher_payload,
                        }
                    finally:
                        await pool.release(page)
                except Exception as exc:
                    if attempt == retries:
                        raise exc
                    print(f"⚠️ Retry {attempt + 1} for {player_id} due to: {exc}")
                    await asyncio.sleep(2.0 * (attempt + 1))
        finally:
            if owns_pool:
                await pool.close()

        return {
            "player_id": player_id,
            "hitter": hitter_payload,
            "pitcher": pitcher_payload,
        }

    async def _fetch_page(self, page: Page, base_url: str, player_id: str) -> Optional[Dict[str, Any]]:
        url = f"{base_url}?playerId={player_id}"
        if not await compliance.is_allowed(url):
            print(f"⚠️  BLOCKED by compliance: {url}")
            return None

        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await throttle.wait()

        profile_text = await self._extract_profile_text(page)
        photo_url = await self._extract_photo_url(page)
        tables = await self._extract_tables(page)

        if not profile_text and not tables:
            return None

        return {
            "url": url,
            "profile_text": profile_text,
            "photo_url": photo_url,
            "tables": tables,
        }

    async def _extract_profile_text(self, page: Page) -> Optional[str]:
        selectors = [
            "#cphContents_cphContents_cphContents_playerProfile",
            "#cphContents_cphContents_cphContents_ucPlayerProfile_lblProfile",
            ".player-info",
            ".playerInfo",
            ".player_profile",
        ]
        for selector in selectors:
            element = await page.query_selector(selector)
            if element:
                text = await element.inner_text()
                cleaned = text.strip()
                if cleaned:
                    return cleaned
        return None

    async def _extract_photo_url(self, page: Page) -> Optional[str]:
        # Retired pages often use different image structures
        selector = "div.photo img"
        element = await page.query_selector(selector)
        if not element:
            # Fallback for old/empty pages
            element = await page.query_selector("#imgProgile")

        if element:
            src = await element.get_attribute("src")
            if src and "/person/" in src:
                if src.startswith("//"):
                    return f"https:{src}"
                if src.startswith("/"):
                    return f"https://www.koreabaseball.com{src}"
                return src
        return None

    async def _extract_tables(self, page: Page) -> List[Dict[str, Any]]:
        script = """
        (tables) => tables.map(table => {
            const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.innerText.trim());
            let rows = Array.from(table.querySelectorAll('tbody tr')).map(tr =>
                Array.from(tr.querySelectorAll('th,td')).map(td => td.innerText.trim())
            );
            if (rows.length === 0) {
                const rawRows = Array.from(table.querySelectorAll('tr'));
                rows = rawRows.map(tr => Array.from(tr.querySelectorAll('th,td')).map(td => td.innerText.trim()));
            }
            const caption = table.querySelector('caption')?.innerText.trim() || null;
            return { caption, headers, rows };
        })
        """
        try:
            return await page.eval_on_selector_all("table", script)
        except Exception:
            return []


async def main():
    crawler = RetiredPlayerDetailCrawler()
    sample_id = "78137"
    payload = await crawler.fetch_player(sample_id)
    print(f"Fetched player {sample_id}")
    print(f"Hitter tables: {len(payload.get('hitter', {}).get('tables', []))}")
    print(f"Pitcher tables: {len(payload.get('pitcher', {}).get('tables', []))}")


if __name__ == "__main__":
    asyncio.run(main())
