"""Fetch Futures League stats from player profile pages."""
from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional, List

from playwright.async_api import async_playwright, Page


class FuturesProfileCrawler:
    hitter_profile_url = "https://www.koreabaseball.com/Futures/Player/HitterDetail.aspx"
    pitcher_profile_url = "https://www.koreabaseball.com/Futures/Player/PitcherDetail.aspx"

    def __init__(self, request_delay: float = 1.5) -> None:
        self.request_delay = request_delay

    async def fetch_player_futures(self, player_id: str) -> Dict[str, Any]:
        """Fetch futures profile data (tables + profile text) for a player."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            profile_text: Optional[str] = None
            tables: List[Dict[str, Any]] = []

            try:
                payload = await self._scrape_profile(page, self.hitter_profile_url, player_id)
                if payload:
                    profile_text = payload.get("profile_text") or profile_text
                    tables.extend(payload.get("tables", []))

                payload = await self._scrape_profile(page, self.pitcher_profile_url, player_id)
                if payload:
                    profile_text = payload.get("profile_text") or profile_text
                    tables.extend(payload.get("tables", []))
            finally:
                await browser.close()

        return {
            "player_id": player_id,
            "profile_text": profile_text,
            "tables": tables,
        }

    async def _scrape_profile(self, page: Page, base_url: str, player_id: str) -> Optional[Dict[str, Any]]:
        url = f"{base_url}?playerId={player_id}"
        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
        except Exception:
            return None

        await asyncio.sleep(self.request_delay)

        profile_text = await self._extract_profile_text(page)
        futures_tables = await self._extract_futures_tables(page)

        if not futures_tables:
            return None

        return {
            "url": url,
            "profile_text": profile_text,
            "tables": futures_tables,
        }

    async def _extract_profile_text(self, page: Page) -> Optional[str]:
        selectors = [
            "#cphContents_cphContents_cphContents_playerProfile",
            "#cphContents_cphContents_cphContents_ucPlayerProfile_lblProfile",
            ".player-info",
            ".playerInfo",
        ]
        for selector in selectors:
            try:
                element = await page.query_selector(selector)
            except Exception:
                element = None
            if element:
                try:
                    text = await element.inner_text()
                except Exception:
                    continue
                if text and text.strip():
                    return text.strip()
        return None

    async def _extract_futures_tables(self, page: Page) -> List[Dict[str, Any]]:
        tab_selectors = [
            "a:has-text(\"퓨처스\")",
            "#cphContents_cphContents_cphContents_ucPlayerYearTabs a[href*='Futures']",
            "#cphContents_cphContents_cphContents_ucPlayerRecord_tabList a[href*='Futures']",
        ]

        futures_clicked = False
        for selector in tab_selectors:
            try:
                tab = await page.wait_for_selector(selector, timeout=3000)
            except Exception:
                continue
            if tab:
                try:
                    await tab.click()
                    futures_clicked = True
                    break
                except Exception:
                    continue

        if not futures_clicked:
            existing = await page.query_selector(
                "div#cphContents_cphContents_cphContents_udpPlayerFutures table,"
                " div#cphContents_cphContents_cphContents_udpFuturesRecord table"
            )
            if not existing:
                return []

        await asyncio.sleep(self.request_delay)

        table_selector = (
            "div#cphContents_cphContents_cphContents_udpPlayerFutures table,"
            " div#cphContents_cphContents_cphContents_udpFuturesRecord table"
        )
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
            tables = await page.eval_on_selector_all(table_selector, script)
        except Exception:
            tables = []
        if not tables:
            try:
                tables = await page.eval_on_selector_all("table", script)
            except Exception:
                tables = []

        return tables


async def main():
    crawler = FuturesProfileCrawler()
    sample_id = "78137"
    payload = await crawler.fetch_player_futures(sample_id)
    print(f"Fetched Futures tables for {sample_id}: {len(payload['tables'])}")


if __name__ == "__main__":
    asyncio.run(main())
