"""Fetch Futures League stats from player profile pages."""
from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional, List

from playwright.async_api import Page
from bs4 import BeautifulSoup

from src.utils.playwright_pool import AsyncPlaywrightPool

class FuturesProfileCrawler:
    hitter_profile_url = "https://www.koreabaseball.com/Futures/Player/HitterDetail.aspx"
    pitcher_profile_url = "https://www.koreabaseball.com/Futures/Player/PitcherDetail.aspx"

    def __init__(self, request_delay: float = 1.5, pool: Optional[AsyncPlaywrightPool] = None) -> None:
        self.request_delay = request_delay
        self.pool = pool

    async def fetch_player_futures(self, player_id: str) -> Dict[str, Any]:
        """Fetch futures profile data (tables + profile text) for a player."""
        pool = self.pool or AsyncPlaywrightPool(max_pages=1, context_kwargs={"locale": "ko-KR"})
        owns_pool = self.pool is None
        await pool.start()
        try:
            page = await pool.acquire()
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
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()

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
            existing = await page.query_selector("table#tblHitterRecord, table#tblPitcherRecord")
            if not existing:
                return []

        await asyncio.sleep(self.request_delay)

        # Get HTML content and parse with BeautifulSoup for proper encoding
        html_content = await page.content()
        soup = BeautifulSoup(html_content, 'lxml')

        tables = []

        # Look for specific Futures tables by ID and mark their type
        hitter_table = soup.find('table', id='tblHitterRecord')
        if hitter_table:
            table_data = self._parse_table_with_bs4(hitter_table)
            if table_data:
                table_data['_table_type'] = 'HITTER'  # Add explicit type marker
                tables.append(table_data)

        pitcher_table = soup.find('table', id='tblPitcherRecord')
        if pitcher_table:
            table_data = self._parse_table_with_bs4(pitcher_table)
            if table_data:
                table_data['_table_type'] = 'PITCHER'  # Add explicit type marker
                tables.append(table_data)

        # If no tables found by ID, try other methods
        if not tables:
            futures_divs = soup.find_all('div', id=lambda x: x and 'Futures' in x if x else False)
            for div in futures_divs:
                for table_elem in div.find_all('table'):
                    table_data = self._parse_table_with_bs4(table_elem)
                    if table_data:
                        tables.append(table_data)

        return tables

    def _parse_table_with_bs4(self, table_elem) -> Optional[Dict[str, Any]]:
        """Parse a table element using BeautifulSoup for proper Korean encoding."""
        try:
            # Extract caption
            caption_elem = table_elem.find('caption')
            caption = caption_elem.get_text(strip=True) if caption_elem else None

            # Extract summary attribute
            summary = table_elem.get('summary', '')

            # Extract headers
            headers = []
            thead = table_elem.find('thead')
            if thead:
                header_row = thead.find('tr')
                if header_row:
                    headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]

            # Extract rows
            rows = []
            tbody = table_elem.find('tbody')
            row_container = tbody if tbody else table_elem

            for tr in row_container.find_all('tr'):
                cells = [cell.get_text(strip=True) for cell in tr.find_all(['th', 'td'])]
                if cells and any(cell for cell in cells):  # Skip empty rows
                    rows.append(cells)

            # If no explicit headers, first row might be headers
            if not headers and rows:
                headers = rows[0]
                rows = rows[1:]

            if headers or rows:
                return {
                    'caption': caption,
                    'summary': summary,
                    'headers': headers,
                    'rows': rows
                }
        except Exception:
            pass

        return None


async def main():
    crawler = FuturesProfileCrawler()
    sample_id = "78137"
    payload = await crawler.fetch_player_futures(sample_id)
    print(f"Fetched Futures tables for {sample_id}: {len(payload['tables'])}")


if __name__ == "__main__":
    asyncio.run(main())
