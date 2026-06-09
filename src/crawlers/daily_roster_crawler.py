"""
Crawler for Daily 1st Team Registration Status.
Source: https://www.koreabaseball.com/Player/Register.aspx
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)

from playwright.async_api import Page  # noqa: E402
from playwright.async_api import TimeoutError as PlaywrightTimeoutError  # noqa: E402
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential  # noqa: E402

from src.utils.playwright_pool import AsyncPlaywrightPool  # noqa: E402
from src.utils.team_codes import resolve_team_code  # noqa: E402


class DailyRosterCrawler:
    """Crawl daily roster changes."""

    def __init__(self, request_delay: float = 1.0, pool: AsyncPlaywrightPool | None = None):
        self.base_url = "https://www.koreabaseball.com/Player/Register.aspx"
        self.request_delay = request_delay
        self.pool = pool

    async def crawl_date_range(self, start_date: str, end_date: str, save_callback=None) -> list[dict[str, Any]]:
        """Crawl roster for a range of dates (format: YYYY-MM-DD)."""
        s_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        e_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        results = []
        pool = self.pool or AsyncPlaywrightPool(max_pages=1)
        owns_pool = self.pool is None
        await pool.start()
        try:
            page = await pool.acquire()
            try:
                # Initial load with Exponential Backoff
                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(3),
                    wait=wait_exponential(multiplier=1, min=2, max=10),
                    retry=retry_if_exception_type(PlaywrightTimeoutError),
                ):
                    with attempt:
                        await page.goto(self.base_url, wait_until="networkidle", timeout=30000)

                # Create a date range list
                from datetime import timedelta

                delta = e_date - s_date
                dates = [s_date + timedelta(days=i) for i in range(delta.days + 1)]

                for d in dates:
                    roster = await self._crawl_date(page, d)
                    if roster:
                        if save_callback:
                            # Call callback (synchronously if it's not async)
                            try:
                                if asyncio.iscoroutinefunction(save_callback):
                                    await save_callback(roster)
                                else:
                                    save_callback(roster)
                            except Exception:
                                logger.exception("⚠️ Callback error")

                        results.extend(roster)
            finally:
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()
        return results

    async def _crawl_date(self, page: Page, target_date: date) -> list[dict[str, Any]]:
        date_str = target_date.strftime("%Y%m%d")
        logger.info(f"📅 Crawling Roster for {target_date}...")

        # Strategy:
        # 1. Set hidden input `hfSearchDate`
        # 2. Call `javascript:__doPostBack(...)`
        await page.evaluate(
            f"document.getElementById('cphContents_cphContents_cphContents_hfSearchDate').value = '{date_str}';"
        )

        # Alternative: We are already on the page. Use JS to trigger the actual change function if exposed?
        # Let's try the safest path: Set Value -> Call `__doPostBack` on a harmless control or just the one used by datepicker.
        # The datepicker usually calls `__doPostBack('...txtSearchDate', '')`.
        triggers = ["ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$btnCalendarSelect"]

        try:
            async with page.expect_response(lambda response: "Register.aspx" in response.url, timeout=5000):
                await page.evaluate(f"__doPostBack('{triggers[0]}', '')")
        except (TimeoutError, PlaywrightTimeoutError):
            logger.debug("__doPostBack timed out (expected for fast/no-op)")
        await page.wait_for_timeout(2000)  # Wait for UI update

        # DEBUG: Check visible date
        visible_date = await page.evaluate(
            "document.querySelector('.date') ? document.querySelector('.date').innerText : 'No Date Element'"
        )
        logger.debug("   Visible Page Date: %s", visible_date)

        # Now iterate teams
        daily_records = []

        # The page has a list of teams. We need to click each or call `fnSearchChange('CODE')`.
        # Team codes map:
        # LG, HH(Hanwha), SS(Samsung), KT, ...
        # Standard KBO codes are used.

        # 10 Teams
        teams = ["LG", "HH", "SS", "KT", "OB", "LT", "HT", "NC", "SK", "WO"]
        # (Check if codes match KBO_TEAM_CODES keys or values. 'OB' is Doosan)

        for t_code in teams:
            try:
                # Switch Team
                await page.evaluate(f"fnSearchChange('{t_code}')")
                await page.wait_for_timeout(300)  # simple wait for update panel

                # Extract data
                records = await self._extract_table(page, t_code, target_date)
                daily_records.extend(records)
            except Exception:
                logger.exception(f"⚠️ Error crawling team {t_code}")

        return daily_records

    async def _extract_table(self, page: Page, team_code: str, roster_date: date) -> list[dict[str, Any]]:
        # Selector for the tables.
        # There are multiple `.tEx` tables for Pitcher, Catcher, etc.
        # We need to grab all.

        script = """
        () => {
            const results = [];
            const tables = document.querySelectorAll('#cphContents_cphContents_cphContents_udpRecord table.tNData');
            // DEBUG: return count if 0
            if (tables.length === 0) return [{ 'status': 'no_tables', 'debug_html_len': document.body.innerHTML.length }];

            tables.forEach(table => {
                let category = 'Unknown';
                let positionColIdx = -1;
                const headers = table.querySelectorAll('th');

                headers.forEach((th, idx) => {
                    if (th.innerText.trim() === '포지션') {
                        positionColIdx = idx;
                    }
                });

                if (positionColIdx === -1 && headers.length >= 2) {
                    category = headers[1].innerText.trim();
                    if (category === '선수명' && headers.length >= 3) {
                         category = headers[2].innerText.trim();
                    }
                }

                const rows = table.querySelectorAll('tbody tr');
                rows.forEach(tr => {
                    const cells = tr.querySelectorAll('td');
                    if (cells.length < 4) return; // "registered... none" row

                    const backNum = cells[0].innerText.trim();
                    const nameLink = cells[1].querySelector('a');
                    const name = cells[1].innerText.trim();
                    let playerId = null;

                    if (nameLink) {
                        const href = nameLink.getAttribute('href');
                        const url = new URL(href, window.location.origin);
                        playerId = url.searchParams.get('playerId');
                    }

                    let pos = category;
                    if (positionColIdx !== -1 && cells.length > positionColIdx) {
                        pos = cells[positionColIdx].innerText.trim();
                    }

                    if (playerId) {
                        results.push({
                            'player_id': playerId,
                            'player_name': name,
                            'back_number': backNum,
                            'category': pos
                        });
                    }
                });
            });
            return results;
        }
        """

        data = await page.evaluate(script)
        if data and data[0].get("status") == "no_tables":
            logger.debug("   No tables found for team %s", team_code)
            return []

        # Post-process
        cleaned = []
        for item in data:
            cleaned.append(
                {
                    "roster_date": roster_date,
                    "team_code": self._normalize_team(team_code, roster_date.year),
                    "player_id": int(item["player_id"]),
                    "player_name": item["player_name"],
                    "position": self._clean_category(item["category"]),
                    "back_number": item["back_number"],
                }
            )
        return cleaned

    def _normalize_team(self, code: str, season_year: int | None = None) -> str:
        resolved = resolve_team_code(code, season_year)
        return resolved if resolved else code

    def _clean_category(self, cat: str) -> str:
        # "투수 (14명)" -> "투수"
        if "(" in cat:
            return cat.split("(")[0].strip()
        return cat


async def main():
    crawler = DailyRosterCrawler()
    # Test for yesterday
    (datetime.now().date()).strftime("%Y-%m-%d")
    data = await crawler.crawl_date_range("2024-05-20", "2024-05-20")
    logger.info(f"Crawled {len(data)} records.")
    for r in data[:5]:
        logger.info(r)


if __name__ == "__main__":
    asyncio.run(main())
