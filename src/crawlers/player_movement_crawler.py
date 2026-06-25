"""Crawler for Player Movement (Trade, FA, Waiver, etc.).
Source: https://www.koreabaseball.com/Player/Trade.aspx.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from sqlalchemy.exc import SQLAlchemyError
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.db.engine import SessionLocal
from src.repositories.source_registry_repository import save_raw_snapshots
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.playwright_retry import NAV_TIMEOUT

logger = logging.getLogger(__name__)

PLAYER_MOVEMENT_SOURCE_KEY = "kbo_player_movement"

PLAYER_MOVEMENT_CRAWL_EXCEPTIONS = (
    PlaywrightError,
    PlaywrightTimeoutError,
    TimeoutError,
    RuntimeError,
    ValueError,
    TypeError,
    KeyError,
    IndexError,
    OSError,
)
PLAYER_MOVEMENT_SAVE_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)


class PlayerMovementCrawler:
    """Crawl player status changes (Trade, FA, Waiver, etc.)."""

    def __init__(self, request_delay: float = 1.0, pool: AsyncPlaywrightPool | None = None) -> None:
        self.base_url = "https://www.koreabaseball.com/Player/Trade.aspx"
        self.request_delay = request_delay
        self.pool = pool
        self._raw_pages: list[dict[str, object]] = []

    async def crawl_years(
        self, start_year: int, end_year: int, *, save_snapshots: bool = False
    ) -> list[dict[str, Any]]:
        """Crawl data for a range of years."""
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
                        await page.goto(self.base_url, wait_until="networkidle", timeout=NAV_TIMEOUT)
                await self._capture_snapshot(page, self.base_url)

                for year in range(start_year, end_year + 1):
                    year_data = await self._crawl_year(page, year)
                    results.extend(year_data)
            finally:
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()
        if save_snapshots:
            self._save_snapshots()
        return results

    async def _crawl_year(self, page: Page, year: int) -> list[dict[str, Any]]:
        logger.info("🔄 Crawling Player Movements for Year: %s...", year)
        results = []

        # Select Year
        try:
            # 1. Select Year
            await page.select_option("#selYear", str(year))

            # 3. Trigger Search
            try:
                # Expect AJAX update or just wait for network idle
                # The page might not reload URL, just DOM update.
                await page.click("#btnSearch")
            except TimeoutError:
                logger.exception("⚠️ Search click timeout - Page might have updated without reload or network is slow.")

            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(1000)
            await self._capture_snapshot(page, f"{self.base_url}?year={year}")

            # 4. Iterate Pagination
            page_num = 1
            prev_page_data_str = ""

            while True:
                logger.info("   PAGE %s: Extracting...", page_num)

                # Extract current page rows
                data = await self._extract_table(page)
                if not data:
                    logger.warning("   ⚠️ No data found on this page.")

                # Check for duplicates (Stop infinite loop)
                current_data_str = str(data)
                if current_data_str == prev_page_data_str:
                    logger.info("   🛑 Duplicate data detected (Same as Page %s). Stopping.", page_num - 1)
                    break
                prev_page_data_str = current_data_str

                results.extend(data)

                # --- Pagination Logic ---
                # Check for Current Page + 1 link
                next_page_num = page_num + 1
                next_page_link = page.get_by_role("link", name=str(next_page_num), exact=True)

                clicked = False

                if await next_page_link.count() > 0 and await next_page_link.is_visible():
                    # Click specific number
                    await next_page_link.click()
                    clicked = True
                else:
                    next_arrow = page.locator("a.pg_next")

                    if await next_arrow.count() > 0 and await next_arrow.is_visible():
                        # Check if it looks disabled (sometimes images are different)
                        # But simpler is to rely on duplicate data check if it fails to advance.
                        await next_arrow.click()
                        clicked = True
                    else:
                        # Try 'next block' if `pg_next` is hidden but `pg_last` exists?
                        # Usually `pg_next` should be available if there is a next page.
                        pass

                if not clicked:
                    logger.info("   ✅ Finished Year %s. No more next pages.", year)
                    break

                # Wait for table update
                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(800)

                # Verify we actually moved?
                # Optional: Check `.paging a.on` text.
                # But duplicates check covers most cases.

                page_num += 1

        except PLAYER_MOVEMENT_CRAWL_EXCEPTIONS:
            logger.exception("⚠️ Error processing year %s", year)

        logger.info("✅ Year %s: Collected %s records.", year, len(results))
        return results

    async def _capture_snapshot(self, page: Page, url: str) -> None:
        self._raw_pages.append(
            {
                "source_key": PLAYER_MOVEMENT_SOURCE_KEY,
                "url": url,
                "html": await page.content(),
                "status_code": 200,
            },
        )

    def _save_snapshots(self) -> None:
        if not self._raw_pages:
            return
        with SessionLocal() as session:
            try:
                saved = save_raw_snapshots(session, list(self._raw_pages))
                session.commit()
                logger.info("[PLAYER_MOVEMENT] Saved %s raw snapshots.", saved)
            except PLAYER_MOVEMENT_SAVE_EXCEPTIONS:
                session.rollback()
                logger.exception("[PLAYER_MOVEMENT] Failed to save raw snapshots")
                raise
            finally:
                self._raw_pages.clear()

    async def _extract_table(self, page: Page) -> list[dict[str, Any]]:
        script = """
        () => {
            const results = [];
            const rows = document.querySelectorAll('.tbl-type02 tbody tr');

            rows.forEach(tr => {
                const cells = tr.querySelectorAll('td');
                if (cells.length < 5) return;

                // Columns: Date, Section, Team, Player, Remarks
                const dateRaw = cells[0].innerText.trim();
                const section = cells[1].innerText.trim();
                const team = cells[2].innerText.trim();
                const player = cells[3].innerText.trim();
                const remarks = cells[4].innerText.trim();

                if (!dateRaw) return; // Empty row?

                results.push({
                    'date': dateRaw,
                    'section': section,
                    'team_code': team,
                    'player_name': player,
                    'remarks': remarks
                });
            });

            return results;
        }
        """
        data = await page.evaluate(script)

        # Post-process (Validate Key fields)
        return [item for item in data if item["date"] and item["section"]]


async def main() -> None:
    # Test run
    crawler = PlayerMovementCrawler()
    data = await crawler.crawl_years(2023, 2023)
    logger.info("Total collected: %s", len(data))
    for d in data[:5]:
        logger.info(d)


if __name__ == "__main__":
    asyncio.run(main())
