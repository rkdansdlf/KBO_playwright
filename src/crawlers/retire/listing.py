"""
Determine retired/inactive player IDs by comparing historical rosters with current active rosters.
"""
from __future__ import annotations

import asyncio
import sys
from typing import Dict, Iterable, List, Optional, Set

from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.compliance import compliance
from src.utils.throttle import throttle


class RetiredPlayerListingCrawler:
    """
    Fetch player ID sets for historical seasons and compute inactive (retired) candidates.
    """

    def __init__(self, request_delay: float = 1.5, pool: Optional[AsyncPlaywrightPool] = None):
        self.request_delay = request_delay
        self.pool = pool
        self.hitter_url = "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx"
        self.pitcher_url = "https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic1.aspx"

    async def _wait(self) -> None:
        await throttle.wait()
        if self.request_delay > throttle.default_delay:
            await asyncio.sleep(self.request_delay - throttle.default_delay)

    async def collect_player_ids_for_year(self, season_year: int) -> Set[str]:
        """Collect all player IDs (hitters + pitchers) for a given season from Record pages."""
        pool = self.pool or AsyncPlaywrightPool(max_pages=1)
        owns_pool = self.pool is None
        await pool.start()
        try:
            page = await pool.acquire()
            try:
                hitter_ids = await self._crawl_record_page_ids(page, self.hitter_url, season_year)
                pitcher_ids = await self._crawl_record_page_ids(page, self.pitcher_url, season_year)
                return hitter_ids | pitcher_ids
            finally:
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()

    async def _crawl_record_page_ids(self, page, base_url: str, year: int) -> Set[str]:
        """Navigate to record page, select year, and paginate to collect player IDs."""
        if not await compliance.is_allowed(base_url):
            print(f"[COMPLIANCE] Blocked record listing: {base_url}")
            return set()

        await self._wait()
        await page.goto(base_url, wait_until="networkidle", timeout=30000)

        # Select Year
        season_selector = 'select[name*="ddlSeason"]'
        await page.wait_for_selector(season_selector, timeout=15000)
        await page.select_option(season_selector, str(year))
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(1000)

        # Select Regular Season (value="0") if available
        try:
            series_selector = 'select[name*="ddlSeries"]'
            await page.select_option(series_selector, "0")
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(500)
        except Exception:
            pass

        ids: Set[str] = set()
        page_num = 1
        while True:
            # Extract IDs from current page
            page_ids = await page.evaluate("""
                () => {
                    const links = document.querySelectorAll('table.tData01.tt tbody tr td:nth-child(2) a');
                    return Array.from(links).map(a => {
                        const href = a.getAttribute('href');
                        const m = href ? href.match(/playerId=(\d+)/) : null;
                        return m ? m[1] : null;
                    }).filter(id => id !== null);
                }
            """)
            ids.update(page_ids)
            print(
                f"    [Year {year}] Page {page_num}: Found {len(page_ids)} IDs (Total: {len(ids)})",
                file=sys.stderr,
            )

            # Try to go to next page
            try:
                # Find current page number from span.on or similar
                current_active_btn = await page.query_selector("div.paging span.on, div.paging a.on")
                current_active_text = await current_active_btn.inner_text() if current_active_btn else "1"
                current_page_val = int(current_active_text.strip())

                # Try to find next numeric button
                next_page_num = current_page_val + 1
                next_page_btn = await page.query_selector(f"div.paging a:has-text('{next_page_num}')")

                # If no next numeric button, try "Next" block button
                if not next_page_btn:
                    next_page_btn = await page.query_selector(
                        "div.paging a[id*='btnNext'], div.paging a:has(img[alt='다음'])"
                    )

                if next_page_btn and await next_page_btn.is_visible():
                    await self._wait()
                    await next_page_btn.click()
                    await page.wait_for_load_state("networkidle")
                    await page.wait_for_timeout(1000)
                    page_num += 1
                else:
                    break
            except Exception:
                break

        return ids

    async def collect_historical_player_ids(self, seasons: Iterable[int]) -> Set[str]:
        historical_ids: Set[str] = set()
        seasons_list = list(seasons)
        print(f"🔍 Collecting historical player IDs for {len(seasons_list)} seasons...", file=sys.stderr)
        for i, season in enumerate(seasons_list):
            print(f"  [{i+1}/{len(seasons_list)}] Fetching IDs for {season}...", file=sys.stderr)
            season_ids = await self.collect_player_ids_for_year(season)
            historical_ids |= season_ids
            print(f"  Found {len(season_ids)} IDs (Total unique: {len(historical_ids)})", file=sys.stderr)
        return historical_ids

    async def determine_inactive_player_ids(
        self,
        start_year: int,
        end_year: int,
        active_year: int,
    ) -> Set[str]:
        """
        Determine inactive player IDs by diffing historical seasons with active roster.
        """
        if start_year > end_year:
            raise ValueError("start_year must be <= end_year")

        seasons = range(start_year, end_year + 1)
        historical_ids = await self.collect_historical_player_ids(seasons)
        print(f"📡 Fetching active player IDs for {active_year}...", file=sys.stderr)
        active_ids = await self.collect_player_ids_for_year(active_year)
        inactive = {pid for pid in historical_ids if pid and pid not in active_ids}
        print(f"✨ Found {len(inactive)} inactive players out of {len(historical_ids)} total unique IDs.", file=sys.stderr)
        return inactive

    def _extract_ids(self, data: Dict[str, List[Dict]]) -> Set[str]:
        ids: Set[str] = set()
        for key in ("hitters", "pitchers"):
            for player in data.get(key, []):
                player_id = player.get("player_id")
                if player_id:
                    ids.add(player_id)
        return ids


async def main():
    crawler = RetiredPlayerListingCrawler(request_delay=1.0)
    inactive_ids = await crawler.determine_inactive_player_ids(
        start_year=1982, end_year=2023, active_year=2024
    )
    print(f"Inactive player IDs discovered: {len(inactive_ids)}")


if __name__ == "__main__":
    asyncio.run(main())
