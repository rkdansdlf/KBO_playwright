"""
Determine retired/inactive player IDs by comparing historical rosters with current active rosters.
"""
from __future__ import annotations

import asyncio
import sys
from typing import Iterable, List, Set, Dict, Optional

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
                hitter_ids = await self._crawl_record_page_ids_with_teams(page, self.hitter_url, season_year)
                pitcher_ids = await self._crawl_record_page_ids_with_teams(page, self.pitcher_url, season_year)
                return hitter_ids | pitcher_ids
            finally:
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()

    async def _crawl_record_page_ids_with_teams(self, page, base_url: str, year: int) -> Set[str]:
        """Navigate to record page, select year, and iterate through all teams to collect IDs."""
        if not await compliance.is_allowed(base_url):
            print(f"[COMPLIANCE] Blocked record listing: {base_url}", file=sys.stderr)
            return set()

        await self._wait()
        await page.goto(base_url, wait_until="load", timeout=30000)

        # Select Year
        season_selector = 'select[id$="ddlSeason_ddlSeason"]'
        await page.wait_for_selector(season_selector, timeout=15000)
        await page.select_option(season_selector, str(year))
        await page.evaluate("el => { if (el.onchange) el.onchange(); else el.dispatchEvent(new Event('change', { bubbles: true })); }", await page.query_selector(season_selector))
        try:
            await page.wait_for_load_state("load", timeout=10000)
        except:
            pass
        await page.wait_for_timeout(1000)

        # Get all team codes
        team_selector = 'select[id$="ddlTeam_ddlTeam"]'
        team_options = await page.locator(f"{team_selector} option").all()
        team_codes = []
        for opt in team_options:
            val = await opt.get_attribute("value")
            if val and val != "" and val != "9999":
                team_codes.append(val)
        
        if not team_codes:
            # Fallback to no team selection (current page)
            return await self._collect_ids_from_pages(page, year)

        all_ids: Set[str] = set()
        for code in team_codes:
            print(f"    [Year {year}] Fetching team {code}", file=sys.stderr)
            await page.select_option(team_selector, code)
            await page.evaluate("el => { if (el.onchange) el.onchange(); else el.dispatchEvent(new Event('change', { bubbles: true })); }", await page.query_selector(team_selector))
            try:
                await page.wait_for_load_state("load", timeout=10000)
            except:
                pass
            await page.wait_for_timeout(500)
            
            team_ids = await self._collect_ids_from_pages(page, year)
            all_ids.update(team_ids)
            
        return all_ids

    async def _collect_ids_from_pages(self, page, year: int) -> Set[str]:
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

            # Try to go to next page
            try:
                current_active_btn = await page.query_selector("div.paging span.on, div.paging a.on")
                if not current_active_btn:
                    break
                current_active_text = await current_active_btn.inner_text()
                current_page_val = int(current_active_text.strip())

                next_page_btn = await page.query_selector(f"div.paging a:has-text('{current_page_val + 1}')")
                
                if not next_page_btn:
                    next_selectors = [
                        "a[id$='btnNext']",
                        "a:has(img[alt='다음'])",
                        "a:has-text('다음')",
                        "a.next"
                    ]
                    for sel in next_selectors:
                        btn = await page.query_selector(f"div.paging {sel}")
                        if btn and await btn.is_visible():
                            next_page_btn = btn
                            break

                if next_page_btn:
                    await self._wait()
                    await next_page_btn.click()
                    await page.wait_for_load_state("load", timeout=10000)
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
        print(f"🔍 Collecting historical player IDs for {len(seasons_list)} seasons in parallel...", file=sys.stderr)

        semaphore = asyncio.Semaphore(10)  # Allow 10 concurrent years

        async def fetch_year(season):
            async with semaphore:
                print(f"  Fetching IDs for {season}...", file=sys.stderr)
                try:
                    return await self.collect_player_ids_for_year(season)
                except Exception as e:
                    print(f"  ❌ Error fetching IDs for {season}: {e}", file=sys.stderr)
                    return set()

        results = await asyncio.gather(*(fetch_year(s) for s in seasons_list))
        for season_ids in results:
            historical_ids |= season_ids

        print(f"✨ Total unique IDs found: {len(historical_ids)}", file=sys.stderr)
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
