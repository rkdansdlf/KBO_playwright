"""
Determine retired/inactive player IDs by comparing historical rosters with current active rosters.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Iterable

from src.utils.compliance import compliance
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.throttle import throttle

logger = logging.getLogger(__name__)
class RetiredPlayerListingCrawler:
    """
    Fetch player ID sets for historical seasons and compute inactive (retired) candidates.
    """

    def __init__(self, request_delay: float = 1.5, pool: AsyncPlaywrightPool | None = None):
        self.request_delay = request_delay
        self.pool = pool
        self.hitter_url = "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx"
        self.pitcher_url = "https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic1.aspx"

    async def _wait(self) -> None:
        await throttle.wait()
        if self.request_delay > throttle.default_delay:
            await asyncio.sleep(self.request_delay - throttle.default_delay)

    async def collect_player_ids_for_year(self, season_year: int) -> dict[str, str]:
        """Collect all player IDs and names (hitters + pitchers) for a given season."""
        pool = self.pool or AsyncPlaywrightPool(max_pages=1)
        owns_pool = self.pool is None
        await pool.start()
        try:
            page = await pool.acquire()
            try:
                hitter_map = await self._crawl_record_page_ids_with_teams(page, self.hitter_url, season_year)
                pitcher_map = await self._crawl_record_page_ids_with_teams(page, self.pitcher_url, season_year)
                hitter_map.update(pitcher_map)
                return hitter_map
            finally:
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()

    async def _crawl_record_page_ids(self, page, base_url: str, year: int) -> dict[str, str]:
        """Navigate to one record page and collect IDs across its pagination.

        Kept as the stable no-team-filter path used by compatibility tests and
        smaller diagnostics. `collect_player_ids_for_year` uses the broader
        team-aware path for production collection.
        """
        if not await compliance.is_allowed(base_url):
            logger.info(f"[COMPLIANCE] Blocked record listing: {base_url}")
            return {}

        await self._wait()
        await page.goto(base_url, wait_until="load", timeout=30000)

        season_selector = 'select[id$="ddlSeason_ddlSeason"], select[name*="ddlSeason"]'
        series_selector = 'select[id$="ddlSeries_ddlSeries"], select[name*="ddlSeries"]'
        await page.wait_for_selector(season_selector, timeout=15000)
        await self._select_option_and_dispatch(page, season_selector, str(year))
        with contextlib.suppress(Exception):
            await page.wait_for_load_state("load", timeout=10000)
        await page.wait_for_timeout(1000)

        try:
            await self._select_option_and_dispatch(page, series_selector, "0")
            await page.wait_for_load_state("load", timeout=10000)
            await page.wait_for_timeout(500)
        except Exception:  # noqa: BLE001
            logger.warning("Failed to select all series option, continuing")
            pass

        return await self._collect_ids_from_pages(page, year)

    async def _select_option_and_dispatch(self, page, selector: str, value: str) -> None:
        await page.select_option(selector, value)
        await page.evaluate(
            """
            selector => {
                const el = document.querySelector(selector);
                if (!el) return false;
                if (el.onchange) {
                    el.onchange();
                } else {
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }
                return true;
            }
            """,
            selector,
        )

    async def _crawl_record_page_ids_with_teams(self, page, base_url: str, year: int) -> dict[str, str]:
        """Navigate to record page, select year, and iterate through all teams to collect IDs and names."""
        if not await compliance.is_allowed(base_url):
            logger.info(f"[COMPLIANCE] Blocked record listing: {base_url}")
            return {}

        await self._wait()
        await page.goto(base_url, wait_until="load", timeout=30000)

        # Select Year
        season_selector = 'select[id$="ddlSeason_ddlSeason"]'
        await page.wait_for_selector(season_selector, timeout=15000)
        await page.select_option(season_selector, str(year))
        await page.evaluate(
            "el => { if (el.onchange) el.onchange(); else el.dispatchEvent(new Event('change', { bubbles: true })); }",
            await page.query_selector(season_selector),
        )
        with contextlib.suppress(Exception):
            await page.wait_for_load_state("load", timeout=10000)
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

        all_players: dict[str, str] = {}
        for code in team_codes:
            logger.info(f"    [Year {year}] Fetching team {code}")
            await page.select_option(team_selector, code)
            await page.evaluate(
                "el => { if (el.onchange) el.onchange(); else el.dispatchEvent(new Event('change', { bubbles: true })); }",
                await page.query_selector(team_selector),
            )
            with contextlib.suppress(Exception):
                await page.wait_for_load_state("load", timeout=10000)
            await page.wait_for_timeout(500)

            team_players = await self._collect_ids_from_pages(page, year)
            all_players.update(team_players)

        return all_players

    async def _collect_ids_from_pages(self, page, year: int) -> dict[str, str]:
        players: dict[str, str] = {}
        page_num = 1
        while True:
            # Extract IDs and Names from current page
            page_players = await page.evaluate(r"""
                () => {
                    const links = document.querySelectorAll('table.tData01.tt tbody tr td:nth-child(2) a');
                    const result = {};
                    links.forEach(a => {
                        const name = (a.innerText || '').trim();
                        const href = a.getAttribute('href');
                        const m = href ? href.match(/playerId=(\d+)/) : null;
                        if (m && name) {
                            result[m[1]] = name;
                        }
                    });
                    return result;
                }
            """)
            players.update(page_players)

            # Try to go to next page
            try:
                current_active_btn = await page.query_selector("div.paging span.on, div.paging a.on")
                if not current_active_btn:
                    break
                current_active_text = await current_active_btn.inner_text()
                current_page_val = int(current_active_text.strip())

                next_page_btn = await page.query_selector(f"div.paging a:has-text('{current_page_val + 1}')")

                if not next_page_btn:
                    next_selectors = ["a[id$='btnNext']", "a:has(img[alt='다음'])", "a:has-text('다음')", "a.next"]
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
            except Exception:  # noqa: BLE001
                logger.warning("Error during pagination, stopping")
                break
        return players

    async def collect_historical_player_ids(self, seasons: Iterable[int]) -> dict[str, str]:
        historical_players: dict[str, str] = {}
        seasons_list = list(seasons)
        logger.info(f"🔍 Collecting historical player IDs for {len(seasons_list)} seasons in parallel...")

        semaphore = asyncio.Semaphore(10)  # Allow 10 concurrent years

        async def fetch_year(season):
            async with semaphore:
                logger.info(f"  Fetching IDs for {season}...")
                try:
                    return await self.collect_player_ids_for_year(season)
                except Exception:
                    logger.exception(f"  ❌ Error fetching IDs for {season}")
                    return {}

        results = await asyncio.gather(*(fetch_year(s) for s in seasons_list))
        for season_players in results:
            historical_players.update(season_players)

        logger.info(f"✨ Total unique IDs found: {len(historical_players)}")
        return historical_players

    async def determine_inactive_player_ids(
        self,
        start_year: int,
        end_year: int,
        active_year: int,
    ) -> set[str]:
        """
        Determine inactive player IDs by diffing historical seasons with active roster.
        Returns ONLY the set of IDs for backward compatibility.
        """
        if start_year > end_year:
            raise ValueError("start_year must be <= end_year")

        seasons = range(start_year, end_year + 1)
        historical_players = await self.collect_historical_player_ids(seasons)
        logger.info(f"📡 Fetching active player IDs for {active_year}...")
        active_players = await self.collect_player_ids_for_year(active_year)

        historical_ids = set(historical_players.keys())
        active_ids = set(active_players.keys())

        inactive = {pid for pid in historical_ids if pid and pid not in active_ids}
        logger.info(f"✨ Found {len(inactive)} inactive players out of {len(historical_ids)} total unique IDs.")
        return inactive

    def _extract_ids(self, data: dict[str, list[dict]]) -> set[str]:
        ids: set[str] = set()
        for key in ("hitters", "pitchers"):
            for player in data.get(key, []):
                player_id = player.get("player_id")
                if player_id:
                    ids.add(player_id)
        return ids


async def main():
    crawler = RetiredPlayerListingCrawler(request_delay=1.0)
    inactive_ids = await crawler.determine_inactive_player_ids(start_year=1982, end_year=2023, active_year=2024)
    logger.info(f"Inactive player IDs discovered: {len(inactive_ids)}")


if __name__ == "__main__":
    asyncio.run(main())
