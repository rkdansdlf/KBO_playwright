"""
Player Daily Stats Crawler (Game-by-Game)
Fetches transactional (per game) statistics for a specific player and season.
This is used to backfill missing or corrupted data in the game_stats tables.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import async_playwright

from src.utils.type_helpers import parse_innings_to_outs

logger = logging.getLogger(__name__)

PLAYER_DAILY_CRAWL_EXCEPTIONS = (
    PlaywrightError,
    TimeoutError,
    asyncio.TimeoutError,
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
)
PLAYER_DAILY_PARSE_EXCEPTIONS = (ValueError, TypeError, IndexError)


class PlayerDailyStatsCrawler:
    def __init__(self, *, headless: bool = True) -> None:
        self.headless = headless
        self.base_url = "https://www.koreabaseball.com/Record/Player/{type}Detail/Daily.aspx?playerId={pid}"

    async def crawl_player_season(self, player_id: int, *, is_pitcher: bool, season: int) -> list[dict[str, Any]]:
        p_type = "Pitcher" if is_pitcher else "Hitter"
        url = self.base_url.format(type=p_type, pid=player_id)

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            page = await browser.new_page()

            try:
                logger.info("📡 Navigating to %s...", url)
                await page.goto(url, wait_until="networkidle")

                # 1. Select Year
                year_selector = "#cphContents_cphContents_cphContents_ddlYear"
                try:
                    await page.select_option(year_selector, value=str(season))
                    await page.wait_for_load_state("networkidle")
                    await asyncio.sleep(1.5)  # Wait for AJAX/Postback
                except PLAYER_DAILY_CRAWL_EXCEPTIONS:
                    logger.exception("   ❌ Failed to select year %s", season)
                    return []

                # 2. Parse All Tables
                rows = await page.evaluate("""() => {
                    const tables = Array.from(document.querySelectorAll('.tbl.tt, .tEx'));
                    const results = [];

                    tables.forEach(table => {
                        const trs = Array.from(table.querySelectorAll('tbody tr'));
                        trs.forEach(tr => {
                            const cells = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim());
                            if (cells.length > 5) {
                                results.push(cells);
                            }
                        });
                    });
                    return results;
                }""")
            except PLAYER_DAILY_CRAWL_EXCEPTIONS:
                logger.exception("   ❌ Error crawling player %s", player_id)
                return []
            else:
                logger.info("   📊 Found %s raw data rows on page.", len(rows))
                all_games = []
                for row in rows:
                    if is_pitcher:
                        data = self._parse_pitcher_row(row, season)
                    else:
                        data = self._parse_hitter_row(row, season)
                    if data:
                        all_games.append(data)
                return all_games
            finally:
                await browser.close()

    def _parse_hitter_row(self, row: list[str], season: int) -> dict[str, Any] | None:
        # [0:Date, 1:Opp, 2:AVG1, 3:PA, 4:AB, 5:R, 6:H, 7:2B, 8:3B, 9:HR, 10:RBI, 11:SB, 12:CS, 13:BB, 14:HBP, 15:SO, 16:GDP, 17:AVG2]
        if len(row) < 17:
            return None
        # ... rest of method unchanged ...
        try:
            date_str = f"{season}-{row[0].replace('.', '-')}"
            return {
                "game_date": date_str,
                "opponent": row[1],
                "stats": {
                    "plate_appearances": int(row[3]),
                    "at_bats": int(row[4]),
                    "runs": int(row[5]),
                    "hits": int(row[6]),
                    "doubles": int(row[7]),
                    "triples": int(row[8]),
                    "home_runs": int(row[9]),
                    "rbi": int(row[10]),
                    "stolen_bases": int(row[11]),
                    "caught_stealing": int(row[12]),
                    "walks": int(row[13]),
                    "hbp": int(row[14]),
                    "strikeouts": int(row[15]),
                    "gdp": int(row[16]),
                },
            }
        except PLAYER_DAILY_PARSE_EXCEPTIONS:
            logger.exception("Failed to parse batter row")
            return None

    def _parse_pitcher_row(self, row: list[str], season: int) -> dict[str, Any] | None:
        # [0:Date, 1:Opp, 2:Type, 3:Res, 4:ERA1, 5:TBF, 6:IP, 7:H, 8:HR, 9:BB, 10:HBP, 11:SO, 12:R, 13:ER, 14:ERA2]
        if len(row) < 14:
            return None
        try:
            date_str = f"{season}-{row[0].replace('.', '-')}"

            # Map Decision
            decision = None
            res = row[3]
            if "승" in res:
                decision = "W"
            elif "패" in res:
                decision = "L"
            elif "세" in res:
                decision = "S"
            elif "홀" in res:
                decision = "H"

            # Parse Innings (e.g. "5 2/3" -> outs)
            ip_str = row[6]
            innings_outs = parse_innings_to_outs(ip_str) or 0

            return {
                "game_date": date_str,
                "opponent": row[1],
                "stats": {
                    "decision": decision,
                    "wins": 1 if decision == "W" else 0,
                    "losses": 1 if decision == "L" else 0,
                    "saves": 1 if decision == "S" else 0,
                    "batters_faced": int(row[5]),
                    "innings_outs": innings_outs,
                    "hits_allowed": int(row[7]),
                    "home_runs_allowed": int(row[8]),
                    "walks_allowed": int(row[9]),
                    "hbp_allowed": int(row[10]),
                    "strikeouts": int(row[11]),
                    "runs_allowed": int(row[12]),
                    "earned_runs": int(row[13]),
                },
            }
        except PLAYER_DAILY_PARSE_EXCEPTIONS:
            logger.exception("Failed to parse pitcher row")
            return None


if __name__ == "__main__":

    async def test() -> None:
        crawler = PlayerDailyStatsCrawler()
        # Jose Fernandez 2020
        data = await crawler.crawl_player_season(69209, is_pitcher=False, season=2020)
        logger.info("Collected %s games for hitter.", len(data))
        if data:
            logger.info(data[0])

        # Pinto 2020
        data_p = await crawler.crawl_player_season(50815, is_pitcher=True, season=2020)
        logger.info("Collected %s games for pitcher.", len(data_p))
        if data_p:
            logger.info(data_p[0])

    asyncio.run(test())
