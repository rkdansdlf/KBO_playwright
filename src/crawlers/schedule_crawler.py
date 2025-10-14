"""
KBO Schedule Crawler POC
Collects game IDs from the KBO schedule page
"""
import asyncio
import time
from datetime import datetime
from typing import List, Dict, Optional
from playwright.async_api import async_playwright, Page

from src.utils.team_codes import team_code_from_game_id_segment


class ScheduleCrawler:
    """Crawls KBO game schedule to extract game IDs"""

    def __init__(self, request_delay: float = 1.5):
        self.base_url = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
        self.request_delay = request_delay

    async def crawl_schedule(self, year: int, month: int) -> List[Dict]:
        """
        Crawl schedule for a specific year and month

        Args:
            year: Season year (e.g., 2025)
            month: Month (1-12)

        Returns:
            List of game dictionaries with game_id, date, teams
        """
        print(f"🔍 Crawling schedule for {year}-{month:02d}...")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                games = await self._crawl_month(page, year, month)
                print(f"✅ Found {len(games)} games")
                return games
            except Exception as e:
                print(f"❌ Error crawling schedule: {e}")
                return []
            finally:
                await browser.close()

    async def crawl_season(self, year: int, months: Optional[List[int]] = None) -> List[Dict]:
        """
        Crawl schedule across multiple months for a given season.

        Args:
            year: Season year
            months: Optional list of months (defaults to March-October)
        """
        months = months or list(range(3, 11))
        all_games: List[Dict] = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            try:
                for month in months:
                    month_games = await self._crawl_month(page, year, month)
                    all_games.extend(month_games)
                return all_games
            finally:
                await browser.close()

    async def _crawl_month(self, page: Page, year: int, month: int) -> List[Dict]:
        url = f"{self.base_url}?year={year}&month={month}&seriesId=0"
        print(f"📡 Fetching: {url}")

        await page.goto(url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(self.request_delay)

        return await self._extract_games(page, year, month)

    async def _extract_games(self, page: Page, year: int, month: int) -> List[Dict]:
        """Extract game information from the schedule page"""
        games = []

        # Find all game links with gameId parameter
        game_links = await page.query_selector_all('a[href*="gameId="]')

        for link in game_links:
            try:
                href = await link.get_attribute('href')
                if not href or 'gameId=' not in href:
                    continue

                # Extract game_id from URL
                game_id = self._extract_game_id(href)
                if not game_id:
                    continue

                # Extract date from game_id (format: YYYYMMDD...)
                game_date = game_id[:8]

                away_segment = game_id[8:10] if len(game_id) >= 10 else None
                home_segment = game_id[10:12] if len(game_id) >= 12 else None

                games.append({
                    'game_id': game_id,
                    'game_date': game_date,
                    'season_year': year,
                    'season_type': 'regular',
                    'away_team_code': team_code_from_game_id_segment(away_segment),
                    'home_team_code': team_code_from_game_id_segment(home_segment),
                    'doubleheader_no': int(game_id[-1]) if game_id[-1].isdigit() else 0,
                    'game_status': 'scheduled',
                    'crawl_status': 'pending',
                    'url': f"https://www.koreabaseball.com{href}" if href.startswith('/') else href
                })

            except Exception as e:
                print(f"⚠️  Error extracting game: {e}")
                continue

        # Remove duplicates based on game_id
        unique_games = {g['game_id']: g for g in games}
        return list(unique_games.values())

    def _extract_game_id(self, href: str) -> str:
        """Extract game_id from URL"""
        try:
            if 'gameId=' in href:
                game_id = href.split('gameId=')[1].split('&')[0]
                return game_id
        except:
            pass
        return ""


async def main():
    """Test the schedule crawler"""
    crawler = ScheduleCrawler()

    # Crawl current month schedule
    now = datetime.now()
    games = await crawler.crawl_schedule(now.year, now.month)

    print(f"\n📊 Schedule Summary:")
    print(f"Total games found: {len(games)}")

    if games:
        print(f"\n📝 First 5 games:")
        for game in games[:5]:
            print(f"  - {game['game_id']} | {game['game_date']}")


if __name__ == "__main__":
    asyncio.run(main())
