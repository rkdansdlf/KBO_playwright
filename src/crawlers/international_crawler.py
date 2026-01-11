
import re
import asyncio
from datetime import datetime
from typing import List, Dict, Optional
from playwright.async_api import async_playwright, Page, Locator

from src.utils.team_codes import team_code_from_game_id_segment, resolve_team_code
from src.models.game import Game

class InternationalScheduleCrawler:
    """
    Crawler for KBO International Games (e.g., Premier 12, WBC).
    Targets static HTML tables in /Schedule/International/ sections.
    """
    
    def __init__(self):
        self.browser = None
        self.page = None
        self.playwright = None

    async def start_browser(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.page = await self.browser.new_page()

    async def close(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def crawl_schedule(self, url: str) -> List[Dict]:
        """
        Crawls the international schedule from the given URL.
        """
        print(f"🌍 Crawling International Schedule: {url}")
        if not self.page:
            await self.start_browser()
            
        await self.page.goto(url, wait_until="networkidle", timeout=30000)
        
        # Determine year from URL or page context
        year_match = re.search(r"(\d{4})", url)
        year = int(year_match.group(1)) if year_match else datetime.now().year
        
        games = []
        
        # Select rows in the main schedule table
        # Debug revealed class is 'tData' (e.g. 'tData new three tbl-ag2018')
        rows = await self.page.locator("table.tData tbody tr").all()
        
        for row in rows:
            game_data = await self._parse_row(row, year)
            if game_data:
                games.append(game_data)
                
        print(f"✅ Found {len(games)} international games.")
        return games

    async def _parse_row(self, row: Locator, year: int) -> Optional[Dict]:
        """Parses a single row from the international schedule table."""
        
        cols = await row.locator("td").all()
        if len(cols) < 3:
            return None
            
        # 1. Date & Time
        date_str = await cols[0].inner_text() # "11.13(수)"
        time_str = await cols[1].inner_text() # "19:30"
        
        # Clean date (remove parens and everything inside)
        date_clean = re.sub(r'\(.*\)', '', date_str).strip() # "11.13"
        try:
            month, day = map(int, date_clean.split('.'))
            game_date = datetime(year, month, day).date()
            if time_str:
                game_time = time_str.strip()
            else:
                game_time = "00:00"
        except ValueError:
            # Often first row is header or empty or special
            return None

        # 2. Teams
        match_cell = cols[2]
        
        # Check if match_cell has team structure
        if await match_cell.locator(".team.away .name").count() == 0:
            return None
            
        away_name = await match_cell.locator(".team.away .name").inner_text()
        home_name = await match_cell.locator(".team.home .name").inner_text()
        
        # Resolve Codes using our new national map
        away_code = resolve_team_code(away_name)
        home_code = resolve_team_code(home_name)
        
        if not away_code or not home_code:
            print(f"⚠️  Unknown team code for: {away_name} vs {home_name}")
            return None

        # 3. Score (if played)
        score_text = await match_cell.locator(".score").inner_text()
        status = "Scheduled"
        away_score = 0
        home_score = 0
        
        if "vs" in score_text: 
            scores = re.findall(r'\d+', score_text)
            if len(scores) == 2:
                away_score = int(scores[0])
                home_score = int(scores[1])
                status = "End" 
        
        # 4. Venue
        venue = "International"
        if len(cols) > 3:
            venue = await cols[3].inner_text()
            
        # 5. Synthesize Game ID
        # Format: YYYYMMDD + Home + Away + 0
        game_id = f"{year}{month:02d}{day:02d}{away_code}{home_code}0" 
        # Wait, standard KBO ID is YYYYMMDD + Away + Home + DH?
        # KBO Example: 20240323HHLG0 -> HH (Away) LG (Home).
        # My previous thought was HomeAway? Let's check schedule_crawler logic.
        # href: gameId=20240323HHLG0
        # schedule_crawler: away_segment = game_id[8:10], home_segment = game_id[10:12]
        # So YES: YYYYMMDD + AWAY + HOME + DH
        
        # Construct Game Object (Dict for now)
        return {
            "game_id": game_id,
            "season_id": int(f"{year}90"), # 202490 (International)
            "game_date": game_date,
            "game_time": game_time,
            "home_team": home_code,
            "away_team": away_code,
            "stadium": venue,
            "status": status,
            "away_score": away_score,
            "home_score": home_score,
            "dh": 0,
            "series_id": 90, # Custom International ID
        }

