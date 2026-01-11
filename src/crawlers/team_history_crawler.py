
import asyncio
import re
from typing import List, Dict, Optional
from playwright.async_api import async_playwright, Page, Locator

from src.models.team_history import TeamHistory
from src.db.engine import SessionLocal
from sqlalchemy import select

class TeamHistoryCrawler:
    """
    Crawls KBO Team History page (https://www.koreabaseball.com/Kbo/League/TeamHistory.aspx)
    Collects: Annual Team Names, Logos, Rankings, Season Info
    """
    
    BASE_URL = "https://www.koreabaseball.com/Kbo/League/TeamHistory.aspx"
    
    def __init__(self):
        self.browser = None
        self.page = None
        self.playwright = None

    async def start(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.page = await self.browser.new_page()

    async def close(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def crawl(self):
        print(f"📜 Crawling Team History from {self.BASE_URL}")
        if not self.page:
            await self.start()
            
        await self.page.goto(self.BASE_URL, wait_until="networkidle")
        
        # Selectors validated by Subagent
        # Table: table.tData.tbd02
        # Row: tbody tr
        # Year: th[scope='row']
        # Cells: td (12 columns)
        
        rows = await self.page.locator("table.tData.tbd02 tbody tr").all()
        print(f"Found {len(rows)} year entries.")
        
        history_data = []
        
        # State tracking: 12 slots for teams (KBO has max 10 active + history slots?)
        # Subagent said 12 columns.
        # We store {name: str, logo: str} for each column index.
        team_slots = [{"name": None, "logo": None} for _ in range(12)]
        
        for row in rows:
            # 1. Get Year
            # The year is in the 'th'.
            year_th = row.locator("th")
            if await year_th.count() == 0: continue
            
            year_text = await year_th.inner_text()
            try:
                year = int(year_text.strip())
            except:
                print(f"⚠️ Skipping invalid year: {year_text}")
                continue
                
            # 2. Iterate Cells
            cells = await row.locator("td").all()
            
            # Subagent said 12 columns. Cells list should be length 12?
            # Or colspan might interfere? KBO history table usually fixed grid.
            
            for i, cell in enumerate(cells):
                if i >= 12: break # Safety
                
                # Check for content
                # Structure:
                # <a> <span class='nums'>Rank</span> <img alt='Name'> </a>
                # OR <a> <span class='nums'>Rank</span> <span>Name</span> </a>
                
                # Parse Rank
                rank_el = cell.locator("span.nums")
                rank = None
                if await rank_el.count() > 0:
                    try:
                        rank = int((await rank_el.inner_text()).strip())
                    except: pass
                
                # Parse Name/Logo (Updates identity if present)
                # Look for img or name span
                img = cell.locator("img")
                name_span = cell.locator("span:not(.nums)")
                
                new_name = None
                new_logo = None
                
                if await img.count() > 0:
                    new_name = await img.get_attribute("alt")
                    new_logo = await img.get_attribute("src")
                elif await name_span.count() > 0:
                    new_name = (await name_span.inner_text()).strip()
                
                # Update State
                if new_name:
                    team_slots[i]["name"] = new_name
                if new_logo:
                    team_slots[i]["logo"] = new_logo
                    
                # Create History Entry if there is a team in this slot (Rank is a good indicator of participation, 
                # but sometimes teams participate without rank? No, KBO always ranks)
                # Or if we have a name in the slot state, it technically existed?
                # Usually only active teams have a cell in the row?
                # Wait, if the cell is empty, the team didn't play?
                # Subagent said "Team Ranks: 2, 1, 7, (Empty), 3..."
                # So if cell content implies participation.
                # If rank is present, definitely participated.
                # If rank is NOT present, did they participate? 
                # In 2024, some columns are empty (Empty in subagent text).
                # So we only record if Rank is found? Or if name is explicitly shown?
                # Actually, "active teams" have ranks.
                
                if rank is not None:
                     # Identify Team Code from Name
                     # We need to map "Samsung Lions" -> "SS"
                     # We can resolve this during SAVE phase or here.
                     # Let's simple store the raw data.
                     
                     current_name = team_slots[i]["name"]
                     current_logo = team_slots[i]["logo"]
                     
                     if current_name:
                         history_data.append({
                             "season": year,
                             "team_name": current_name,
                             "logo_url": current_logo,
                             "ranking": rank,
                             "slot_index": i # Debug info
                         })
            
            print(f"Processed {year}: {len([h for h in history_data if h['season'] == year])} teams.")

        return history_data

    async def save(self, data: List[Dict]):
        print(f"💾 Saving {len(data)} history entries...")
        # For simplicity in this Task, we will just print/dry-run or basic upsert.
        # But Phase 7 requires filling `team_history` table.
        # Logic: 
        # 1. Resolve team_code (SS, OB, etc) from team_name using utils/DB.
        # 2. Resolve franchise_id from team_code.
        # 3. Insert into team_history.
        pass

if __name__ == "__main__":
    crawler = TeamHistoryCrawler()
    asyncio.run(crawler.crawl())
