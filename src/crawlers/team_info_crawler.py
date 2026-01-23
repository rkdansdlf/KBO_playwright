
import asyncio
from typing import List, Dict, Optional
from playwright.async_api import async_playwright, Page, Locator

from src.utils.playwright_blocking import install_async_resource_blocking

from src.models.franchise import Franchise
from src.db.engine import SessionLocal
from sqlalchemy import select, update

class TeamInfoCrawler:
    """
    Crawls KBO Team Info page (https://www.koreabaseball.com/Kbo/League/TeamInfo.aspx)
    Collects: CEO, Owner, Founded Date, Homepage, Phone, Address
    """
    
    BASE_URL = "https://www.koreabaseball.com/Kbo/League/TeamInfo.aspx"
    
    def __init__(self):
        self.browser = None
        self.page = None
        self.playwright = None
        self.context = None

    async def start(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.context = await self.browser.new_context()
        await install_async_resource_blocking(self.context)
        self.page = await self.context.new_page()

    async def close(self):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def crawl(self):
        print(f"🏢 Crawling Team Info from {self.BASE_URL}")
        if not self.page:
            await self.start()
            
        await self.page.goto(self.BASE_URL, wait_until="networkidle")
        
        # 1. Get List of Teams using validated selector
        rows = await self.page.locator("table.tData tbody tr").all()
        print(f"Found {len(rows)} team entries.")
        
        teams_data = []

        for i in range(len(rows)):
            # Re-query rows to avoid stale element errors if modal navigation causes DOM updates (though usually it doesn't for modals)
            # But KBO site might be tricky.
            row = self.page.locator("table.tData tbody tr").nth(i)
            cols = await row.locator("td").all()
            if len(cols) < 4: continue
            
            # Extract basic info
            team_name_full = await cols[0].inner_text() 
            found_year_text = await cols[1].inner_text()
            hometown = await cols[2].inner_text()
            
            # Clean name (remove extra spaces)
            team_name = team_name_full.strip()
            print(f"Processing {team_name}...")

            # 2. Open Modal for Details
            # Use strict selector for detailed view link (a.showTg)
            link = row.locator("td").nth(0).locator("a.showTg").first # Use first if multiple (though showTg usually unique per row)
            if await link.count() > 0:
                await link.click()
                
                # Wait for modal
                # Selector: div[id^='layerPop']
                # We need to wait for one that is VISIBLE.
                modal = self.page.locator("div[id^='layerPop']:visible")
                try:
                    await modal.wait_for(state="visible", timeout=3000)
                    
                    # Extract Modal Data using XPath/Text matching as validated
                    # Helper to get text from 'th' neighbor
                    async def get_modal_field(label: str):
                        # Locate th with text -> following sibling td
                        # Use XPath for EXACT text content matching
                        xpath = f".//th[normalize-space(text())='{label}']/following-sibling::td[1]"
                        el = modal.locator(f"xpath={xpath}")
                        if await el.count() > 0:
                            return (await el.inner_text()).strip()
                        return None
                    
                    owner = await get_modal_field("구단주")
                    ceo = await get_modal_field("대표이사")
                    address = await get_modal_field("구단사무실")
                    phone = await get_modal_field("대표전화")
                    homepage = await get_modal_field("홈페이지")
                    
                    # Close modal
                    # Try Escape key first
                    await self.page.keyboard.press("Escape")
                    
                    # Fallback to click if still visible
                    try:
                        if await modal.is_visible(timeout=1000):
                            close_btn = self.page.locator("a.btn_close, img[alt='닫기']").first
                            if await close_btn.count() > 0:
                                await close_btn.click()
                    except: pass
                    
                    await self.page.locator("div[id^='layerPop']").wait_for(state="hidden", timeout=3000)
                    
                except Exception as e:
                    print(f"⚠️ Failed to parse modal for {team_name}: {e}")
                    # Force close if stuck
                    await self.page.keyboard.press("Escape")
                    owner, ceo, address, phone, homepage = None, None, None, None, None
            else:
                print(f"⚠️ No link found for {team_name}")
                owner, ceo, address, phone, homepage = None, None, None, None, None

            info = {
                "name": team_name,
                "found_year": found_year_text,
                "city": hometown,
                "owner": owner,
                "ceo": ceo,
                "address": address,
                "phone": phone,
                "homepage": homepage
            }
            teams_data.append(info)
            await asyncio.sleep(0.5) # Politeness

        return teams_data

    async def save(self, data: List[Dict]):
        print(f"💾 Saving {len(data)} team profiles...")
        with SessionLocal() as session:
            for item in data:
                # Update Franchise metadata
                # Logic: Find Franchise by name? 
                # We have 'Team Name'. e.g. "Samsung Lions".
                # Map to 'SS'.
                # Current 'team_franchises' has 'name' column.
                # Use strict matching first?
                
                # Check DB for name match
                stmt = select(Franchise).where(Franchise.name.like(f"%{item['name']}%"))
                result = session.execute(stmt).scalars().first()
                if result:
                   meta = result.metadata_json or {}
                   meta.update({
                       "found_year": item["found_year"],
                       "owner": item["owner"],
                       "ceo": item["ceo"],
                       "address": item["address"],
                       "phone": item["phone"]
                   })
                   result.metadata_json = meta
                   result.web_url = item["homepage"]
                   session.add(result)
                   print(f"✅ Updated {result.name}")
                else:
                    print(f"⚠️ Could not find franchise for {item['name']}")
            session.commit()

if __name__ == "__main__":
    crawler = TeamInfoCrawler()
    asyncio.run(crawler.crawl())
