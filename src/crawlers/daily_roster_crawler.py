"""
Crawler for Daily 1st Team Registration Status.
Source: https://www.koreabaseball.com/Player/Register.aspx
"""
from __future__ import annotations

import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, date

from playwright.async_api import async_playwright, Page

from src.utils.team_codes import resolve_team_code
from src.utils.safe_print import safe_print as print

class DailyRosterCrawler:
    """Crawl daily roster changes."""

    def __init__(self, request_delay: float = 1.0):
        self.base_url = "https://www.koreabaseball.com/Player/Register.aspx"
        self.request_delay = request_delay

    async def crawl_date_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Crawl roster for a range of dates (format: YYYY-MM-DD)."""
        s_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        e_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        results = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Initial load
            await page.goto(self.base_url, wait_until="networkidle")
            
            # Create a date range list
            from datetime import timedelta
            delta = e_date - s_date
            dates = [s_date + timedelta(days=i) for i in range(delta.days + 1)]
            
            for d in dates:
                roster = await self._crawl_date(page, d)
                if roster:
                    results.extend(roster)
                
            await browser.close()
        return results

    async def _crawl_date(self, page: Page, target_date: date) -> List[Dict[str, Any]]:
        date_str = target_date.strftime("%Y%m%d")
        print(f"📅 Crawling Roster for {target_date}...")

        # Evaluate setting hidden field and posting back
        # The page uses update panel, so we wait for network idle or specific element changes
        js_nav = f"""
        () => {{
            document.getElementById('cphContents_cphContents_cphContents_hfSearchDate').value = '{date_str}';
            __doPostBack('ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$btnPreDate',''); 
            // Note: Triggering any date button (Pre/Next) with the updated hidden field usually works 
            // or specific reload function if known. 
            // Let's try simulating the refresh.
        }}
        """
        # A more reliable way is to simulate what DatePicker does or use the search button if exists.
        # But this page auto-refreshes on date change if using UI.
        # Let's try injecting the value and triggering the postback the datepicker uses or just simple reload if query param works?
        # Query params are NOT used here (stateful).
        
        # Strategy:
        # 1. Set hidden input `hfSearchDate`
        # 2. Call `javascript:__doPostBack(...)`
        await page.evaluate(f"document.getElementById('cphContents_cphContents_cphContents_hfSearchDate').value = '{date_str}';")
        
        # We need to trigger the update. The 'Previous' button ID is one way, but might shift date?
        # Let's assume there's a refresh mechanism.
        # Actually, clicking the 'Calendar' and picking date calls postback.
        # Let's use the 'Previous Date' logic but FORCE the date in hidden field?
        # If we click 'PreDate', backend might subtract 1 day from hidden field.
        # Safe bet: Navigate via URL is NOT an option.
        # Creating a specific POST might be hard with Playwright.
        
        # Alternative: We are already on the page. Use JS to trigger the actual change function if exposed?
        # Let's try the safest path: Set Value -> Call `__doPostBack` on a harmless control or just the one used by datepicker.
        # The datepicker usually calls `__doPostBack('...txtSearchDate', '')`.
        triggers = ['ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$btnCalendarSelect']
        
        try:
             async with page.expect_response(lambda response: "Register.aspx" in response.url, timeout=5000) as response_info:
                 await page.evaluate(f"__doPostBack('{triggers[0]}', '')")
        except Exception:
             # Timeout means maybe no request or fast.
             pass
        await page.wait_for_timeout(2000) # Wait for UI update
        
        # DEBUG: Check visible date
        visible_date = await page.evaluate("document.querySelector('.date') ? document.querySelector('.date').innerText : 'No Date Element'")
        print(f"   [DEBUG] Visible Page Date: {visible_date}")
        
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
                await page.wait_for_timeout(300) # simple wait for update panel
                
                # Extract data
                records = await self._extract_table(page, t_code, target_date)
                daily_records.extend(records)
            except Exception as e:
                print(f"⚠️ Error crawling team {t_code}: {e}")
                
        return daily_records

    async def _extract_table(self, page: Page, team_code: str, roster_date: date) -> List[Dict[str, Any]]:
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
                const headers = table.querySelectorAll('th');
                if (headers.length >= 2) {
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
                    
                    if (playerId) {
                        results.push({
                            'player_id': playerId,
                            'player_name': name,
                            'back_number': backNum,
                            'category': category
                        });
                    }
                });
            });
            return results;
        }
        """
        
        data = await page.evaluate(script)
        if data and data[0].get('status') == 'no_tables':
            print(f"   [DEBUG] No tables found for team {team_code}")
            return []

        
        # Post-process
        cleaned = []
        for item in data:
            cleaned.append({
                "roster_date": roster_date,
                "team_code": self._normalize_team(team_code),
                "player_id": int(item['player_id']),
                "player_name": item['player_name'],
                "position": self._clean_category(item['category']),
                "back_number": item['back_number']
            })
        return cleaned

    def _normalize_team(self, code: str) -> str:
        # Map website code to our DB code
        # 'OB' -> we used 'OB' for Doosan? Or 'DO'?
        # In our DB we have 'OB' as historical? Check `src/utils/team_codes.py`
        # The site uses 'OB', 'SK', etc. 
        # Our `team_codes.py` should handle this or we store as is.
        # Usually 'OB' -> 'OB' (Doosan Base). 
        # But wait, 'SK' is 'SSG' now?
        # Site uses 'SK' for SSG (Landers).
        # We should map to canonical if we want consistency with `teams` table.
        resolved = resolve_team_code(code)
        return resolved if resolved else code

    def _clean_category(self, cat: str) -> str:
        # "투수 (14명)" -> "투수"
        if "(" in cat:
            return cat.split("(")[0].strip()
        return cat

async def main():
    crawler = DailyRosterCrawler()
    # Test for yesterday
    yesterday = (datetime.now().date()).strftime("%Y-%m-%d")
    data = await crawler.crawl_date_range("2024-05-20", "2024-05-20")
    print(f"Crawled {len(data)} records.")
    for r in data[:5]:
        print(r)

if __name__ == "__main__":
    asyncio.run(main())
