
"""
Crawler for Player Movement (Trade, FA, Waiver, etc.).
Source: https://www.koreabaseball.com/Player/Trade.aspx
"""
from __future__ import annotations

import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, date

from playwright.async_api import async_playwright, Page, TimeoutError

from src.utils.safe_print import safe_print as print

class PlayerMovementCrawler:
    """Crawl player status changes (Trade, FA, Waiver, etc.)."""

    def __init__(self, request_delay: float = 1.0):
        self.base_url = "https://www.koreabaseball.com/Player/Trade.aspx"
        self.request_delay = request_delay

    async def crawl_years(self, start_year: int, end_year: int) -> List[Dict[str, Any]]:
        """Crawl data for a range of years."""
        results = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Initial load
            await page.goto(self.base_url, wait_until="networkidle")
            
            for year in range(start_year, end_year + 1):
                year_data = await self._crawl_year(page, year)
                results.extend(year_data)
                
            await browser.close()
        return results

    async def _crawl_year(self, page: Page, year: int) -> List[Dict[str, Any]]:
        print(f"🔄 Crawling Player Movements for Year: {year}...")
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
                print("⚠️ Search click timeout - Page might have updated without reload or network is slow.")
            
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(1000)
            
            # 4. Iterate Pagination
            page_num = 1
            prev_page_data_str = ""
            
            while True:
                print(f"   PAGE {page_num}: Extracting...")
                
                # Extract current page rows
                data = await self._extract_table(page)
                if not data:
                    print("   ⚠️ No data found on this page.")
                
                # Check for duplicates (Stop infinite loop)
                current_data_str = str(data)
                if current_data_str == prev_page_data_str:
                    print(f"   🛑 Duplicate data detected (Same as Page {page_num-1}). Stopping.")
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
                    # Try "Next" arrow ( > ) or "Next 10" arrow ( >> )
                    # Note: 'pg_next' is typically single page advance. 'pg_last' is next block.
                    # If we are at end of block (e.g. Page 10), explicit '11' is NOT visible.
                    # We MUST click arrow. which arrow?
                    # The subagent said `pg_next` is for next page. `pg_last` is for next 10.
                    # Let's try `pg_next` first.
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
                    print(f"   ✅ Finished Year {year}. No more next pages.")
                    break
                
                # Wait for table update
                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(800)
                
                # Verify we actually moved?
                # Optional: Check `.paging a.on` text.
                # But duplicates check covers most cases.
                
                page_num += 1
                
        except Exception as e:
            print(f"⚠️ Error processing year {year}: {e}")
            import traceback
            traceback.print_exc()

        print(f"✅ Year {year}: Collected {len(results)} records.")
        return results

    async def _extract_table(self, page: Page) -> List[Dict[str, Any]]:
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
        valid_data = []
        for item in data:
            if item['date'] and item['section']:
                valid_data.append(item)
                
        return valid_data

async def main():
    # Test run
    crawler = PlayerMovementCrawler()
    data = await crawler.crawl_years(2023, 2023)
    print(f"Total collected: {len(data)}")
    for d in data[:5]:
        print(d)

if __name__ == "__main__":
    asyncio.run(main())
