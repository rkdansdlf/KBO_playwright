"""
Crawler for KBO Awards (MVP, Golden Glove, Defense, Series).
Source: https://www.koreabaseball.com/Player/Awards/PlayerPrize.aspx
"""
import asyncio
import argparse
from typing import List, Dict, Any
from playwright.async_api import async_playwright, Page

from src.db.engine import SessionLocal
from src.repositories.award_repository import AwardRepository
from src.utils.safe_print import safe_print as print
from src.utils.playwright_blocking import install_async_resource_blocking

class AwardCrawler:
    def __init__(self):
        self.base_url_map = {
            "player_prize": "https://www.koreabaseball.com/Player/Awards/PlayerPrize.aspx",
            "golden_glove": "https://www.koreabaseball.com/Player/Awards/GoldenGlove.aspx",
            "defense_prize": "https://www.koreabaseball.com/Player/Awards/DefensePrize.aspx",
            "series_prize": "https://www.koreabaseball.com/Player/Awards/SeriesPrize.aspx",
        }

    async def run(self, award_types: List[str] = None, save: bool = False):
        if not award_types or "all" in award_types:
            award_types = list(self.base_url_map.keys())

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            await install_async_resource_blocking(context)
            page = await context.new_page()

            all_data = []
            for atype in award_types:
                url = self.base_url_map.get(atype)
                if not url:
                    print(f"Unknown award type: {atype}")
                    continue
                
                print(f"Crawling {atype} from {url}...")
                await page.goto(url, wait_until="networkidle")
                
                try:
                    if atype == "player_prize":
                        data = await self.crawl_player_prize(page)
                    elif atype == "golden_glove":
                        data = await self.crawl_golden_glove(page)
                    elif atype == "defense_prize":
                        data = await self.crawl_defense_prize(page)
                    elif atype == "series_prize":
                        data = await self.crawl_series_prize(page)
                    else:
                        data = []
                except Exception as e:
                    print(f"Error crawling {atype}: {e}")
                    import traceback
                    traceback.print_exc()
                    data = []
                
                all_data.extend(data)
                print(f"  > Found {len(data)} records for {atype}")

            await browser.close()
            
            if save:
                self.save_to_db(all_data)
            else:
                # Dry run print
                for d in all_data[:5]:
                    print(d)
                print(f"... and {len(all_data)-5} more.")

    async def crawl_player_prize(self, page: Page) -> List[Dict]:
        """
        Parses MVP and Rookie of the Year table.
        Cols: Year | MVP Cell | Rookie Cell
        MVP Cell contains spans: Name, Team, Position
        """
        script = """
        () => {
            const rows = document.querySelectorAll('table tbody tr');
            const results = [];
            rows.forEach(tr => {
                const cells = tr.querySelectorAll('td');
                // Check if first cell is Year
                let year = parseInt(cells[0].innerText.trim());
                if (isNaN(year)) {
                    // Try th if exists
                    const th = tr.querySelector('th');
                    if (th) year = parseInt(th.innerText.trim());
                }
                if (isNaN(year)) return;

                // MVP (index 1)
                if (cells.length > 1) {
                    const mvpSpans = cells[1].querySelectorAll('span');
                    if (mvpSpans.length >= 2) {
                        results.push({
                            year: year,
                            award_type: 'MVP',
                            category: null,
                            player_name: mvpSpans[0].innerText.trim(),
                            team_name: mvpSpans[1].innerText.trim()
                        });
                    }
                }

                // Rookie (index 2)
                if (cells.length > 2) {
                    const rookieSpans = cells[2].querySelectorAll('span');
                    if (rookieSpans.length >= 2) {
                        results.push({
                            year: year,
                            award_type: 'Rookie of the Year',
                            category: null,
                            player_name: rookieSpans[0].innerText.trim(),
                            team_name: rookieSpans[1].innerText.trim()
                        });
                    }
                }
            });
            return results;
        }
        """
        return await page.evaluate(script)

    async def crawl_golden_glove(self, page: Page) -> List[Dict]:
        """
        Parses Golden Glove table.
        Cols: Year | P | C | 1B | 2B | 3B | SS | OF | DH
        """
        script = """
        () => {
            const rows = document.querySelectorAll('table tbody tr');
            const results = [];
            const categories = ['P', 'C', '1B', '2B', '3B', 'SS', 'OF', 'DH'];
            
            rows.forEach(tr => {
                const cells = tr.querySelectorAll('td');
                let year = parseInt(cells[0].innerText.trim());
                 if (isNaN(year)) {
                    const th = tr.querySelector('th');
                    if (th) year = parseInt(th.innerText.trim());
                }
                if (isNaN(year)) return;

                // Cells 1..8
                categories.forEach((cat, idx) => {
                    const cellIdx = idx + 1;
                    if (cellIdx < cells.length) {
                        const cell = cells[cellIdx];
                        
                        // Check for <p> tags (Outfielders often have multiple)
                        const paragraphs = cell.querySelectorAll('p');
                        if (paragraphs.length > 0) {
                            paragraphs.forEach(p => {
                                const spans = p.querySelectorAll('span');
                                if (spans.length >= 2) {
                                    results.push({
                                        year: year,
                                        award_type: 'Golden Glove',
                                        category: cat,
                                        player_name: spans[0].innerText.trim(),
                                        team_name: spans[1].innerText.trim()
                                    });
                                }
                            });
                        } else {
                            // Single winner in cell
                            const spans = cell.querySelectorAll('span');
                            if (spans.length >= 2) {
                                results.push({
                                    year: year,
                                    award_type: 'Golden Glove',
                                    category: cat,
                                    player_name: spans[0].innerText.trim(),
                                    team_name: spans[1].innerText.trim()
                                });
                            }
                        }
                    }
                });
            });
            return results;
        }
        """
        return await page.evaluate(script)

    async def crawl_defense_prize(self, page: Page) -> List[Dict]:
        """
        Parses Defense Prize.
        Cols: Year | P | C | 1B | 2B | 3B | SS | LF | CF | RF
        """
        script = """
        () => {
            const rows = document.querySelectorAll('table tbody tr');
            const results = [];
            const categories = ['P', 'C', '1B', '2B', '3B', 'SS', 'LF', 'CF', 'RF'];
            
            rows.forEach(tr => {
                const cells = tr.querySelectorAll('td');
                let year = parseInt(cells[0].innerText.trim());
                 if (isNaN(year)) {
                    const th = tr.querySelector('th');
                    if (th) year = parseInt(th.innerText.trim());
                }
                if (isNaN(year)) return;

                categories.forEach((cat, idx) => {
                    const cellIdx = idx + 1;
                    if (cellIdx < cells.length) {
                         const cell = cells[cellIdx];
                         const paragraphs = cell.querySelectorAll('p');
                         if (paragraphs.length > 0) {
                             paragraphs.forEach(p => {
                                 const spans = p.querySelectorAll('span');
                                 if (spans.length >= 2) {
                                     results.push({
                                         year: year,
                                         award_type: 'Defense Prize',
                                         category: cat,
                                         player_name: spans[0].innerText.trim(),
                                         team_name: spans[1].innerText.trim()
                                     });
                                 }
                             });
                         } else {
                             const spans = cell.querySelectorAll('span');
                             if (spans.length >= 2) {
                                 results.push({
                                     year: year,
                                     award_type: 'Defense Prize',
                                     category: cat,
                                     player_name: spans[0].innerText.trim(),
                                     team_name: spans[1].innerText.trim()
                                 });
                             }
                         }
                    }
                });
            });
            return results;
        }
        """
        return await page.evaluate(script)

    async def crawl_series_prize(self, page: Page) -> List[Dict]:
        """
        Parses Series Prize.
        Cols: Year | All-Star MVP | KS MVP
        """
        script = """
        () => {
            const rows = document.querySelectorAll('table tbody tr');
            const results = [];
            
            rows.forEach(tr => {
                const cells = tr.querySelectorAll('td');
                let year = parseInt(cells[0].innerText.trim());
                 if (isNaN(year)) {
                    const th = tr.querySelector('th');
                    if (th) year = parseInt(th.innerText.trim());
                }
                if (isNaN(year)) return;
                
                // AS MVP (Idx 1)
                if (cells.length > 1) {
                     const spans = cells[1].querySelectorAll('span');
                     if (spans.length >= 2) {
                         results.push({
                            year: year,
                            award_type: 'All-Star MVP',
                            category: null, 
                            player_name: spans[0].innerText.trim(),
                            team_name: spans[1].innerText.trim()
                         });
                     } else if (cells[1].innerText.trim() && cells[1].innerText.trim() !== '-') {
                        // Sometimes might be just text? Check previous findings.
                        // Assuming spans. If not, fallback to text parsing?
                        // Let's rely on spans for now as per other pages.
                     }
                }

                // KS MVP (Idx 2)
                if (cells.length > 2) {
                     const spans = cells[2].querySelectorAll('span');
                     if (spans.length >= 2) {
                         results.push({
                            year: year,
                            award_type: 'Korean Series MVP',
                            category: null, 
                            player_name: spans[0].innerText.trim(),
                            team_name: spans[1].innerText.trim()
                         });
                     }
                }
            });
            return results;
        }
        """
        return await page.evaluate(script)

    def save_to_db(self, data: List[Dict]):
        session = SessionLocal()
        repo = AwardRepository(session)
        count = 0
        try:
            for item in data:
                try:
                    repo.save_award(item)
                    count += 1
                except Exception as ex:
                    print(f"Skipping duplicate or error: {item} - {ex}")
            session.commit()
            print(f"✅ Saved {count} awards to database.")
        except Exception as e:
            session.rollback()
            print(f"❌ Error saving to DB: {e}")
        finally:
            session.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crawl KBO Awards")
    parser.add_argument("--types", nargs="+", help="Award types to crawl", 
                        choices=["player_prize", "golden_glove", "defense_prize", "series_prize", "all"])
    parser.add_argument("--save", action="store_true", help="Save to database")
    
    args = parser.parse_args()
    
    crawler = AwardCrawler()
    asyncio.run(crawler.run(args.types, args.save))
