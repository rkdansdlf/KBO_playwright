"""
Player Daily Stats Crawler (Game-by-Game)
Fetches transactional (per game) statistics for a specific player and season.
This is used to backfill missing or corrupted data in the game_stats tables.
"""

from __future__ import annotations
import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from playwright.async_api import Page, async_playwright

logger = logging.getLogger(__name__)

class PlayerDailyStatsCrawler:
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.base_url = "https://www.koreabaseball.com/Record/Player/{type}Detail/Daily.aspx?playerId={pid}"

    async def crawl_player_season(self, player_id: int, is_pitcher: bool, season: int) -> List[Dict[str, Any]]:
        p_type = "Pitcher" if is_pitcher else "Hitter"
        url = self.base_url.format(type=p_type, pid=player_id)
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            page = await browser.new_page()
            
            try:
                print(f"📡 Navigating to {url}...")
                await page.goto(url, wait_until="networkidle")
                
                # 1. Select Year
                year_selector = "#cphContents_cphContents_cphContents_ddlYear"
                try:
                    await page.select_option(year_selector, value=str(season))
                    await page.wait_for_load_state("networkidle")
                    await asyncio.sleep(1.5) # Wait for AJAX/Postback
                except Exception:
                    logger.exception(f"   ❌ Failed to select year {season}")
                    return []

                # 2. Parse All Tables
                # Daily.aspx displays multiple monthly tables.
                rows = await page.evaluate("""() => {
                    const tables = Array.from(document.querySelectorAll('.tbl.tt, .tEx'));
                    const results = [];
                    
                    tables.forEach(table => {
                        const trs = Array.from(table.querySelectorAll('tbody tr'));
                        trs.forEach(tr => {
                            const cells = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim());
                            if (cells.length > 5) { // Skip empty/header rows
                                results.push(cells);
                            }
                        });
                    });
                    return results;
                }""")
                
                print(f"   📊 Found {len(rows)} raw data rows on page.")
                
                all_games = []
                for row in rows:
                    if is_pitcher:
                        data = self._parse_pitcher_row(row, season)
                    else:
                        data = self._parse_hitter_row(row, season)
                    if data:
                        all_games.append(data)
                
                return all_games

            except Exception:
                logger.exception(f"   ❌ Error crawling player {player_id}")
                return []
            finally:
                await browser.close()
                
    def _parse_hitter_row(self, row: List[str], season: int) -> Optional[Dict[str, Any]]:
        # [0:Date, 1:Opp, 2:AVG1, 3:PA, 4:AB, 5:R, 6:H, 7:2B, 8:3B, 9:HR, 10:RBI, 11:SB, 12:CS, 13:BB, 14:HBP, 15:SO, 16:GDP, 17:AVG2]
        if len(row) < 17: return None
        # ... rest of method unchanged ...
        try:
            date_str = f"{season}-{row[0].replace('.', '-')}"
            return {
                'game_date': date_str,
                'opponent': row[1],
                'stats': {
                    'plate_appearances': int(row[3]),
                    'at_bats': int(row[4]),
                    'runs': int(row[5]),
                    'hits': int(row[6]),
                    'doubles': int(row[7]),
                    'triples': int(row[8]),
                    'home_runs': int(row[9]),
                    'rbi': int(row[10]),
                    'stolen_bases': int(row[11]),
                    'caught_stealing': int(row[12]),
                    'walks': int(row[13]),
                    'hbp': int(row[14]),
                    'strikeouts': int(row[15]),
                    'gdp': int(row[16])
                }
            }
        except Exception:
            return None

    def _parse_pitcher_row(self, row: List[str], season: int) -> Optional[Dict[str, Any]]:
        # [0:Date, 1:Opp, 2:Type, 3:Res, 4:ERA1, 5:TBF, 6:IP, 7:H, 8:HR, 9:BB, 10:HBP, 11:SO, 12:R, 13:ER, 14:ERA2]
        if len(row) < 14: return None
        try:
            date_str = f"{season}-{row[0].replace('.', '-')}"
            
            # Map Decision
            decision = None
            res = row[3]
            if '승' in res: decision = 'W'
            elif '패' in res: decision = 'L'
            elif '세' in res: decision = 'S'
            elif '홀' in res: decision = 'H'
            
            # Parse Innings (e.g. "5 2/3" -> outs)
            ip_str = row[6]
            innings_outs = self._parse_innings_to_outs(ip_str)

            return {
                'game_date': date_str,
                'opponent': row[1],
                'stats': {
                    'decision': decision,
                    'wins': 1 if decision == 'W' else 0,
                    'losses': 1 if decision == 'L' else 0,
                    'saves': 1 if decision == 'S' else 0,
                    'batters_faced': int(row[5]),
                    'innings_outs': innings_outs,
                    'hits_allowed': int(row[7]),
                    'home_runs_allowed': int(row[8]),
                    'walks_allowed': int(row[9]),
                    'hbp_allowed': int(row[10]),
                    'strikeouts': int(row[11]),
                    'runs_allowed': int(row[12]),
                    'earned_runs': int(row[13])
                }
            }
        except Exception:
            return None

    def _parse_innings_to_outs(self, ip_str: str) -> int:
        if not ip_str or ip_str == '-': return 0
        try:
            if ' ' in ip_str:
                whole, frac = ip_str.split(' ')
                outs = int(whole) * 3
                if '1/3' in frac: outs += 1
                elif '2/3' in frac: outs += 2
                return outs
            else:
                if '/' in ip_str: # Only fraction
                    if '1/3' in ip_str: return 1
                    if '2/3' in ip_str: return 2
                    return 0
                return int(ip_str) * 3
        except Exception:
            return 0

if __name__ == "__main__":
    async def test():
        crawler = PlayerDailyStatsCrawler()
        # Jose Fernandez 2020
        data = await crawler.crawl_player_season(69209, False, 2020)
        print(f"Collected {len(data)} games for hitter.")
        if data: print(data[0])
        
        # Pinto 2020
        data_p = await crawler.crawl_player_season(50815, True, 2020)
        print(f"Collected {len(data_p)} games for pitcher.")
        if data_p: print(data_p[0])

    asyncio.run(test())
