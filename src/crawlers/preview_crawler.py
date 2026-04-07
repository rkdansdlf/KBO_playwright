"""
KBO Preview Crawler
Fetches Pre-game information (Starting Pitchers, Lineups) for LLM context generation.
Uses KBO's internal XHR APIs (GetKboGameList, GetLineUpAnalysis) for stability.
"""
from __future__ import annotations

import json
import asyncio
from typing import List, Dict, Any, Optional

from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.safe_print import safe_print as print

class PreviewCrawler:
    def __init__(self, request_delay: float = 1.0, pool: Optional[AsyncPlaywrightPool] = None):
        self.request_delay = request_delay
        self.pool = pool

    async def crawl_preview_for_date(self, game_date: str) -> List[Dict[str, Any]]:
        """
        주어진 날짜(game_date: 'YYYYMMDD')의 모든 경기에 대해
        선발투수와 선발 라인업(발표되었을 경우) 정보를 수집합니다.
        """
        print(f"🔍 Fetching Pre-game preview data for {game_date}...")
        
        pool = self.pool or AsyncPlaywrightPool(max_pages=1)
        owns_pool = self.pool is None
        await pool.start()
        
        results = []
        try:
            page = await pool.acquire()
            
            # 1. Fetch Game List and Starting Pitchers
            # Navigate to base page to set referer/cookies properly
            await page.goto("https://www.koreabaseball.com/", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(self.request_delay)
            
            list_url = "https://www.koreabaseball.com/ws/Main.asmx/GetKboGameList"
            list_res = await page.request.post(
                list_url,
                form={"leId": "1", "srId": "0,1,3,4,5,7,9", "date": game_date},
                headers={"Referer": "https://www.koreabaseball.com/"}
            )
            
            if not list_res.ok:
                print(f"❌ Failed to fetch game list for {game_date} (Status: {list_res.status})")
                return []
                
            data = await list_res.json()
            games = data.get('game', [])
            if not games:
                print(f"ℹ️ No games found or no starting pitchers announced for {game_date}.")
                return []
                
            for g in games:
                game_id = g.get('G_ID')
                if not game_id:
                    continue
                    
                season_year = str(g.get('SEASON_ID', game_date[:4]))
                le_id = str(g.get('LE_ID', 1))
                sr_id = str(g.get('SR_ID', 0))
                
                away_team_name = g.get('AWAY_NM', '')
                home_team_name = g.get('HOME_NM', '')
                away_starter = g.get('T_PIT_P_NM', '').strip()
                home_starter = g.get('B_PIT_P_NM', '').strip()
                away_starter_id = g.get('T_PIT_P_ID')
                home_starter_id = g.get('B_PIT_P_ID')
                stadium = g.get('S_NM')
                start_time = g.get('G_TM')
                
                preview_data = {
                    "game_id": game_id,
                    "game_date": game_date,
                    "stadium": stadium,
                    "start_time": start_time,
                    "away_team_name": away_team_name,
                    "home_team_name": home_team_name,
                    "away_starter": away_starter,
                    "away_starter_id": away_starter_id,
                    "home_starter": home_starter,
                    "home_starter_id": home_starter_id,
                    "away_lineup": [],
                    "home_lineup": []
                }
                
                # 2. Fetch Lineups for this specific game
                # Lineups might not be announced until ~1 hour before the game.
                await asyncio.sleep(self.request_delay)
                lineup_url = "https://www.koreabaseball.com/ws/Schedule.asmx/GetLineUpAnalysis"
                lineup_res = await page.request.post(
                    lineup_url,
                    form={"leId": le_id, "srId": sr_id, "seasonId": season_year, "gameId": game_id},
                    headers={"Referer": "https://www.koreabaseball.com/Schedule/GameCenter/Preview/LineUp.aspx"}
                )
                
                if lineup_res.ok:
                    try:
                        lu_data = await lineup_res.json()
                        # Parse Home Lineup (Index 3 is Home)
                        if len(lu_data) > 3:
                            preview_data["home_lineup"] = self._parse_lineup_grid(lu_data[3])
                        # Parse Away Lineup (Index 4 is Away)
                        if len(lu_data) > 4:
                            preview_data["away_lineup"] = self._parse_lineup_grid(lu_data[4])
                    except Exception as e:
                        print(f"⚠️ Error parsing lineup for {game_id}: {e}")
                else:
                     print(f"⚠️ Failed to fetch lineup API for {game_id}")
                
                results.append(preview_data)
                print(f"✅ Preview Extracted: {game_id} (Starter: {away_starter} vs {home_starter}) [Lineups: {len(preview_data['away_lineup'])} vs {len(preview_data['home_lineup'])}]")

            return results
            
        except Exception as e:
            print(f"❌ PreviewCrawler error: {e}")
            return []
        finally:
            if owns_pool:
                await pool.close()

    def _parse_lineup_grid(self, grid_str_list: List[str]) -> List[Dict[str, str]]:
        """Parses the nested KBO Lineup grid JSON string into a structured list."""
        lineup = []
        if not grid_str_list or not isinstance(grid_str_list, list):
            return lineup
            
        try:
            grid_data = json.loads(grid_str_list[0])
            rows = grid_data.get('rows', [])
            for row in rows:
                cells = row.get('row', [])
                if len(cells) >= 3:
                    order = str(cells[0].get('Text', '')).strip()
                    pos = str(cells[1].get('Text', '')).strip()
                    name = str(cells[2].get('Text', '')).strip()
                    
                    if order.isdigit():
                        lineup.append({
                            "batting_order": int(order),
                            "position": pos,
                            "player_name": name
                        })
        except Exception:
            pass
        return lineup
