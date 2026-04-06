import json
import asyncio
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.utils.playwright_pool import AsyncPlaywrightPool

async def parse_lineup_api():
    pool = AsyncPlaywrightPool(max_pages=1)
    
    # KBO Lineup API Parameters
    game_id = "20240924SSOB0"
    season_id = "2024"
    le_id = "1"
    sr_id = "0"
    api_url = "https://www.koreabaseball.com/ws/Schedule.asmx/GetLineUpAnalysis"
    
    await pool.start()
    try:
        page = await pool.acquire()
        # Set Referer by navigating to Main first
        await page.goto("https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx", wait_until="domcontentloaded")
        
        response = await page.request.post(
            api_url,
            form={"leId": le_id, "srId": sr_id, "seasonId": season_id, "gameId": game_id},
            headers={"Referer": "https://www.koreabaseball.com/Schedule/GameCenter/Preview/LineUp.aspx"}
        )
        
        if response.ok:
            data = await response.json()
            # data is a list of arrays. 
            # Index 1: Away Team Info
            # Index 2: Home Team Info
            # Index 3: Away Lineup Grid (JSON string)
            # Index 4: Home Lineup Grid (JSON string)
            
            away_team = data[1][0]['T_NM'] if len(data) > 1 else 'Unknown'
            home_team = data[2][0]['T_NM'] if len(data) > 2 else 'Unknown'
            print(f"Matchup: {away_team} vs {home_team}")
            
            for team_idx, team_name in [(3, away_team), (4, home_team)]:
                if len(data) > team_idx:
                    grid_str = data[team_idx]
                    if grid_str and isinstance(grid_str, list) and isinstance(grid_str[0], str):
                        grid_data = json.loads(grid_str[0])
                        rows = grid_data.get('rows', [])
                        print(f"\n--- {team_name} 라인업 ---")
                        for row in rows:
                            cells = row.get('row', [])
                            # Column 0: 타순 (1~9)
                            # Column 1: 포지션 (e.g. 중견수)
                            # Column 2: 이름 (e.g. 박해민)
                            if len(cells) >= 3:
                                order = cells[0].get('Text')
                                pos = cells[1].get('Text')
                                name = cells[2].get('Text')
                                if order and order.isdigit(): # 실제 타자인 경우만
                                    print(f"{order}번 타자 | {pos} | {name}")
        else:
            print("Failed to fetch lineup API")
            
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(parse_lineup_api())