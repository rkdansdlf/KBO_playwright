import asyncio
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.utils.playwright_pool import AsyncPlaywrightPool

async def fetch_game_list():
    pool = AsyncPlaywrightPool(max_pages=1)
    url = "https://www.koreabaseball.com/ws/Main.asmx/GetKboGameList"
    
    await pool.start()
    try:
        page = await pool.acquire()
        await page.goto("https://www.koreabaseball.com/", wait_until="domcontentloaded")
        
        response = await page.request.post(
            url,
            form={"leId": "1", "srId": "0,1,3,4,5,7,9", "date": "20240924"},
            headers={"Referer": "https://www.koreabaseball.com/"}
        )
        
        if response.ok:
            data = await response.json()
            if 'game' in data:
                print(f"Found {len(data['game'])} games.")
                for g in data['game']:
                    print(f"{g['G_ID']} | Away: {g['T_PIT_P_NM']} vs Home: {g['B_PIT_P_NM']}")
            else:
                print("No 'game' key in response:", data)
        else:
            print("Failed:", response.status)
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(fetch_game_list())
