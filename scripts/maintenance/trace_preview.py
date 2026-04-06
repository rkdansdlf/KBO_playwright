import asyncio
import os
import sys
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.utils.playwright_pool import AsyncPlaywrightPool

async def trace_preview():
    pool = AsyncPlaywrightPool(max_pages=1)
    game_id = "20240924SSOB0"
    game_date = "20240924"
    url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={game_date}&gameId={game_id}&section=PREVIEW"
    
    await pool.start()
    try:
        page = await pool.acquire()
        
        async def handle_response(response):
            if "ws/Schedule.asmx" in response.url or "ws/" in response.url or "asmx" in response.url:
                try:
                    data = await response.json()
                    print(f"✅ XHR Found: {response.url}")
                    print(json.dumps(data, indent=2, ensure_ascii=False)[:1000])
                except:
                    pass
                    
        page.on("response", handle_response)
        
        print(f"📡 Navigating to {url} and listening to XHR...")
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(5) 
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(trace_preview())