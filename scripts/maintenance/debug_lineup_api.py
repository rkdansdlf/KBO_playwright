
import asyncio
import os
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.utils.playwright_pool import AsyncPlaywrightPool

async def debug_lineup_api():
    pool = AsyncPlaywrightPool(max_pages=1)
    
    # 2024 Opening Day Game (Hanwha vs LG)
    game_id = "20240323HHLG0"
    season_id = "2024"
    le_id = "1"
    sr_id = "0"
    
    api_url = "https://www.koreabaseball.com/ws/Schedule.asmx/GetLineUpAnalysis"
    
    print(f"📡  Testing API: {api_url}")
    
    await pool.start()
    try:
        page = await pool.acquire()
        # Navigate to base page first to set cookies/referer context
        await page.goto("https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx", wait_until="domcontentloaded")
        
        # Make API request
        response = await page.request.post(
            api_url,
            form={
                "leId": le_id,
                "srId": sr_id,
                "seasonId": season_id,
                "gameId": game_id
            },
            headers={
                "Referer": "https://www.koreabaseball.com/Schedule/GameCenter/Preview/LineUp.aspx"
            }
        )
        
        print(f"Status: {response.status}")
        if response.ok:
            data = await response.json()
            print("✅ API Response Received")
            # Dump structure
            import json
            print(json.dumps(data, indent=2, ensure_ascii=False)[:2000]) # First 2000 chars
        else:
            print(f"❌ API Request Failed: {response.status} {response.status_text}")
            print(await response.text())
            
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(debug_lineup_api())
