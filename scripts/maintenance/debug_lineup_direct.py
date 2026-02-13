
import asyncio
import os
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.utils.playwright_pool import AsyncPlaywrightPool

async def debug_lineup_direct():
    pool = AsyncPlaywrightPool(max_pages=1)
    
    # 2024 Opening Day Game (Hanwha vs LG)
    game_id = "20240323HHLG0"
    season_id = "2024"
    le_id = "1"
    sr_id = "0"
    
    url = f"https://www.koreabaseball.com/Schedule/GameCenter/Preview/LineUp.aspx?leId={le_id}&srId={sr_id}&seasonId={season_id}&gameId={game_id}"
    
    print(f"📡  Fetching Direct URL: {url}")
    
    await pool.start()
    try:
        page = await pool.acquire()
        await page.goto(url, wait_until="load", timeout=30000)
        
        # Save HTML
        content = await page.content()
        with open('data/lineup_direct.html', 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"📸 Saved to data/lineup_direct.html")
        
        # Check for player names
        if '문현빈' in content:
            print("✅ Found '문현빈' (Moon Hyun-bin)")
        else:
            print("❌ '문현빈' NOT found")
            
        if '홍창기' in content:
            print("✅ Found '홍창기' (Hong Chang-ki)")
        else:
            print("❌ '홍창기' NOT found")
            
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(debug_lineup_direct())
