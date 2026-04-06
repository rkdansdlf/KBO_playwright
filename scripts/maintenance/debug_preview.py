import asyncio
import os
import sys
import re

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.utils.playwright_pool import AsyncPlaywrightPool
from bs4 import BeautifulSoup

async def debug_preview():
    pool = AsyncPlaywrightPool(max_pages=1)
    game_id = "20240924SSOB0"
    game_date = "20240924"
    url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={game_date}&gameId={game_id}&section=PREVIEW"
    
    await pool.start()
    try:
        page = await pool.acquire()
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(4) 
        
        html = await page.content()
        soup = BeautifulSoup(html, 'lxml')
        
        # Let's find any text containing "선발투수", "상대전적", or "방어율"
        elements = soup.find_all(string=re.compile(r'선발|방어율|상대|시즌 성적'))
        print(f"Found {len(elements)} matching text elements.")
        
        for el in elements:
            parent = el.parent
            # Move up slightly to get the container block
            for _ in range(3):
                if parent and parent.name not in ['html', 'body']:
                    if parent.get('class'):
                        print(f"Text: '{el.strip()}' -> Container: {parent.name}, class: {parent.get('class')}")
                        break
                    parent = parent.parent
                    
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(debug_preview())
