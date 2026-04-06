import asyncio
import os
import sys
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.utils.playwright_pool import AsyncPlaywrightPool

async def trace_preview_click():
    pool = AsyncPlaywrightPool(max_pages=1)
    game_id = "20240924SSOB0"
    game_date = "20240924"
    url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={game_date}&gameId={game_id}"
    
    await pool.start()
    try:
        page = await pool.acquire()
        
        async def handle_response(response):
            if "Preview" in response.url or "ws/" in response.url:
                print(f"✅ Intercepted URL: {response.url}")
                try:
                    if response.url.endswith(".aspx") or response.url.endswith(".html"):
                        print(f"  Content: {(await response.text())[:500]}")
                    else:
                        print(f"  JSON: {str(await response.json())[:500]}")
                except:
                    pass
                    
        page.on("response", handle_response)
        
        print(f"📡 Navigating to {url}")
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(2)
        
        print("👆 Clicking '프리뷰' (Preview) tab...")
        # KBO GameCenter tabs usually use class="tab" or similar
        tabs = await page.locator('.tab-area li a, .tab li a').all()
        for t in tabs:
            text = await t.inner_text()
            if "프리뷰" in text or "PREVIEW" in text.upper():
                await t.click()
                print(f"  Clicked {text} tab.")
                break
        
        await asyncio.sleep(5) # Wait for network requests to settle
        
        print("\n--- Final HTML Content ---")
        html = await page.content()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'lxml')
        main_area = soup.select_one('#tabPreview, .preview-cont, .contents')
        if main_area:
            print(main_area.get_text(separator=' ', strip=True)[:1000])
        else:
             print("No preview container found")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(trace_preview_click())
