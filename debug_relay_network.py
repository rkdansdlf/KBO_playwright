
import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Log requests
        page.on("request", lambda request: print(f">> {request.method} {request.url}"))
        
        url = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId=20250322LTLG0&gameDate=20250322"
        print(f"Loading {url}")
        await page.goto(url, wait_until="networkidle")
        
        try:
            tab = await page.wait_for_selector('a:has-text("중계")', timeout=5000)
            print("Clicking Relay Tab...")
            await tab.click()
            await page.wait_for_timeout(5000)
        except Exception as e:
            print(f"Error: {e}")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
