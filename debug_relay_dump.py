
import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        url = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId=20250322LTLG0&gameDate=20250322"
        print(f"Loading {url}")
        await page.goto(url, wait_until="networkidle")
        
        # Click Relay Tab
        try:
            tab = await page.wait_for_selector('a:has-text("중계")', timeout=5000)
            await tab.click()
            print("Clicked Relay Tab")
            await asyncio.sleep(5)  # Wait for load
        except Exception as e:
            print(f"Failed to click: {e}")
            
        content = await page.content()
        with open("relay_dump.html", "w") as f:
            f.write(content)
        print("Dumped relay_dump.html")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
