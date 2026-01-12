
import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        # Force section=RELAY
        url = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId=20250322LTLG0&gameDate=20250322&section=RELAY"
        print(f"Loading {url}")
        await page.goto(url, wait_until="networkidle")
        
        await asyncio.sleep(5) # Wait for potential JS render
        
        content = await page.content()
        with open("relay_dump_direct.html", "w") as f:
            f.write(content)
        print("Dumped relay_dump_direct.html")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
