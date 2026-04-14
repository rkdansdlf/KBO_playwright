import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        print("Navigating...")
        await page.goto("https://www.koreabaseball.com/Record/Player/PitcherDetail/Basic.aspx?playerId=79171", wait_until="domcontentloaded")
        
        # Try finding typical selectors or just wait briefly
        try:
            await page.wait_for_selector(".player_basic", timeout=5000)
        except:
            pass
            
        html = await page.content()
        with open("profile.html", "w") as f:
            f.write(html)
        print("Done")
        await browser.close()

asyncio.run(main())
