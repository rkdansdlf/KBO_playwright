import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate=20260521&gameId=20260521LGHT0&section=REVIEW")
        await page.wait_for_selector("#tblAwayHitter1", timeout=10000)
        
        # Save HTML for inspection
        html = await page.content()
        with open("scratch/20260521LGHT0_review.html", "w") as f:
            f.write(html)
        print("HTML saved to scratch/20260521LGHT0_review.html")
        await browser.close()

asyncio.run(run())
