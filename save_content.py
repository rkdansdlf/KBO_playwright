import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        url = "https://www.koreabaseball.com/Record/Player/PitcherDetail/Daily.aspx?playerId=50815"
        print(f"Navigating to {url}")
        await page.goto(url)
        
        # Select 2020
        print("Selecting year 2020...")
        year_select = await page.query_selector("select[id*='ddlYear']")
        if year_select:
            await year_select.select_option("2020")
            await page.wait_for_timeout(5000) 
            await page.wait_for_load_state("networkidle")
        
        content = await page.inner_html("#cphContents_cphContents_cphContents_udpRecord")
        with open("content_area.html", "w") as f:
            f.write(content)
        print("Content saved to content_area.html")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())
