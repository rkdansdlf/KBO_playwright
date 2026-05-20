import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Navigate to KBO REGULAR Batting Basic1
        url = "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx"
        await page.goto(url)
        
        # Select 2024
        await page.select_option("#cphContents_cphContents_cphContents_ddlSeason_ddlSeason", "2024")
        await page.wait_for_load_state("networkidle")
        
        # Select REGULAR (already default usually)
        
        headers = await page.evaluate("""() => {
            const ths = Array.from(document.querySelectorAll('table.tData01 thead th'));
            return ths.map(th => th.innerText.trim());
        }""")
        print(f"Basic1 Headers: {headers}")
        
        # Basic2
        await page.click("#cphContents_cphContents_cphContents_udpContent > div.sub-content > div.record_result > div.tab-type > ul > li:nth-child(2) > a")
        await page.wait_for_load_state("networkidle")
        
        headers2 = await page.evaluate("""() => {
            const ths = Array.from(document.querySelectorAll('table.tData01 thead th'));
            return ths.map(th => th.innerText.trim());
        }""")
        print(f"Basic2 Headers: {headers2}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())
