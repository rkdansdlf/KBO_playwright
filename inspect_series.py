
import asyncio
from playwright.async_api import async_playwright

async def inspect_series():
    url = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url)
        
        # Get options from #ddlSeries
        options = await page.eval_on_selector_all('#ddlSeries option', "options => options.map(o => ({text: o.innerText, value: o.value}))")
        
        print("🔍 Found Series Options:")
        for opt in options:
            print(f"  - [{opt['value']}] {opt['text']}")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(inspect_series())
