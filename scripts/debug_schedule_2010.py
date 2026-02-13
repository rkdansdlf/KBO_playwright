import asyncio
from playwright.async_api import async_playwright

async def debug_2010():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        print("Navigate to Schedule...")
        await page.goto("https://www.koreabaseball.com/Schedule/Schedule.aspx")
        
        # Select 2010
        print("Select 2010...")
        await page.select_option("#ddlYear", "2010")
        await page.wait_for_load_state("networkidle")
        await asyncio.sleep(1)
        
        # Select Month 04
        print("Select Month 04...")
        await page.select_option("#ddlMonth", "04")
        await page.wait_for_load_state("networkidle")
        await asyncio.sleep(1)
        
        # Check Series Options
        options = await page.eval_on_selector_all("#ddlSeries option", "opts => opts.map(o => ({text: o.innerText, value: o.value}))")
        print("\n--- Available Series for 2010 ---")
        for opt in options:
            print(f"Option: {opt['text']} (Value: {opt['value']})")
        
        # Try each series
        for opt in options:
            val = opt['value']
            text = opt['text']
            print(f"\n--- Testing Series: {text} ({val}) ---")
            
            await page.select_option("#ddlSeries", val)
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(1)
            
            # Check rows
            rows = await page.eval_on_selector_all(".tbl tbody tr", "rows => rows.length")
            first_row_text = await page.eval_on_selector(".tbl tbody tr:first-child", "row => row ? row.innerText : 'None'")
            print(f"Rows found: {rows}")
            print(f"First row text: {first_row_text.strip()}")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_2010())
