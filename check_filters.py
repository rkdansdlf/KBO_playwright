"""
Check if there are any filters applied on the search page
"""
import asyncio
from playwright.async_api import async_playwright

SEARCH_URL = "https://www.koreabaseball.com/Player/Search.aspx?searchWord=%25"

async def check_filters():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        try:
            await page.goto(SEARCH_URL, wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)

            # Check for any select/dropdown filters
            print("=== Checking for filter dropdowns ===")
            selects = page.locator("select")
            select_count = await selects.count()
            print(f"Found {select_count} select elements")

            for i in range(select_count):
                sel = selects.nth(i)
                name = await sel.get_attribute("name")
                sid = await sel.get_attribute("id")
                value = await sel.input_value()
                print(f"\nSelect {i}:")
                print(f"  name={name}, id={sid}")
                print(f"  current value={value}")

                # Get all options
                options = sel.locator("option")
                opt_count = await options.count()
                print(f"  options ({opt_count}):")
                for j in range(min(opt_count, 10)):
                    opt_value = await options.nth(j).get_attribute("value")
                    opt_text = await options.nth(j).inner_text()
                    selected = await options.nth(j).get_attribute("selected")
                    marker = " (SELECTED)" if selected else ""
                    print(f"    [{j}] value='{opt_value}', text='{opt_text}'{marker}")

            # Check for radio buttons
            print("\n\n=== Checking for radio buttons ===")
            radios = page.locator("input[type='radio']")
            radio_count = await radios.count()
            print(f"Found {radio_count} radio buttons")

            for i in range(min(radio_count, 10)):
                rad = radios.nth(i)
                name = await rad.get_attribute("name")
                value = await rad.get_attribute("value")
                checked = await rad.is_checked()
                print(f"Radio {i}: name={name}, value={value}, checked={checked}")

            # Check the search result count text
            print("\n\n=== Search result count ===")
            body_text = await page.locator("body").inner_text()
            import re
            match = re.search(r'검색결과\s*:\s*(\d+[,\d]*)\s*건', body_text)
            if match:
                count_text = match.group(1)
                print(f"검색결과: {count_text}건")

            print("\n=== Waiting 30 seconds for inspection ===")
            await asyncio.sleep(30)

        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(check_filters())
