"""
Debug script to understand KBO player search pagination
"""
import asyncio
from playwright.async_api import async_playwright

SEARCH_URL = "https://www.koreabaseball.com/Player/Search.aspx?searchWord=%25"
TABLE_ROWS = "table.tEx tbody tr"

async def debug_pagination():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        try:
            await page.goto(SEARCH_URL, wait_until="domcontentloaded")
            await page.wait_for_selector(TABLE_ROWS, timeout=15000)

            # Check initial page info
            try:
                page_info = await page.locator("input[id$='hfPage']").input_value()
                print(f"Current page: {page_info}")
            except Exception as e:
                print(f"Could not get page info: {e}")

            # Find all pagination buttons
            print("\n=== Pagination buttons ===")

            # Check for Next button
            next_selectors = [
                "a[id$='ucPager_btnNext']",
                "a[id$='_btnNext']",
                "a.btn-paging-next",
                "//a[contains(@id, 'btnNext')]",
            ]

            for selector in next_selectors:
                if selector.startswith("//"):
                    locator = page.locator(f"xpath={selector}")
                else:
                    locator = page.locator(selector)

                count = await locator.count()
                print(f"{selector}: count={count}")

                if count > 0:
                    is_visible = await locator.first.is_visible()
                    is_enabled = await locator.first.is_enabled()
                    href = await locator.first.get_attribute("href")
                    onclick = await locator.first.get_attribute("onclick")
                    print(f"  visible={is_visible}, enabled={is_enabled}")
                    print(f"  href={href}")
                    print(f"  onclick={onclick}")

            # Check pagination container
            print("\n=== Pagination container ===")
            pager = page.locator("div.paging, div[class*='pag'], div[id$='Pager']")
            pager_count = await pager.count()
            print(f"Pager containers found: {pager_count}")

            if pager_count > 0:
                pager_html = await pager.first.inner_html()
                print(f"Pager HTML (first 500 chars):\n{pager_html[:500]}")

            # Try to understand page structure
            print("\n=== Page number buttons ===")
            page_num_buttons = page.locator("a[id$='ucPager_btnNo']")
            page_num_count = await page_num_buttons.count()
            print(f"Page number buttons: {page_num_count}")

            # Wait for user to inspect
            print("\n=== Waiting 30 seconds for manual inspection ===")
            await asyncio.sleep(30)

        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_pagination())
