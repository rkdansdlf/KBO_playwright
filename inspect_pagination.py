"""
Inspect pagination HTML structure
"""
import asyncio
from playwright.async_api import async_playwright

SEARCH_URL = "https://www.koreabaseball.com/Player/Search.aspx?searchWord=%25"
TABLE_ROWS = "table.tEx tbody tr"

async def inspect():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        try:
            await page.goto(SEARCH_URL, wait_until="domcontentloaded")
            await page.wait_for_selector(TABLE_ROWS, timeout=15000)

            # Get pagination HTML
            pagination_selectors = [
                "div.paging",
                "div[class*='paging']",
                "div[class*='pager']",
                "div[id*='Pager']",
                "div[id*='pager']",
            ]

            print("=== Searching for pagination containers ===")
            for sel in pagination_selectors:
                els = page.locator(sel)
                count = await els.count()
                if count > 0:
                    print(f"\n{sel}: found {count} elements")
                    for i in range(min(count, 2)):
                        html = await els.nth(i).inner_html()
                        print(f"\nElement {i}:")
                        print(html[:1000])

            # Check for any anchor tags with href containing page numbers or navigation
            print("\n\n=== Checking all anchor tags in possible pagination area ===")
            # Look for common pagination patterns
            anchors = page.locator("a[href*='javascript'], a[onclick*='page'], a[id*='btn']")
            count = await anchors.count()
            print(f"Found {count} potential pagination anchors")

            for i in range(min(count, 20)):
                href = await anchors.nth(i).get_attribute("href")
                onclick = await anchors.nth(i).get_attribute("onclick")
                text = await anchors.nth(i).inner_text()
                aid = await anchors.nth(i).get_attribute("id")
                print(f"\nAnchor {i}:")
                print(f"  id={aid}")
                print(f"  text='{text}'")
                print(f"  href={href}")
                print(f"  onclick={onclick}")

            print("\n=== Waiting 30 seconds ===")
            await asyncio.sleep(30)

        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(inspect())
