"""
Test navigation to page 52 to see why pagination stopped
"""
import asyncio
from playwright.async_api import async_playwright

SEARCH_URL = "https://www.koreabaseball.com/Player/Search.aspx?searchWord=%25"
TABLE_ROWS = "table.tEx tbody tr"
NEXT_BTN = "a[id$='ucPager_btnNext']"

async def test_page_52():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        try:
            await page.goto(SEARCH_URL, wait_until="domcontentloaded")
            await page.wait_for_selector(TABLE_ROWS, timeout=15000)

            # Navigate to around page 50
            for i in range(50):
                next_btn = page.locator(NEXT_BTN)
                count = await next_btn.count()

                if count == 0:
                    print(f"❌ Next button not found at page {i+1}")
                    break

                # Check visibility
                try:
                    is_visible = await next_btn.first.is_visible()
                    is_enabled = await next_btn.first.is_enabled()
                    print(f"Page {i+1}: Next button visible={is_visible}, enabled={is_enabled}")
                except Exception as e:
                    print(f"Page {i+1}: Error checking button state: {e}")
                    break

                # Click next
                try:
                    await next_btn.first.click(timeout=5000)
                    await asyncio.sleep(2)  # Wait for page load
                except Exception as e:
                    print(f"❌ Click failed at page {i+1}: {e}")
                    break

            # Wait for manual inspection
            print("\n=== Waiting 60 seconds for manual inspection ===")
            await asyncio.sleep(60)

        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_page_52())
