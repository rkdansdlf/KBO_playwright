"""
Check if there's a total count indicator on the player search page
"""
import asyncio
from playwright.async_api import async_playwright

SEARCH_URL = "https://www.koreabaseball.com/Player/Search.aspx?searchWord=%25"

async def check_total():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        try:
            await page.goto(SEARCH_URL, wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)

            # Get full page HTML
            html = await page.content()

            # Search for number patterns that might be total count
            import re
            numbers = re.findall(r'(\d{1,3}[,\d]*)\s*(?:건|명|개|players?|results?)', html, re.IGNORECASE)
            print("Potential count indicators:")
            for num in set(numbers):
                print(f"  - {num}")

            # Check specific elements that might show total
            selectors = [
                "span.total",
                "div.total",
                "span[class*='count']",
                "div[class*='count']",
                "span[class*='total']",
            ]

            print("\n=== Checking count elements ===")
            for sel in selectors:
                els = page.locator(sel)
                count = await els.count()
                if count > 0:
                    for i in range(count):
                        text = await els.nth(i).inner_text()
                        print(f"{sel}[{i}]: {text}")

            # Get all text from page
            body_text = await page.locator("body").inner_text()

            # Search for "5120" or "5,120" in body
            if "5120" in body_text or "5,120" in body_text:
                print("\n✅ Found '5120' or '5,120' in page body")
                # Find context around it
                idx = body_text.find("5120") if "5120" in body_text else body_text.find("5,120")
                context = body_text[max(0, idx-50):idx+50]
                print(f"Context: ...{context}...")
            else:
                print("\n❌ '5120' or '5,120' not found in page body")

            print("\n=== Waiting 10 seconds for manual inspection ===")
            await asyncio.sleep(10)

        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(check_total())
