"""
Debug script to find correct selectors for player list page
"""
import asyncio
from playwright.async_api import async_playwright


async def debug_player_list_page():
    """Debug player list page structure"""

    # Test with hitter page
    url = "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx?gyear=2024"

    print(f"🔍 Debugging: {url}\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(url, wait_until="networkidle")
        await asyncio.sleep(2)

        # Find all tables
        print("=" * 60)
        print("Looking for ALL tables...")
        print("=" * 60)

        tables = await page.query_selector_all('table')
        print(f"\n✅ Found {len(tables)} total tables\n")

        for i, table in enumerate(tables):
            class_name = await table.get_attribute('class')
            id_name = await table.get_attribute('id')
            summary = await table.get_attribute('summary')

            print(f"Table {i+1}:")
            print(f"  class: {class_name}")
            print(f"  id: {id_name}")
            print(f"  summary: {summary}")

            # Get first few rows to see structure
            rows = await table.query_selector_all('tr')
            print(f"  total rows: {len(rows)}")

            if len(rows) > 0:
                first_row = rows[0]
                text = await first_row.inner_text()
                print(f"  first row: {text[:100]}...")

            print()

        # Try specific selectors
        print("=" * 60)
        print("Testing specific selectors...")
        print("=" * 60)

        selectors_to_try = [
            'table.tData',
            'table.tEx',
            'table#cphContents_cphContents_cphContents_udpRecord',
            'div.record_result table',
            'table[summary*="선수"]',
            'table[summary*="기록"]',
        ]

        for selector in selectors_to_try:
            elements = await page.query_selector_all(selector)
            if elements:
                print(f"\n✅ '{selector}' found {len(elements)} element(s)")
                # Show first element's rows count
                if len(elements) > 0:
                    rows = await elements[0].query_selector_all('tbody tr')
                    print(f"   First table has {len(rows)} data rows")
            else:
                print(f"❌ '{selector}' found 0 elements")

        # Look for player links
        print("\n" + "=" * 60)
        print("Looking for player links...")
        print("=" * 60)

        player_links = await page.query_selector_all('a[href*="playerId"]')
        print(f"\n✅ Found {len(player_links)} player links")

        if player_links:
            print(f"\nSample player links:")
            for link in player_links[:5]:
                href = await link.get_attribute('href')
                text = await link.inner_text()
                print(f"  {text}: {href}")

        await browser.close()


async def main():
    await debug_player_list_page()


if __name__ == "__main__":
    asyncio.run(main())
