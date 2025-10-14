"""
Debug script to find correct selectors for hitter tables
"""
import asyncio
from playwright.async_api import async_playwright


async def debug_game_page(game_id: str, game_date: str):
    """Debug game page structure"""
    url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={game_id}&gameDate={game_date}&section=REVIEW"

    print(f"🔍 Debugging: {url}\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # headless=False to see browser
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
            print(f"Table {i+1}:")
            print(f"  class: {class_name}")
            print(f"  id: {id_name}")

            # Get first row to see structure
            first_row = await table.query_selector('tr')
            if first_row:
                text = await first_row.inner_text()
                print(f"  first row: {text[:100]}...")
            print()

        # Specifically look for hitter tables
        print("=" * 60)
        print("Looking for HITTER tables...")
        print("=" * 60)

        # Try different selectors
        selectors_to_try = [
            '.tblAwayHitter1',
            '.tblAwayHitter2',
            '.tblAwayHitter3',
            '.tblHomeHitter1',
            '.tblHomeHitter2',
            '.tblHomeHitter3',
            'table[summary*="타자"]',
            'table[summary*="hitter"]',
            '.tbl.tt',
            'div.box-score-area table',
        ]

        for selector in selectors_to_try:
            tables = await page.query_selector_all(selector)
            if tables:
                print(f"\n✅ Selector '{selector}' found {len(tables)} table(s)")
                for j, table in enumerate(tables):
                    rows = await table.query_selector_all('tbody tr')
                    print(f"   Table {j+1}: {len(rows)} rows")
            else:
                print(f"❌ Selector '{selector}' found 0 tables")

        # Check for specific divs
        print("\n" + "=" * 60)
        print("Looking for DIVS with class containing 'hitter'...")
        print("=" * 60)

        divs = await page.query_selector_all('div[class*="hitter"], div[class*="Hitter"]')
        print(f"\n✅ Found {len(divs)} divs with 'hitter' in class")

        for i, div in enumerate(divs):
            class_name = await div.get_attribute('class')
            print(f"Div {i+1}: class='{class_name}'")

        # Wait to inspect manually if needed
        print("\n⏸️  Browser will stay open for 30 seconds for manual inspection...")
        await asyncio.sleep(30)

        await browser.close()


async def main():
    # Use the game from POC test
    game_id = "20251001NCLG0"
    game_date = "20251001"

    await debug_game_page(game_id, game_date)


if __name__ == "__main__":
    asyncio.run(main())
