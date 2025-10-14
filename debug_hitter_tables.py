"""
Debug script to identify home/away hitter table selectors
"""
import asyncio
from playwright.async_api import async_playwright


async def debug_hitter_tables(game_id: str, game_date: str):
    """Debug hitter table structure for both away and home teams"""
    url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={game_id}&gameDate={game_date}&section=REVIEW"

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

            # Get row count
            rows = await table.query_selector_all('tbody tr')
            print(f"  data rows: {len(rows)}")

            # Check if this looks like a hitter table
            if len(rows) > 0:
                first_row = rows[0]
                cells = await first_row.query_selector_all('td, th')
                if len(cells) > 0:
                    first_cell_text = await cells[0].inner_text()
                    print(f"  first cell: {first_cell_text[:50]}")

            print()

        # Try specific hitter selectors
        print("=" * 60)
        print("Testing hitter-related selectors...")
        print("=" * 60)

        selectors_to_try = [
            '.tblAwayHitter1', '.tblAwayHitter2', '.tblAwayHitter3',
            '.tblHomeHitter1', '.tblHomeHitter2', '.tblHomeHitter3',
            'table[summary*="원정"]', 'table[summary*="홈"]',
            'table[summary*="타자"]', 'table[summary*="hitter"]',
            'div.box-score-area table',
            'table.tbl',
        ]

        for selector in selectors_to_try:
            elements = await page.query_selector_all(selector)
            if elements:
                print(f"\n✅ '{selector}' found {len(elements)} element(s)")
                for j, elem in enumerate(elements):
                    rows = await elem.query_selector_all('tbody tr')
                    print(f"   Table {j+1}: {len(rows)} rows")
            else:
                print(f"❌ '{selector}' found 0 elements")

        # Look for divs containing hitter data
        print("\n" + "=" * 60)
        print("Looking for hitter-related divs...")
        print("=" * 60)

        divs = await page.query_selector_all('div[class*="hitter"], div[class*="Hitter"], div[class*="batter"], div[class*="away"], div[class*="home"]')
        print(f"\n✅ Found {len(divs)} relevant divs")

        for i, div in enumerate(divs[:10]):  # First 10 only
            class_name = await div.get_attribute('class')
            id_name = await div.get_attribute('id')
            print(f"Div {i+1}: class='{class_name}' id='{id_name}'")

        # Check box-score-area structure
        print("\n" + "=" * 60)
        print("Analyzing box-score-area structure...")
        print("=" * 60)

        box_score = await page.query_selector('.box-score-area, div[class*="box-score"]')
        if box_score:
            # Find all tables within box-score area
            tables_in_box = await box_score.query_selector_all('table')
            print(f"\n✅ Found {len(tables_in_box)} tables in box-score area")

            for i, table in enumerate(tables_in_box[:10]):  # First 10
                class_name = await table.get_attribute('class')
                rows = await table.query_selector_all('tbody tr')

                # Check first row content to identify table type
                if len(rows) > 0:
                    first_row = rows[0]
                    first_cell = await first_row.query_selector('td')
                    if first_cell:
                        text = await first_cell.inner_text()
                        print(f"\nTable {i+1} (class: {class_name}):")
                        print(f"  Rows: {len(rows)}")
                        print(f"  First cell: {text[:50]}")

        await browser.close()


async def main():
    # Use the test game
    game_id = "20251001NCLG0"
    game_date = "20251001"

    await debug_hitter_tables(game_id, game_date)


if __name__ == "__main__":
    asyncio.run(main())
