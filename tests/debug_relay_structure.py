"""
Debug script to inspect RELAY section HTML structure.
"""
import asyncio
from playwright.async_api import async_playwright


async def main():
    game_id = "20251013SKSS0"
    game_date = "20251013"
    url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={game_id}&gameDate={game_date}&section=RELAY"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Show browser
        page = await browser.new_page()

        print(f"Loading: {url}")
        await page.goto(url, wait_until="networkidle", timeout=60000)
        await asyncio.sleep(3)

        # Get all possible relay-related selectors
        selectors = [
            '.relay-bx',
            '.game-relay',
            '#relay',
            '.relay',
            '[class*="relay"]',
            '[id*="relay"]',
            '.box-relay',
            '#game-relay'
        ]

        print("\nSearching for RELAY containers...")
        for sel in selectors:
            elements = await page.query_selector_all(sel)
            if elements:
                print(f"  Found {len(elements)} elements with selector: {sel}")
                first = elements[0]
                text = await first.inner_text()
                print(f"    Sample text: {text[:100]}...")

        # Get page HTML
        html = await page.content()

        # Save HTML for inspection
        with open("relay_page_structure.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("\nSaved full HTML to: relay_page_structure.html")

        print("\nPress Enter to close browser...")
        await asyncio.sleep(10)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
