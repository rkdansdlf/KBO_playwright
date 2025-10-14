"""
Debug script to inspect RELAY content after clicking tab.
"""
import asyncio
from playwright.async_api import async_playwright


async def main():
    game_id = "20251013SKSS0"
    game_date = "20251013"
    url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={game_id}&gameDate={game_date}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        print(f"Loading: {url}")
        await page.goto(url, wait_until="networkidle", timeout=60000)
        await asyncio.sleep(2)

        # Try to find and click RELAY tab
        print("\nLooking for RELAY tab...")
        relay_selectors = [
            'li.tab-tit[section="RELAY"] a',
            'a:has-text("중계")',
            'li:has-text("중계")',
        ]

        for sel in relay_selectors:
            try:
                element = await page.query_selector(sel)
                if element:
                    text = await element.inner_text()
                    print(f"  Found: {sel} - Text: {text}")
                    await element.click()
                    print(f"  Clicked!")
                    await asyncio.sleep(3)
                    break
            except Exception as e:
                print(f"  Failed: {sel} - {e}")

        # Get content after clicking
        print("\nExtracting gameCenterContents...")
        content_div = await page.query_selector('#gameCenterContents')
        if content_div:
            html = await content_div.inner_html()
            with open("tests/relay_content_after_click.html", "w", encoding="utf-8") as f:
                f.write(html)
            print("Saved to: tests/relay_content_after_click.html")

            # Try to find common relay structures
            print("\nLooking for relay data structures...")
            relay_structures = [
                'div[class*="relay"]',
                'div[class*="inning"]',
                'div[class*="play"]',
                'table',
                'ul',
                'li'
            ]

            for sel in relay_structures:
                elements = await content_div.query_selector_all(sel)
                if elements:
                    print(f"  {sel}: {len(elements)} found")

        print("\nKeeping browser open for 10 seconds...")
        await asyncio.sleep(10)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
