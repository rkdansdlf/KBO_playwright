"""
Simple debug script to see what happens when clicking RELAY tab.
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
        await asyncio.sleep(3)

        # Check if gameCenterContents exists before click
        before_click = await page.query_selector('#gameCenterContents')
        print(f"Before click - gameCenterContents exists: {before_click is not None}")

        # Click RELAY tab
        print("\nClicking RELAY tab...")
        relay_tab = await page.query_selector('a:has-text("텍스트중계")')
        if relay_tab:
            await relay_tab.click()
            print("Clicked!")
            await asyncio.sleep(5)

            # Check after click
            after_click = await page.query_selector('#gameCenterContents')
            print(f"After click - gameCenterContents exists: {after_click is not None}")

            if after_click:
                html = await after_click.inner_html()
                print(f"\nContent length: {len(html)} characters")
                print(f"First 500 chars:\n{html[:500]}")

        print("\nKeeping browser open for 15 seconds...")
        await asyncio.sleep(15)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
