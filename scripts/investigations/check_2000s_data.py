import asyncio
import os

from playwright.async_api import async_playwright


async def check_historical_data(game_id, game_date):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1280, "height": 1200})
        page = await context.new_page()

        # Test Review section
        url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={game_date}&gameId={game_id}&section=REVIEW"
        print(f"[*] Testing {game_id} ({game_date}) - {url}")

        try:
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await page.wait_for_timeout(3000)

            content = await page.content()
            print(f"   - Final URL: {page.url}")
            print(f"   - Content Length: {len(content)}")

            # Check for box score tables
            hitter_tables = await page.query_selector_all("table[id*='Hitter']")
            pitcher_tables = await page.query_selector_all("table[id*='Pitcher']")
            print(f"   - Hitter Tables: {len(hitter_tables)}, Pitcher Tables: {len(pitcher_tables)}")

            if len(hitter_tables) > 0:
                header = await hitter_tables[0].query_selector("thead tr")
                if header:
                    print(f"   - Hitter Header: {await header.inner_text()}")

            # Take screenshot
            os.makedirs("debug_shots/historical", exist_ok=True)
            await page.screenshot(path=f"debug_shots/historical/{game_id}.png")

        except Exception as e:
            print(f"   [X] Error: {e}")
        finally:
            await browser.close()


async def main():
    # 2001
    await check_historical_data("20010405HHSS0", "20010405")
    # 2002
    await check_historical_data("20020405HTOB0", "20020405")
    # 2009
    await check_historical_data("20090404HHLT0", "20090404")


if __name__ == "__main__":
    asyncio.run(main())
