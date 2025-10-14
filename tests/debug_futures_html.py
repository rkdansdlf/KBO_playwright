"""Debug Futures crawler by saving HTML."""
import asyncio
from playwright.async_api import async_playwright
from pathlib import Path

async def main():
    player_id = "51868"
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(locale='ko-KR')
        page = await context.new_page()

        # Try hitter URL
        hitter_url = f"https://www.koreabaseball.com/Futures/Player/HitterDetail.aspx?playerId={player_id}"
        print(f"Fetching: {hitter_url}")

        try:
            await page.goto(hitter_url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)

            html = await page.content()
            Path("debug_futures_hitter.html").write_text(html, encoding='utf-8')
            print(f"Saved hitter HTML ({len(html)} bytes)")

            # Check if "퓨처스" tab exists
            futures_tab = await page.query_selector('a:has-text("퓨처스")')
            if futures_tab:
                print("Found Futures tab, clicking...")
                await futures_tab.click()
                await asyncio.sleep(2)
                html2 = await page.content()
                Path("debug_futures_clicked.html").write_text(html2, encoding='utf-8')
                print(f"Saved clicked HTML ({len(html2)} bytes)")
            else:
                print("No Futures tab found")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
