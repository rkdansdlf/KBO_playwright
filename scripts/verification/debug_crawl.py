import asyncio

from playwright.async_api import async_playwright


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1920, "height": 1080})

        url = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate=20250325&gameId=20250325HHLG0"
        print(f"Navigating to: {url}")

        await page.goto(url, wait_until="domcontentloaded")
        await asyncio.sleep(5)  # wait for JS to render

        # Find and click the REVIEW tab
        review_tab = await page.query_selector("li[section='REVIEW']")
        if review_tab:
            print("Clicking REVIEW tab...")
            await review_tab.click()
            await asyncio.sleep(3)
        else:
            print("REVIEW tab not found!")

        # Scan for tables
        tables = await page.query_selector_all("table")
        print(f"\nFound {len(tables)} table(s) on the page:")
        for t in tables:
            tid = await t.get_attribute("id")
            tclass = await t.get_attribute("class")
            print(f"  - Table ID: {tid}, Class: {tclass}")

        # Print table row HTML
        print("\n--- tblAwayHitter1 headers ---")
        headers = await page.query_selector_all("#tblAwayHitter1 thead th")
        header_texts = [await h.inner_text() for h in headers]
        print("Headers:", header_texts)

        print("\n--- tblAwayHitter1 first 3 rows ---")
        rows = await page.query_selector_all("#tblAwayHitter1 tbody tr")
        for i, r in enumerate(rows[:3]):
            html = await r.evaluate("el => el.outerHTML")
            print(f"Row {i + 1}:\n{html}\n")

        print("\n--- tblHomeHitter1 first 3 rows ---")
        rows = await page.query_selector_all("#tblHomeHitter1 tbody tr")
        for i, r in enumerate(rows[:3]):
            html = await r.evaluate("el => el.outerHTML")
            print(f"Row {i + 1}:\n{html}\n")

        # Take a full-page screenshot
        await page.screenshot(path="data/debug_2025_review_page.png", full_page=True)
        print("\nScreenshot saved to data/debug_2025_review_page.png")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
