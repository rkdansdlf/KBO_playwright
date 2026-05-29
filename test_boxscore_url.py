import asyncio

from playwright.async_api import async_playwright


async def try_boxscore_url():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Try direct URL with section=BOXSCORE
        url = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate=20240323&gameId=20240323HHLG0&section=BOXSCORE"
        print(f"Navigating to {url}...")
        await page.goto(url, wait_until="networkidle")
        await page.wait_for_timeout(5000)

        content = await page.content()
        if "박스스코어" in content:
            print("Success! '박스스코어' found in page content with section=BOXSCORE.")
            # Check for batter stats
            if "타수" in content or "안타" in content:
                print("Batter stats found!")
        else:
            print("'박스스코어' still NOT found.")

        # Take screenshot
        await page.screenshot(path="gamecenter_boxscore.png")

        # Let's search for any link that might lead to stats
        links = await page.query_selector_all("a")
        for link in links:
            text = await link.inner_text()
            if "박스" in text or "기록" in text:
                print(f"Found link: {text.strip()} -> {await link.get_attribute('href')}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(try_boxscore_url())
