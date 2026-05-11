import asyncio
from playwright.async_api import async_playwright
import os

async def find_boxscore_link():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={'width': 1280, 'height': 1200})
        page = await context.new_page()
        
        url = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate=20240323&gameId=20240323HHLG0&section=REVIEW"
        print(f"Navigating to {url}...")
        await page.goto(url, wait_until="networkidle")
        
        # Wait for the GameCenter content to load
        await page.wait_for_timeout(5000)
        
        # Take a screenshot of the initial state
        await page.screenshot(path="gamecenter_initial.png")
        print("Screenshot saved to gamecenter_initial.png")
        
        # List all button and link texts
        elements = await page.query_selector_all("a, button, li")
        print(f"Found {len(elements)} clickable-looking elements.")
        
        for el in elements:
            text = await el.inner_text()
            text = text.strip()
            if text and ("박스" in text or "score" in text.lower() or "상세" in text or "기록" in text):
                outer_html = await el.evaluate("el => el.outerHTML")
                print(f"Potential Match: Text='{text}', HTML='{outer_html[:100]}...'")
        
        # Check if there's an iframe
        iframes = page.frames
        print(f"Total frames: {len(iframes)}")
        for i, frame in enumerate(iframes):
            print(f"Frame {i}: {frame.name} ({frame.url})")
            if "GameCenter" in frame.url or "BoxScore" in frame.url:
                print(f"Relevant frame found: {frame.url}")
                # Try to find text in this frame
                sub_elements = await frame.query_selector_all("a, button, span")
                for sub_el in sub_elements:
                    sub_text = await sub_el.inner_text()
                    sub_text = sub_text.strip()
                    if sub_text and ("박스" in sub_text or "score" in sub_text.lower() or "상세" in sub_text):
                        print(f"Found in frame {i}: Text='{sub_text}'")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(find_boxscore_link())
