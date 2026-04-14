import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        print("Navigating to Relay page...")
        await page.goto("https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId=20241015SSOB0&gameDate=20241015&section=RELAY", wait_until="networkidle")
        
        await page.wait_for_timeout(3000) # Give it time to load JS
        
        # Check tabs
        tabs = await page.locator("ul.tab-type3 > li > a").all_text_contents()
        print("Available tabs:", tabs)
        
        # Try to find relay text
        html = await page.content()
        with open("relay_page.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("Saved HTML to relay_page.html")
        
        # Look for typical class names
        for class_name in ['.relay-bx', '.relay-txt', '.sms-bx', '#contents', '.txt-box', '.tab-type3']:
            count = await page.locator(class_name).count()
            print(f"Class {class_name} count: {count}")
            
        await browser.close()

asyncio.run(main())
