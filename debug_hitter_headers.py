
import asyncio
from playwright.async_api import async_playwright

async def debug_headers():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        # Using REVIEW section as it was successful before
        url = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId=20241001LTNC0&gameDate=20241001&section=REVIEW"
        await page.goto(url, wait_until="networkidle")
        await asyncio.sleep(2)
        
        tables = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('table')).map(t => ({
                id: t.id,
                headers: Array.from(t.querySelectorAll('th')).map(th => th.innerText.trim()).filter(x => x),
                col_count: t.querySelectorAll('thead th').length
            }));
        }""")
        for t in tables:
            print(f"ID: {t['id'] or 'NoID'}, Cols: {t['col_count']}, Headers: {t['headers']}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_headers())
