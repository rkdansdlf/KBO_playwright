
import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        # Game 20250322LTLG0
        await page.goto("https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId=20250322LTLG0&gameDate=20250322&section=REVIEW", wait_until="networkidle")
        
        tables = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('table')).map((t, i) => `<!-- TABLE ${i} -->\n` + t.outerHTML);
        }""")
        
        with open("tables_dump.html", "w", encoding="utf-8") as f:
            for t in tables:
                f.write(t + "\n\n")
        
        print(f"Dumped {len(tables)} tables to tables_dump.html")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
