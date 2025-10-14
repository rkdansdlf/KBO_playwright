"""Check Futures total (year-by-year) page."""
import asyncio
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from pathlib import Path

async def main():
    player_id = "51868"
    # Try the "Total" (year-by-year) page
    total_url = f"https://www.koreabaseball.com/Futures/Player/HitterTotal.aspx?playerId={player_id}"

    async with async_playwright() as p:
        br = await p.chromium.launch(headless=True)
        context = await br.new_context(locale='ko-KR')
        page = await context.new_page()

        try:
            print(f"Loading: {total_url}")
            await page.goto(total_url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(1)

            html = await page.content()
            Path("futures_total_debug.html").write_text(html, encoding='utf-8')
            print(f"Saved HTML ({len(html)} bytes)")

        finally:
            await br.close()

    # Analyze
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")
    print(f"\nTotal tables: {len(tables)}")

    for i, t in enumerate(tables):
        headers = [th.get_text(strip=True) for th in t.select("thead th, thead td")]
        if not headers:
            first_row = t.find("tr")
            if first_row:
                headers = [cell.get_text(strip=True) for cell in first_row.find_all(["th", "td"])]

        print(f"\nTable {i+1}:")
        print(f"  Headers: {headers[:15]}")

        rows = t.select("tbody tr")
        print(f"  Body rows: {len(rows)}")

        if rows:
            for j, row in enumerate(rows[:3]):
                cells = [cell.get_text(strip=True) for cell in row.find_all(["td", "th"])]
                print(f"  Row {j+1}: {cells[:10]}")

if __name__ == "__main__":
    asyncio.run(main())
