"""Debug Futures batting crawler."""
import asyncio
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from pathlib import Path

async def main():
    player_id = "51868"
    # CORRECT URL: Futures-specific page
    profile_url = f"https://www.koreabaseball.com/Futures/Player/HitterDetail.aspx?playerId={player_id}"

    async with async_playwright() as p:
        br = await p.chromium.launch(headless=True)
        context = await br.new_context(locale='ko-KR')
        page = await context.new_page()

        try:
            print(f"Loading: {profile_url}")
            await page.goto(profile_url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(1)

            # Try to click Futures tab
            try:
                futures_tab = await page.wait_for_selector('text="퓨처스"', timeout=3000)
                if futures_tab:
                    print("Found Futures tab, clicking...")
                    await futures_tab.click()
                    await asyncio.sleep(2)
                else:
                    print("No Futures tab found")
            except Exception as e:
                print(f"Error clicking tab: {e}")

            html = await page.content()
            Path("futures_profile_debug.html").write_text(html, encoding='utf-8')
            print(f"Saved HTML ({len(html)} bytes)")

        finally:
            await br.close()

    # Analyze HTML
    soup = BeautifulSoup(html, "lxml")

    print("\n=== Looking for '퓨처스' labels ===")
    labels = soup.find_all(lambda tag: "퓨처스" in tag.get_text())
    for i, label in enumerate(labels[:5]):
        print(f"{i+1}. {label.name}: {label.get_text(strip=True)[:50]}")

    print("\n=== All tables on page ===")
    tables = soup.find_all("table")
    print(f"Total tables: {len(tables)}")

    for i, t in enumerate(tables):
        print(f"\nTable {i+1}:")
        print(f"  ID: {t.get('id')}")
        print(f"  Class: {t.get('class')}")

        # Headers
        headers = [th.get_text(strip=True) for th in t.select("thead th, thead td")]
        if not headers:
            first_row = t.find("tr")
            if first_row:
                headers = [cell.get_text(strip=True) for cell in first_row.find_all(["th", "td"])]

        print(f"  Headers ({len(headers)}): {headers[:10]}")

        # Row count
        rows = t.select("tbody tr")
        if not rows:
            rows = t.find_all("tr")
        print(f"  Rows: {len(rows)}")

        # First data row
        if rows:
            first_row = rows[0]
            cells = [cell.get_text(strip=True) for cell in first_row.find_all(["td", "th"])]
            print(f"  First row: {cells[:10]}")

if __name__ == "__main__":
    asyncio.run(main())
