import asyncio
import json

from playwright.async_api import async_playwright


async def inspect_game(page, url, year_label):
    print(f"\n--- Inspecting {year_label}: {url} ---")
    await page.goto(url, wait_until="networkidle", timeout=60000)
    await asyncio.sleep(5)  # Wait for potential JS rendering

    results = {}

    # Check Hitter 3 tables
    for tid in ["tblAwayHitter3", "tblHomeHitter3"]:
        table = await page.query_selector(f"table#{tid}")
        if table:
            headers = [await h.inner_text() for h in await table.query_selector_all("thead th")]
            results[tid] = {"headers": headers}
            # Check for HR and BB
            results[tid]["has_HR"] = "HR" in headers or "홈런" in headers
            results[tid]["has_BB"] = "BB" in headers or "4사구" in headers or "볼넷" in headers
        else:
            results[tid] = "Not found"

    # Check for inning-by-inning table between Hitter1 and Hitter3
    # We'll just look for all tables and see their IDs and order
    all_tables = await page.query_selector_all("table")
    table_sequence = []
    for t in all_tables:
        tid = await t.get_attribute("id")
        tclass = await t.get_attribute("class")
        table_sequence.append({"id": tid, "class": tclass})

    results["sequence"] = table_sequence

    # Check Pitcher tables
    for tid in ["tblAwayPitcher", "tblHomePitcher"]:
        table = await page.query_selector(f"table#{tid}")
        if table:
            headers = [await h.inner_text() for h in await table.query_selector_all("thead th")]
            results[tid] = {"headers": headers}
            # Get first row data
            first_row = await table.query_selector("tbody tr")
            if first_row:
                cells = [await c.inner_text() for c in await first_row.query_selector_all("td")]
                results[tid]["first_row"] = cells
        else:
            results[tid] = "Not found"

    return results


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Game 1 (2019)
        url1 = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate=20190323&gameId=20190323KTSK0&section=REVIEW"
        res1 = await inspect_game(page, url1, "2019")

        # Game 2 (2024)
        url2 = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate=20240323&gameId=20240323HHLG0&section=REVIEW"
        res2 = await inspect_game(page, url2, "2024")

        print("\n=== Comparison Results ===")
        print("2019 Results:")
        print(json.dumps(res1, indent=2, ensure_ascii=False))
        print("\n2024 Results:")
        print(json.dumps(res2, indent=2, ensure_ascii=False))

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
