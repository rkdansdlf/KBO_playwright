"""Read-only probe: 2001 schedule review links + boxscore page availability."""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path.cwd()))

SCHEDULE_URL = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
OUT_DIR = Path("data/schedules/legacy_html")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def probe(year: str, month: str, series: str = "0,9,6") -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(SCHEDULE_URL, wait_until="domcontentloaded", timeout=30000)
        time.sleep(2)

        page.select_option("#ddlYear", year)
        time.sleep(1)
        page.select_option("#ddlMonth", month)
        time.sleep(1)
        try:
            page.select_option("#ddlSeries", series)
        except PlaywrightError as e:  # noqa: BLE001
            print(f"  series select warning: {e}")
        time.sleep(3)

        tbl = page.query_selector(".tbl-type06")
        if not tbl:
            print("  ERROR: table .tbl-type06 not found")
            browser.close()
            return
        links = tbl.query_selector_all("tbody a")
        print(f"  tbody <a> count: {len(links)}")
        review_links = [l for l in links if "리뷰" in (l.inner_text() or "")]
        print(f"  review links: {len(review_links)}")
        for l in review_links[:5]:
            href = l.get_attribute("href")
            print(f"    href: {href}")
            if href:
                m = re.search(r"gameId=([A-Za-z0-9]+)", href)
                game_id = m.group(1) if m else "?"
                print(f"    game_id candidate: {game_id}")

        if review_links:
            href = review_links[0].get_attribute("href")
            print(f"\n  Navigating to first review page: {href}")
            page.goto(f"https://www.koreabaseball.com{href}", wait_until="domcontentloaded", timeout=45000)
            time.sleep(3)
            html = page.content()
            print(f"  review page html length: {len(html)}")
            print(f"  title: {page.title()}")
            out = OUT_DIR / f"{year}{month}_probe_review.html"
            out.write_text(html, encoding="utf-8")
            print(f"  saved to {out}")
            for marker in ["선수ID", "playerId", "tbl-type", "이닝", "타수", "투수"]:
                print(f"    marker '{marker}': {html.count(marker)}")

        browser.close()


if __name__ == "__main__":
    probe(year=sys.argv[1] if len(sys.argv) > 1 else "2001", month=sys.argv[2] if len(sys.argv) > 2 else "04")
