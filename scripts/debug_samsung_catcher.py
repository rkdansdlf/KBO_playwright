import time

from playwright.sync_api import sync_playwright


def debug_samsung_catcher():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx")
        page.wait_for_load_state("networkidle")

        # 1. Select Samsung (SS)
        page.select_option("select#cphContents_cphContents_cphContents_ddlTeam_ddlTeam", "SS")
        page.wait_for_load_state("networkidle")
        time.sleep(2)

        # 2. Select Catcher (2)
        page.select_option("select#cphContents_cphContents_cphContents_ddlPos_ddlPos", "2")
        page.wait_for_load_state("networkidle")
        time.sleep(2)

        rows = page.query_selector_all("table.tData01.tt tbody tr")
        print(f"Samsung Catchers: {len(rows)}")
        for row in rows:
            cells = row.query_selector_all("td")
            txts = [c.inner_text().strip() for c in cells]
            print(f"  Player: {txts[1]} | CS: {txts[15]} | PB: {txts[13]}")

        browser.close()


if __name__ == "__main__":
    debug_samsung_catcher()
