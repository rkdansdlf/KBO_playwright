import time

from playwright.sync_api import sync_playwright


def check_team_catcher():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx")
        page.wait_for_load_state("networkidle")

        # 1. Select Team "SSG" (SK)
        print("Selecting Team SK...")
        with page.expect_response("**/Record/Player/Defense/Basic.aspx", timeout=15000):
            page.select_option("select#cphContents_cphContents_cphContents_ddlTeam_ddlTeam", "SK")
        page.wait_for_load_state("networkidle")
        time.sleep(3)

        # 2. Select Position "포수" (2)
        print("Selecting Position Catcher...")
        with page.expect_response("**/Record/Player/Defense/Basic.aspx", timeout=15000):
            page.select_option("select#cphContents_cphContents_cphContents_ddlPos_ddlPos", "2")
        page.wait_for_load_state("networkidle")
        time.sleep(5)

        print("Final HTML state check...")
        rows = page.query_selector_all("table.tData01.tt tbody tr")
        print(f"SSG Catchers: {len(rows)}")
        for row in rows:
            cells = row.query_selector_all("td")
            print(f"  {cells[1].inner_text()} | POS: {cells[3].inner_text()} | Columns: {len(cells)}")

        browser.close()


if __name__ == "__main__":
    check_team_catcher()
