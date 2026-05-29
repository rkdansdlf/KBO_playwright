import time

from playwright.sync_api import sync_playwright


def check_all_teams_catcher():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx")
        page.wait_for_load_state("networkidle")

        # Select 2025
        page.select_option("select#cphContents_cphContents_cphContents_ddlSeason_ddlSeason", "2025")
        page.wait_for_load_state("networkidle")

        # 1. Select "Catcher" (2)
        print("Selecting Position Catcher...")
        with page.expect_response("**/Record/Player/Defense/Basic.aspx", timeout=15000):
            page.select_option("select#cphContents_cphContents_cphContents_ddlPos_ddlPos", "2")
        page.wait_for_load_state("networkidle")
        time.sleep(3)

        # 2. Select "All Teams" (empty)
        print("Selecting All Teams...")
        # Since it might already be empty, selecting it again might not trigger response.
        # So we check the current value first.
        curr_team = page.evaluate(
            "document.querySelector('select#cphContents_cphContents_cphContents_ddlTeam_ddlTeam').value"
        )
        if curr_team != "":
            with page.expect_response("**/Record/Player/Defense/Basic.aspx", timeout=15000):
                page.select_option("select#cphContents_cphContents_cphContents_ddlTeam_ddlTeam", "")
            page.wait_for_load_state("networkidle")
            time.sleep(3)

        # Check rows
        rows = page.query_selector_all("table.tData01.tt tbody tr")
        print(f"Found {len(rows)} rows")
        for i, row in enumerate(rows[:10]):
            cells = row.query_selector_all("td")
            if len(cells) > 3:
                print(f"Row {i}: {cells[1].inner_text()} | POS: {cells[3].inner_text()}")

        browser.close()


if __name__ == "__main__":
    check_all_teams_catcher()
