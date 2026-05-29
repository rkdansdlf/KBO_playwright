from playwright.sync_api import sync_playwright


def check_team_pos_catcher():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx")
        page.wait_for_load_state("networkidle")

        # Select Team "삼성" (SS)
        page.select_option("select#cphContents_cphContents_cphContents_ddlTeam_ddlTeam", "SS")
        page.wait_for_load_state("networkidle")

        # Select Position "포수" (2)
        page.select_option("select#cphContents_cphContents_cphContents_ddlPos_ddlPos", "2")
        page.wait_for_load_state("networkidle")

        headers = page.query_selector_all("table.tData01.tt thead th")
        print(f"Headers for SS Catcher: {len(headers)} columns")

        browser.close()


if __name__ == "__main__":
    check_team_pos_catcher()
