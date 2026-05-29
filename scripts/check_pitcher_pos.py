from playwright.sync_api import sync_playwright


def check_pitcher_pos():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx")
        page.wait_for_load_state("networkidle")

        try:
            page.select_option("select#cphContents_cphContents_cphContents_ddlPos_ddlPos", "1")
            page.wait_for_load_state("networkidle")
            headers = page.query_selector_all("table.tData01.tt thead th")
            header_texts = [h.inner_text().strip() for h in headers]
            print(f"Headers for Pitcher (val=1): {len(header_texts)} columns -> {header_texts}")
        except Exception as e:
            print(f"Failed to select val=1: {e}")

        browser.close()


if __name__ == "__main__":
    check_pitcher_pos()
