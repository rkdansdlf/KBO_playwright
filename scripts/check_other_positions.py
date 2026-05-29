from playwright.sync_api import sync_playwright


def check_other_positions():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx")
        page.wait_for_load_state("networkidle")

        pos_options = {"Catcher": "2", "Infield": "3,4,5,6", "Outfield": "7,8,9"}

        for name, val in pos_options.items():
            page.select_option("select#cphContents_cphContents_cphContents_ddlPos_ddlPos", val)
            page.wait_for_load_state("networkidle")
            headers = page.query_selector_all("table.tData01.tt thead th")
            header_texts = [h.inner_text().strip() for h in headers]
            print(f"Headers for {name}: {len(header_texts)} columns -> {header_texts}")

        browser.close()


if __name__ == "__main__":
    check_other_positions()
