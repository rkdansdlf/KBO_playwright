from playwright.sync_api import sync_playwright


def check_catcher_columns():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx")
        page.wait_for_load_state("networkidle")

        # Select "포수" (2) in Position dropdown
        page.select_option("select#cphContents_cphContents_cphContents_ddlPos_ddlPos", "2")
        page.wait_for_load_state("networkidle")

        # Check table headers
        headers = page.query_selector_all("table.tData01.tt thead th")
        header_texts = [h.inner_text().strip() for h in headers]
        print(f"Headers for Catcher: {header_texts}")

        # Check if there are other tabs
        tabs = page.query_selector_all("ul.tab-tit li a")
        for tab in tabs:
            print(f"Tab: {tab.inner_text()} -> {tab.get_attribute('href')}")

        browser.close()


if __name__ == "__main__":
    check_catcher_columns()
