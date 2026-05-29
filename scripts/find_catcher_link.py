from playwright.sync_api import sync_playwright


def find_catcher_link():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx")
        page.wait_for_load_state("networkidle")

        # 1. Search for text "도루저지" (Caught Stealing)
        elements = page.query_selector_all("text='도루저지'")
        for el in elements:
            print(f"Found '도루저지' in: {el.tag_name()} (text: {el.inner_text()})")

        # 2. Look at the sub-menu or tabs specifically
        menu_items = page.query_selector_all(".sub-menu li, .tab-area li, .lnb li")
        for item in menu_items:
            print(f"Menu Item: {item.inner_text()}")

        # 3. Check for specific selectors like 'Catcher'
        if page.query_selector("a:has-text('포수')"):
            print("Found '포수' (Catcher) link")

        browser.close()


if __name__ == "__main__":
    find_catcher_link()
