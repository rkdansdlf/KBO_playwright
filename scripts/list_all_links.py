from playwright.sync_api import sync_playwright


def list_all_links():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx")
        page.wait_for_load_state("networkidle")

        links = page.query_selector_all("a")
        for link in links:
            txt = link.inner_text().strip()
            href = link.get_attribute("href")
            if txt and href:
                print(f"{txt} | {href}")

        browser.close()


if __name__ == "__main__":
    list_all_links()
