from playwright.sync_api import sync_playwright


def test_catcher_url():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        urls = [
            "https://www.koreabaseball.com/Record/Player/Defense/Catcher.aspx",
            "https://www.koreabaseball.com/Record/Player/Defense/Detail.aspx",
        ]

        for url in urls:
            print(f"Testing URL: {url}")
            try:
                page.goto(url, timeout=10000)
                page.wait_for_load_state("networkidle")
                title = page.title()
                print(f"  Title: {title}")
                # Check for table headers
                headers = page.query_selector_all("th")
                header_texts = [h.inner_text().strip() for h in headers]
                print(f"  Headers: {header_texts[:15]}")
            except Exception as e:
                print(f"  Failed: {e}")
            print("-" * 20)

        browser.close()


if __name__ == "__main__":
    test_catcher_url()
