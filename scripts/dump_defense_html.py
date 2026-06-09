import logging

from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)


def dump_defense_html():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx")
        page.wait_for_load_state("networkidle")

        # Print all <a> tags that might be tabs
        tabs = page.query_selector_all("ul.tab-tit li a")
        for tab in tabs:
            logger.info("Tab: %s -> %s", tab.inner_text(), tab.get_attribute('href'))

        # Print side menu links
        side_links = page.query_selector_all(".lnb a")
        for link in side_links:
            logger.info("Side Link: %s -> %s", link.inner_text(), link.get_attribute('href'))

        browser.close()


if __name__ == "__main__":
    dump_defense_html()
