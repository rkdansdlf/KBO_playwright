import logging

logger = logging.getLogger(__name__)

from playwright.sync_api import Page

from src.utils.request_policy import RequestPolicy


def goto_next_page(page: Page, policy: RequestPolicy | None = None) -> bool:
    try:
        pagination = page.query_selector(".paging")
        if not pagination:
            return False

        next_links = pagination.query_selector_all("a")
        for link in next_links:
            text = link.inner_text().strip()
            if "다음" in text or ">" in text:
                href = link.get_attribute("href")
                if href and "javascript:" not in href:
                    link.click()
                    page.wait_for_load_state("networkidle", timeout=30000)
                    if policy:
                        policy.delay()
                    return True

        return False

    except Exception:
        logger.exception("      ⚠️ 페이지 이동 중 오류")
        return False
