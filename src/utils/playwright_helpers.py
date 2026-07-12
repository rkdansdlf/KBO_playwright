"""유틸리티: playwright helpers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from src.utils.playwright_retry import NAV_TIMEOUT

if TYPE_CHECKING:
    from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)


def goto_next_page(page: Page, policy: RequestPolicy | None = None) -> bool:
    """Handle the goto next page operation.

    Args:
        page: Page.
        policy: Policy.
        page: Page.
        policy: Policy.
        page: Playwright page object.
        policy: Policy.

    Returns:
        True if the condition is met, False otherwise.

    """
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
                    page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
                    if policy:
                        policy.delay()
                    return True

    except (PlaywrightError, PlaywrightTimeoutError, RuntimeError):
        logger.exception("      ⚠️ 페이지 이동 중 오류")
        return False
    else:
        return False
