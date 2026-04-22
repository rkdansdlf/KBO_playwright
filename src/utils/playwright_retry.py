"""Playwright navigation and selector retry utilities."""
from __future__ import annotations

import logging
import time

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeout

logger = logging.getLogger(__name__)


def retry_navigation(
    page: Page,
    url: str,
    max_retries: int = 3,
    timeout: int = 30000,
    wait_until: str = "load",
) -> bool:
    """Retry page.goto with simple incremental backoff."""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Navigating to {url} (Attempt {attempt}/{max_retries})")
            page.goto(url, wait_until=wait_until, timeout=timeout)
            page.wait_for_load_state("networkidle", timeout=timeout)
            return True
        except PlaywrightTimeout:
            logger.warning(f"Timeout navigating to {url} on attempt {attempt}")
            if attempt == max_retries:
                return False
            time.sleep(attempt * 2)
        except Exception as e:
            logger.error(f"Error navigating to {url} on attempt {attempt}: {e}")
            if attempt == max_retries:
                return False
            time.sleep(attempt * 2)
    return False


def retry_wait_for_selector(
    page: Page,
    selector: str,
    max_retries: int = 2,
    timeout: int = 15000,
    state: str = "visible",
) -> bool:
    """Retry wait_for_selector, reloading between timeout attempts."""
    for attempt in range(1, max_retries + 1):
        try:
            page.wait_for_selector(selector, timeout=timeout, state=state)
            return True
        except PlaywrightTimeout:
            if attempt == max_retries:
                return False
            logger.warning(f"Selector {selector} not found on attempt {attempt}, retrying...")
            # Try reloading if it's a transient issue
            try:
                page.reload(wait_until="networkidle", timeout=timeout)
            except Exception:
                pass
            time.sleep(2)
    return False
