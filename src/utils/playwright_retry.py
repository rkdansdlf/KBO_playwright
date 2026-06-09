"""Playwright navigation and selector retry utilities."""

from __future__ import annotations

import contextlib
import logging
import os

from playwright.sync_api import Page
from playwright.sync_api import TimeoutError as PlaywrightTimeout

from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)

_policy = RequestPolicy()

# Environment-variable defaults (set in .env, evaluated once at import)
_SELECTOR_TIMEOUT = int(os.getenv("KBO_PLAYWRIGHT_SELECTOR_TIMEOUT", "15000"))
_NAVIGATION_TIMEOUT = int(os.getenv("KBO_PLAYWRIGHT_NAVIGATION_TIMEOUT", "30000"))
_CLICK_TIMEOUT = int(os.getenv("KBO_PLAYWRIGHT_CLICK_TIMEOUT", "15000"))
_MAX_RETRIES = int(os.getenv("KBO_PLAYWRIGHT_MAX_RETRIES", "3"))


def retry_navigation(
    page: Page,
    url: str,
    max_retries: int = _MAX_RETRIES,
    timeout: int = _NAVIGATION_TIMEOUT,
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
            _policy.delay()
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error navigating to {url} on attempt {attempt}: {e}")
            if attempt == max_retries:
                return False
            _policy.delay()
    return False


def retry_click(
    page: Page,
    selector: str,
    max_retries: int = _MAX_RETRIES,
    timeout: int = _CLICK_TIMEOUT,
    pre_wait_timeout: int = _SELECTOR_TIMEOUT,
) -> bool:
    """Retry page.click with wait_for_selector pre-check, reloading on timeout."""
    for attempt in range(1, max_retries + 1):
        try:
            page.wait_for_selector(selector, timeout=pre_wait_timeout, state="visible")
            page.click(selector, timeout=timeout)
            return True
        except PlaywrightTimeout:
            if attempt == max_retries:
                return False
            logger.warning(f"Click on {selector} timed out on attempt {attempt}, retrying...")
            with contextlib.suppress(Exception):
                page.reload(wait_until="networkidle", timeout=timeout)
            _policy.delay()
    return False


def retry_wait_for_selector(
    page: Page,
    selector: str,
    max_retries: int = _MAX_RETRIES,
    timeout: int = _SELECTOR_TIMEOUT,
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
            with contextlib.suppress(Exception):
                page.reload(wait_until="networkidle", timeout=timeout)
            _policy.delay()
    return False
