"""__init__.py 패키지."""

from __future__ import annotations

import logging
import os

from playwright.async_api import BrowserType as AsyncBrowserType
from playwright.sync_api import BrowserType as SyncBrowserType

logger = logging.getLogger(__name__)

# Keep references to original launch methods for fallback and test restoration
_original_async_launch = AsyncBrowserType.launch
_original_sync_launch = SyncBrowserType.launch


def _apply_playwright_patch() -> None:
    """Apply global monkeypatch to Playwright browser launches to support Browserless Chrome."""
    ws_endpoint = os.getenv("PLAYWRIGHT_WS_ENDPOINT")
    if not ws_endpoint:
        return

    try:

        async def _patched_async_launch(self: object, *args: object, **kwargs: object) -> object:
            endpoint = os.getenv("PLAYWRIGHT_WS_ENDPOINT")
            if endpoint:
                logger.info(
                    "[PLAYWRIGHT-PATCH] Redirecting async launch for %s to remote browser at %s",
                    getattr(self, "name", "browser"),
                    endpoint,
                )
                connect_keys = {"ws_endpoint", "headers", "timeout", "slow_mo"}
                connect_kwargs = {k: v for k, v in kwargs.items() if k in connect_keys}
                return await self.connect(endpoint, **connect_kwargs)  # type: ignore[attr-defined]
            return await _original_async_launch(self, *args, **kwargs)

        def _patched_sync_launch(self: object, *args: object, **kwargs: object) -> object:
            endpoint = os.getenv("PLAYWRIGHT_WS_ENDPOINT")
            if endpoint:
                logger.info(
                    "[PLAYWRIGHT-PATCH] Redirecting sync launch for %s to remote browser at %s",
                    getattr(self, "name", "browser"),
                    endpoint,
                )
                connect_keys = {"ws_endpoint", "headers", "timeout", "slow_mo"}
                connect_kwargs = {k: v for k, v in kwargs.items() if k in connect_keys}
                return self.connect(endpoint, **connect_kwargs)  # type: ignore[attr-defined]
            return _original_sync_launch(self, *args, **kwargs)

        # Apply monkeypatches
        AsyncBrowserType.launch = _patched_async_launch  # type: ignore[assignment]
        SyncBrowserType.launch = _patched_sync_launch  # type: ignore[assignment]
        logger.info("[PLAYWRIGHT-PATCH] Playwright launch methods globally patched.")
    except ImportError:
        pass


_apply_playwright_patch()
