"""
Async Playwright browser/page pool with optional resource blocking.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional, List

from playwright.async_api import async_playwright, Browser, BrowserContext, Page

from src.utils.playwright_blocking import install_async_resource_blocking


class AsyncPlaywrightPool:
    def __init__(
        self,
        *,
        max_pages: int = 1,
        headless: bool = True,
        browser_type: str = "chromium",
        context_kwargs: Optional[Dict[str, Any]] = None,
        block_resources: bool = True,
        timeout_ms: Optional[int] = None,
        requires_auth: bool = False,
    ) -> None:
        self.max_pages = max_pages
        self.headless = headless
        self.browser_type = browser_type
        self.context_kwargs = context_kwargs or {}
        self.block_resources = block_resources
        self.timeout_ms = timeout_ms
        self.requires_auth = requires_auth

        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._queue: Optional[asyncio.Queue[Page]] = None
        self._pages: List[Page] = []
        self._started = False

    async def __aenter__(self) -> "AsyncPlaywrightPool":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def start(self) -> None:
        if self._started:
            return

        from src.utils.kbo_auth import KboAuthenticator
        
        # Automated Authentication if required
        if self.requires_auth:
            if not KboAuthenticator.is_authenticated():
                print("[POOL] Session missing. Triggering auto-login...")
                auth = KboAuthenticator()
                success = await auth.login(headless=self.headless)
                if not success:
                    print("[POOL] Warning: Auto-login failed. Proceeding without auth.")
            
            if KboAuthenticator.is_authenticated():
                print("[POOL] Using saved session state.")
                self.context_kwargs["storage_state"] = KboAuthenticator.get_auth_state_path()

        self._playwright = await async_playwright().start()
        browser_factory = getattr(self._playwright, self.browser_type)
        
        # Add evasion arguments
        launch_args = [
            "--disable-blink-features=AutomationControlled",
        ]
        self._browser = await browser_factory.launch(headless=self.headless, args=launch_args)
        
        # Define realistic User-Agent if not provided
        if "user_agent" not in self.context_kwargs:
            self.context_kwargs["user_agent"] = (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            )

        self._context = await self._browser.new_context(**self.context_kwargs)
        
        # Inject Stealth Script
        stealth_script = """
        () => {
            // 1. Mask navigator.webdriver
            Object.defineProperty(navigator, 'webdriver', { get: () => false });

            // 2. Mock chrome.runtime
            window.chrome = { runtime: {} };

            // 3. Fix navigator.languages
            Object.defineProperty(navigator, 'languages', { get: () => ['ko-KR', 'ko', 'en-US', 'en'] });

            // 4. Mock WebGL vendor/renderer for high-end look
            const getParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {
                if (parameter === 37445) return 'Intel Inc.';
                if (parameter === 37446) return 'Intel(R) Iris(TM) Plus Graphics 640';
                return getParameter.apply(this, arguments);
            };
            
            // 5. Hide direct automation clues
            delete navigator.__proto__.webdriver;
        }
        """
        await self._context.add_init_script(stealth_script)

        if self.block_resources:
            await install_async_resource_blocking(self._context)

        self._queue = asyncio.Queue(maxsize=self.max_pages)
        for _ in range(self.max_pages):
            page = await self._context.new_page()
            if self.timeout_ms:
                page.set_default_timeout(self.timeout_ms)
            self._pages.append(page)
            await self._queue.put(page)
        self._started = True

    async def acquire(self) -> Page:
        if not self._started:
            await self.start()
        if not self._queue:
            raise RuntimeError("Playwright pool not initialized")
        return await self._queue.get()

    async def release(self, page: Page) -> None:
        if not self._queue:
            return
        if page.is_closed() and self._context:
            page = await self._context.new_page()
            if self.timeout_ms:
                page.set_default_timeout(self.timeout_ms)
            self._pages.append(page)
        await self._queue.put(page)

    @asynccontextmanager
    async def page(self) -> Page:
        page = await self.acquire()
        try:
            yield page
        finally:
            await self.release(page)

    async def close(self) -> None:
        if not self._started:
            return
        for page in self._pages:
            try:
                await page.close()
            except Exception:
                pass
        self._pages = []
        if self._context:
            try:
                await self._context.close()
            except Exception:
                pass
        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass
        self._started = False
        self._context = None
        self._browser = None
        self._playwright = None
        self._queue = None


__all__ = ["AsyncPlaywrightPool"]
