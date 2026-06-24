"""
Async Playwright browser/page pool with optional resource blocking.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self

from playwright.async_api import Browser, BrowserContext, Page, async_playwright
from playwright.async_api import Error as PlaywrightError

from src.utils.playwright_blocking import install_async_resource_blocking

if TYPE_CHECKING:
    from types import TracebackType

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AsyncPlaywrightPoolOptions:
    max_pages: int = 1
    headless: bool = True
    browser_type: str = "chromium"
    context_kwargs: dict[str, Any] | None = None
    block_resources: bool = True
    timeout_ms: int | None = None
    requires_auth: bool = False


class AsyncPlaywrightPool:
    def __init__(self, options: AsyncPlaywrightPoolOptions | None = None, **overrides: object) -> None:
        if options is None:
            options = AsyncPlaywrightPoolOptions(**overrides)
        elif overrides:
            msg = "Pass either AsyncPlaywrightPoolOptions or keyword options, not both"
            raise TypeError(msg)

        self.max_pages = options.max_pages
        self.headless = options.headless
        self.browser_type = options.browser_type
        self.context_kwargs = options.context_kwargs or {}
        self.block_resources = options.block_resources
        self.timeout_ms = options.timeout_ms
        self.requires_auth = options.requires_auth

        self._playwright = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._queue: asyncio.Queue[Page] | None = None
        self._pages: list[Page] = []
        self._started = False

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def start(self) -> None:
        if self._started:
            return

        await self._prepare_auth_state()

        try:
            await self._start_browser_context()
            if self.block_resources and self._context:
                await install_async_resource_blocking(self._context)
            await self._create_pages()
            self._started = True
        except (PlaywrightError, RuntimeError, OSError):
            await self.close()
            raise

    async def _prepare_auth_state(self) -> None:
        from src.utils.kbo_auth import KboAuthenticator

        # Automated Authentication if required
        if not self.requires_auth:
            return
        if not KboAuthenticator.is_authenticated():
            logger.info("[POOL] Session missing. Triggering auto-login...")
            auth = KboAuthenticator()
            success = await auth.login(headless=self.headless)
            if not success:
                logger.info("[POOL] Warning: Auto-login failed. Proceeding without auth.")

        if KboAuthenticator.is_authenticated():
            logger.info("[POOL] Using saved session state.")
            self.context_kwargs["storage_state"] = KboAuthenticator.get_auth_state_path()

    async def _start_browser_context(self) -> None:
        self._playwright = await async_playwright().start()
        browser_factory = getattr(self._playwright, self.browser_type)

        # Add evasion arguments
        launch_args = [
            "--disable-blink-features=AutomationControlled",
        ]
        self._browser = await browser_factory.launch(headless=self.headless, args=launch_args)

        # Dynamic User-Agent Rotation
        if "user_agent" not in self.context_kwargs:
            from src.utils.request_policy import RequestPolicy

            policy = RequestPolicy()
            self.context_kwargs["user_agent"] = policy.random_user_agent()

        self._context = await self._browser.new_context(**self.context_kwargs)
        await self._context.add_init_script(self._stealth_script())

    @staticmethod
    def _stealth_script() -> str:
        return """
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

    async def _create_pages(self) -> None:
        if not self._context:
            msg = "Playwright context not initialized"
            raise RuntimeError(msg)
        self._queue = asyncio.Queue(maxsize=self.max_pages)
        for _ in range(self.max_pages):
            page = await self._context.new_page()
            if self.timeout_ms:
                page.set_default_timeout(self.timeout_ms)
            self._pages.append(page)
            await self._queue.put(page)

    async def acquire(self) -> Page:
        if not self._started:
            await self.start()
        if not self._queue:
            msg = "Playwright pool not initialized"
            raise RuntimeError(msg)
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
        for page in self._pages:
            with suppress(PlaywrightError, RuntimeError, OSError):
                await page.close()
        self._pages = []
        if self._context:
            with suppress(PlaywrightError, RuntimeError, OSError):
                await self._context.close()
        if self._browser:
            with suppress(PlaywrightError, RuntimeError, OSError):
                await self._browser.close()
        if self._playwright:
            with suppress(PlaywrightError, RuntimeError, OSError):
                await self._playwright.stop()
        self._started = False
        self._context = None
        self._browser = None
        self._playwright = None
        self._queue = None


__all__ = ["AsyncPlaywrightPool"]
