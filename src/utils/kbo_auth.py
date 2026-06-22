"""
KBO Authentication Utility
Handles automated login and session persistence.
"""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import async_playwright

from src.urls import GAME_CENTER
from src.utils.playwright_retry import SEL_TIMEOUT

logger = logging.getLogger(__name__)
load_dotenv()


class KboAuthenticator:
    LOGIN_URL = "https://www.koreabaseball.com/Member/Login.aspx"
    AUTH_STATE_PATH = "data/kbo_auth_state.json"

    def __init__(self, user_id: str | None = None, user_pwd: str | None = None) -> None:
        self.user_id = user_id or os.getenv("KBO_USER_ID")
        self.user_pwd = user_pwd or os.getenv("KBO_USER_PWD")

    async def login(self, *, headless: bool = True) -> bool:
        """Perform login and save state to file."""
        if not self.user_id or not self.user_pwd:
            logger.info("[AUTH] Error: KBO_USER_ID or KBO_USER_PWD not set.")
            return False

        logger.info("[AUTH] Attempting login for user: %s...", self.user_id)

        async with async_playwright() as p:
            # Replicate stealth launch args
            launch_args = ["--disable-blink-features=AutomationControlled"]
            browser = await p.chromium.launch(headless=headless, args=launch_args)

            # Use realistic User-Agent
            user_agent = (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            )
            context = await browser.new_context(user_agent=user_agent)

            # Add basic stealth init script
            await context.add_init_script(
                "() => { Object.defineProperty(navigator, 'webdriver', { get: () => false }); }",
            )

            page = await context.new_page()

            try:
                await page.goto(self.LOGIN_URL, wait_until="networkidle")

                # Fill login form
                await page.fill("#cphContents_cphContents_cphContents_txtUserId", self.user_id)
                await page.fill("#cphContents_cphContents_cphContents_txtPassWord", self.user_pwd)

                # Click login button
                await page.click("#cphContents_cphContents_cphContents_btnLogin")

                # Wait for navigation or success indicator
                await page.wait_for_load_state("networkidle")

                # Check if logged in (usually header text changes to "로그아웃")
                content = await page.content()
                if "로그아웃" in content:
                    logger.info("[AUTH] Login successful! Warming up session...")

                    # 1. Natural navigation to GameCenter
                    try:
                        await page.goto(
                            GAME_CENTER,
                            wait_until="networkidle",
                            timeout=SEL_TIMEOUT,
                        )

                        # 2. Mimic human behavior (Scroll)
                        await page.evaluate("window.scrollTo(0, 500)")
                        await asyncio.sleep(1)
                        await page.evaluate("window.scrollTo(0, 0)")
                        await asyncio.sleep(2)  # Wait for Akamai to finalize _abck cookie
                    except (PlaywrightError, TimeoutError, OSError) as e:
                        logger.warning("[AUTH] Session warm-up warning (ignoring): %s", e)

                    # Save state
                    Path(self.AUTH_STATE_PATH).parent.mkdir(parents=True, exist_ok=True)
                    await context.storage_state(path=self.AUTH_STATE_PATH)
                    return True
            except (PlaywrightError, TimeoutError, OSError):
                logger.exception("[AUTH] Exception during login")
                return False
            else:
                logger.info("[AUTH] Login failed: Logout button not found after redirection.")
                return False
            finally:
                await browser.close()

    @classmethod
    def is_authenticated(cls) -> bool:
        """Check if auth state file exists."""
        return Path(cls.AUTH_STATE_PATH).exists()

    @classmethod
    def get_auth_state_path(cls) -> str:
        return cls.AUTH_STATE_PATH


async def main() -> None:
    # Simple CLI tool to refresh login
    auth = KboAuthenticator()
    success = await auth.login(headless=True)
    if success:
        logger.info("✨ Auth state saved successfully.")
    else:
        logger.error("❌ Auth failed.")


if __name__ == "__main__":
    asyncio.run(main())
