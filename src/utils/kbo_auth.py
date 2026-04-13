"""
KBO Authentication Utility
Handles automated login and session persistence.
"""
import os
import asyncio
from pathlib import Path
from typing import Optional
from playwright.async_api import async_playwright, BrowserContext
from dotenv import load_dotenv

load_dotenv()

class KboAuthenticator:
    LOGIN_URL = "https://www.koreabaseball.com/Member/Login.aspx"
    AUTH_STATE_PATH = "data/kbo_auth_state.json"

    def __init__(self, user_id: Optional[str] = None, user_pwd: Optional[str] = None):
        self.user_id = user_id or os.getenv("KBO_USER_ID")
        self.user_pwd = user_pwd or os.getenv("KBO_USER_PWD")

    async def login(self, headless: bool = True) -> bool:
        """Perform login and save state to file."""
        if not self.user_id or not self.user_pwd:
            print("[AUTH] Error: KBO_USER_ID or KBO_USER_PWD not set.")
            return False

        print(f"[AUTH] Attempting login for user: {self.user_id}...")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context()
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
                    print("[AUTH] Login successful! Warming up session...")
                    
                    # Navigate to a GameCenter related page once to "activate" member cookies fully for that section
                    # Using a generic GameCenter entry point
                    try:
                        await page.goto("https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx", wait_until="networkidle", timeout=10000)
                    except Exception as e:
                        print(f"[AUTH] Session warm-up warning (ignoring): {e}")

                    # Save state
                    os.makedirs(os.path.dirname(self.AUTH_STATE_PATH), exist_ok=True)
                    await context.storage_state(path=self.AUTH_STATE_PATH)
                    return True
                else:
                    print("[AUTH] Login failed: Logout button not found after redirection.")
                    return False
                    
            except Exception as e:
                print(f"[AUTH] Exception during login: {e}")
                return False
            finally:
                await browser.close()

    @classmethod
    def is_authenticated(cls) -> bool:
        """Check if auth state file exists."""
        return os.path.exists(cls.AUTH_STATE_PATH)

    @classmethod
    def get_auth_state_path(cls) -> str:
        return cls.AUTH_STATE_PATH

async def main():
    # Simple CLI tool to refresh login
    auth = KboAuthenticator()
    success = await auth.login(headless=True)
    if success:
        print("✨ Auth state saved successfully.")
    else:
        print("❌ Auth failed.")

if __name__ == "__main__":
    asyncio.run(main())
