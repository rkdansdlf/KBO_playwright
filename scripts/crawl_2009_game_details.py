"""One-off legacy 2009 detail extractor.

This script intentionally remains outside the shared async
`game_collection_service` because it drives a sync Playwright page and the
legacy 2009-only parser directly. Do not use it as an operational collection
entry point; use the standard CLIs for modern schedule/detail collection.
"""

import logging
import sys
import time
import urllib.parse
from pathlib import Path

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path.cwd()))
from src.crawlers.legacy_game_detail_crawler import LegacyGameDetailCrawler

from src.db.engine import SessionLocal
from src.repositories.game_repository import save_game_detail
from src.services.player_id_resolver import PlayerIdResolver

LEGACY_DETAIL_EXCEPTIONS = (PlaywrightError, TimeoutError, RuntimeError, ValueError, TypeError, OSError)


def crawl_2009_details():
    logger.info(
        "[LEGACY] scripts/crawl_2009_game_details.py is a manual 2009 repair/debug path. "
        "Operational detail collection should use src.cli.collect_games or src.cli.run_daily_update.",
    )

    # DB Session
    session = SessionLocal()
    resolver = PlayerIdResolver(session)
    resolver.preload_season_index(2009)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # 1. Navigate to 2009 Schedule
        url = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
        logger.info("📡 Navigating to Schedule: %s", url)
        page.goto(url, wait_until="networkidle")

        # Select 2009, 04, Regular
        logger.info("   Selecting Year 2009...")
        page.select_option("#ddlYear", "2009")
        time.sleep(1)
        logger.info("   Selecting Month 04...")
        page.select_option("#ddlMonth", "04")
        time.sleep(1)
        logger.info("   Selecting Series...")
        sys.stdout.flush()
        try:
            page.select_option("#ddlSeries", "0,9,6")
        except LEGACY_DETAIL_EXCEPTIONS as e:
            logger.warning("⚠️ Error selecting series: %s", e)
        time.sleep(2)
        logger.info("   Series selected.")
        sys.stdout.flush()

        logger.info("   Locating table...")
        tbl = page.query_selector(".tbl-type06")
        if not tbl:
            logger.error("❌ Table not found!")
            return

        logger.info("   Finding links...")
        links = tbl.query_selector_all("tbody a")
        review_links = [link for link in links if "리뷰" in link.inner_text()]

        logger.info("Found %s review links.", len(review_links))

        for i, link in enumerate(review_links[:1]):
            try:
                logger.info("   Processing Game %s...", i + 1)
                href = link.get_attribute("href")
                logger.info("   Link: %s", href)

                # Navigation
                try:
                    logger.info("   [Driver] Navigating to URL directly...")
                    full_url = f"https://www.koreabaseball.com{href}"
                    page.goto(full_url, wait_until="networkidle", timeout=30000)
                    logger.info("   [Driver] Navigation done.")
                except LEGACY_DETAIL_EXCEPTIONS as e:
                    logger.warning("⚠️ [Driver] Navigation failed: %s", e)

                # Extract Data
                logger.info("   [Driver] Instantiating Crawler...")
                crawler = LegacyGameDetailCrawler(resolver=resolver)

                # Derive Game ID from href
                parsed = urllib.parse.urlparse(href)
                qs = urllib.parse.parse_qs(parsed.query)
                # gameId=20090404HHSK0
                game_id = qs.get("gameId", [f"20090404_TEST_{i}"])[0]
                game_date = qs.get("gameDate", ["20090404"])[0]

                logger.info("   [Driver] Extracting details for %s...", game_id)
                try:
                    data = crawler.extract_game_details(page, game_id, game_date)
                    logger.info("   [Driver] Extraction done.")
                except LEGACY_DETAIL_EXCEPTIONS:
                    logger.exception("🔥 [Driver] Extraction CRASHED")
                    import traceback

                    traceback.print_exc()
                    raise

                # Save to DB
                logger.info("   [Driver] Saving to DB...")
                saved = save_game_detail(data)
                if saved:
                    logger.info("   ✅ Game %s saved successfully!", game_id)
                else:
                    logger.warning("   ❌ Failed to save game %s.", game_id)

                logger.info("📊 Extracted Data Structure:")
                logger.info("  Game ID: %s", data["game_id"])
                logger.info("  Teams: %s", data["teams"])

                # Go back for next game
                logger.info("   [Driver] Going back...")
                page.go_back()
                time.sleep(2)

            except Exception:
                logger.exception("🔥 [CRITICAL] Loop iteration failed")
                import traceback

                traceback.print_exc()

            logger.info("   [Driver] End of loop iteration.")

        browser.close()
    session.close()


if __name__ == "__main__":
    crawl_2009_details()
