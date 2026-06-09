"""Manual prototype for the 2000 legacy GameCenter parser.

This is a parser investigation script, not an operational collection path. It
does not save to the database.
"""

import logging

logger = logging.getLogger(__name__)

import os
import sys

from playwright.sync_api import sync_playwright

sys.path.insert(0, os.getcwd())

from src.crawlers.legacy_game_detail_crawler import LegacyGameDetailCrawler


def run_prototype():
    print(
        "[DEBUG] scripts/maintenance/prototype_2000_crawler.py performs a live legacy parser probe only. "
        "It does not persist data."
    )
    # Example game from 2000 season Opening Day (April 5, 2000)
    # Samsung (SS) vs Lotte (LT) at Busan
    game_id = "20000405SSLT0"
    game_date = "2000-04-05"

    # KBO Box Score URL pattern
    url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={game_id}"

    logger.info(f"🚀 Testing extraction for Game: {game_id}")
    logger.info(f"🔗 URL: {url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            page.goto(url, wait_until="networkidle")
            logger.info(f"✅ Page loaded. Current URL: {page.url}")

            # Save screenshot for debugging
            page.screenshot(path="debug_2000_main.png")
            logger.info("📸 Screenshot saved to debug_2000_main.png")

            # Try to find 'Box Score' or '리뷰' tab
            try:
                page.wait_for_selector("#liBoxScore", timeout=5000)
                page.click("#liBoxScore")
                logger.info("✅ Clicked #liBoxScore")
            except Exception:  # noqa: BLE001
                logger.warning("⚠️ #liBoxScore not found. Trying direct BoxScore URL...")
                direct_url = f"https://www.koreabaseball.com/Schedule/GameCenter/BoxScore.aspx?gameId={game_id}"
                page.goto(direct_url, wait_until="networkidle")
                logger.info(f"🔗 Navigated to direct URL: {page.url}")
                page.screenshot(path="debug_2000_boxscore.png")

            # Wait for stats tables to load
            logger.info("⏳ Waiting for statistics tables...")
            page.wait_for_selector("#tblAwayHitter1", timeout=10000)

            crawler = LegacyGameDetailCrawler()
            details = crawler.extract_game_details(page, game_id, game_date)

            logger.info("\n" + "=" * 50)
            logger.info(f"📊 Extracted Details for {game_id}")
            logger.info(f"Stadium: {details['metadata'].get('stadium')}")
            logger.info(f"Away Team: {details['away_team_code']}")
            logger.info(f"Home Team: {details['home_team_code']}")

            logger.info("\n🏏 Away Hitters (Top 3):")
            for h in details["hitters"]["away"][:3]:
                logger.info(f"  - {h['player_name']} ({h['position']}): {h['stats']}")

            logger.info("\n🏏 Home Hitters (Top 3):")
            for h in details["hitters"]["home"][:3]:
                logger.info(f"  - {h['player_name']} ({h['position']}): {h['stats']}")

            logger.info("\n⚾ Away Pitchers:")
            for p_stat in details["pitchers"]["away"]:
                logger.info(f"  - {p_stat['player_name']}: {p_stat['stats']}")

            logger.info("\n⚾ Home Pitchers:")
            for p_stat in details["pitchers"]["home"]:
                logger.info(f"  - {p_stat['player_name']}: {p_stat['stats']}")

            logger.info("=" * 50)

        except Exception as e:  # noqa: BLE001
            logger.error(f"❌ Error during prototype run: {e}")
            import traceback

            traceback.print_exc()
        finally:
            browser.close()


if __name__ == "__main__":
    run_prototype()
