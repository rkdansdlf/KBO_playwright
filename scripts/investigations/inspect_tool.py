#!/usr/bin/env python3
"""Unified KBO inspection tool.

Provides subcommands to query local SQLite database, inspect KBO GameCenter pages
via Playwright, or inspect hitter/pitcher detail pages.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# Try importing pandas, default to simple print if not available
try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    from playwright.async_api import Error as PlaywrightError
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
    from playwright.async_api import async_playwright

    HAS_PLAYWRIGHT = True
except ImportError:
    PlaywrightError = RuntimeError
    PlaywrightTimeoutError = TimeoutError
    HAS_PLAYWRIGHT = False

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "kbo_dev.db"
DB_QUERY_EXCEPTIONS = (sqlite3.DatabaseError, RuntimeError, ValueError, TypeError)
PLAYWRIGHT_EXCEPTIONS = (PlaywrightError, PlaywrightTimeoutError, RuntimeError, ValueError, TypeError, OSError)


def run_db_query(db_path: Path, query: str) -> None:
    """Run raw SQL query on local SQLite database."""
    logger.info(f"Connecting to SQLite DB at {db_path}...")
    if not db_path.exists():
        logger.info(f"ERROR: DB path {db_path} does not exist.")
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    try:
        if HAS_PANDAS:
            df = pd.read_sql_query(query, conn)
            logger.info(df.to_string(index=False))
        else:
            cursor = conn.cursor()
            cursor.execute(query)
            cols = [desc[0] for desc in cursor.description]
            logger.info(" | ".join(cols))
            logger.info("-" * (len(" | ".join(cols))))
            for row in cursor.fetchall():
                logger.info(" | ".join(str(val) for val in row))
    except DB_QUERY_EXCEPTIONS as e:
        logger.error(f"Query Execution Error: {e}")
    finally:
        conn.close()


def db_inspect_game(db_path: Path, game_id: str) -> None:
    """Inspect pitching and batting stats for a specific game ID."""
    logger.info(f"\n--- Game Batting Stats for {game_id} ---")
    batting_query = f"""
        SELECT player_name, plate_appearances, at_bats, runs, hits, rbi, walks, strikeouts
        FROM game_batting_stats
        WHERE game_id = '{game_id}'
    """
    run_db_query(db_path, batting_query)

    logger.info(f"\n--- Game Pitching Stats for {game_id} ---")
    pitching_query = f"""
        SELECT player_name, wins, losses, saves, holds, innings_pitched, decision
        FROM game_pitching_stats
        WHERE game_id = '{game_id}'
    """
    run_db_query(db_path, pitching_query)


def db_inspect_player(db_path: Path, player_id: int, season: str) -> None:
    """Inspect pitching and batting stats for a specific player ID and season."""
    logger.info(f"\n--- Game Batting Details for Player {player_id} in {season} ---")
    batting_query = f"""
        SELECT g.game_id, g.game_date, gb.plate_appearances, gb.at_bats, gb.runs, gb.hits, gb.rbi, gb.walks, gb.strikeouts
        FROM game_batting_stats gb
        JOIN game g ON gb.game_id = g.game_id
        WHERE gb.player_id = {player_id} AND SUBSTR(gb.game_id, 1, 4) = '{season}'
        ORDER BY g.game_date
    """
    run_db_query(db_path, batting_query)

    logger.info(f"\n--- Game Pitching Details for Player {player_id} in {season} ---")
    pitching_query = f"""
        SELECT g.game_id, g.game_date, gp.wins, gp.losses, gp.saves, gp.holds, gp.innings_pitched, gp.appearance_seq
        FROM game_pitching_stats gp
        JOIN game g ON gp.game_id = g.game_id
        WHERE gp.player_id = {player_id} AND SUBSTR(gp.game_id, 1, 4) = '{season}'
        ORDER BY g.game_date
    """
    run_db_query(db_path, pitching_query)


def run_db_summary(db_path: Path) -> None:
    """Print the count of rows for each table in SQLite."""
    logger.info("📊 Local DB Summary:")
    if not db_path.exists():
        logger.info(f"Local DB not found at {db_path}")
        return

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    def get_count(table_name):
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cur.fetchone()[0]
        except sqlite3.DatabaseError:
            return "MISSING"

    logger.info("\n=== Player Data ===")
    logger.info(f"  player_basic: {get_count('player_basic')}")
    logger.info(f"  player_season_batting: {get_count('player_season_batting')}")
    logger.info(f"  player_season_pitching: {get_count('player_season_pitching')}")

    logger.info("\n=== Game Data ===")
    logger.info(f"  games: {get_count('game')}")
    logger.info(f"  game_metadata: {get_count('game_metadata')}")
    logger.info(f"  game_inning_scores: {get_count('game_inning_scores')}")
    logger.info(f"  game_lineups: {get_count('game_lineups')}")
    logger.info(f"  game_batting_stats: {get_count('game_batting_stats')}")
    logger.info(f"  game_pitching_stats: {get_count('game_pitching_stats')}")
    logger.info(f"  game_summary: {get_count('game_summary')}")

    logger.info("\n=== Other Data ===")
    logger.info(f"  teams: {get_count('teams')}")
    logger.info(f"  kbo_seasons: {get_count('kbo_seasons')}")
    logger.info(f"  awards: {get_count('awards')}")

    conn.close()
    logger.info("\n✅ Local summary complete")


def run_oci_summary() -> None:
    """Print the count of rows for each table in OCI PostgreSQL."""
    try:
        from dotenv import load_dotenv
        from sqlalchemy import create_engine, text
        from sqlalchemy.exc import SQLAlchemyError
    except ImportError:
        logger.info("ERROR: sqlalchemy and python-dotenv are required for OCI summary.")
        sys.exit(1)

    load_dotenv()
    db_url = os.getenv("OCI_DB_URL")
    if not db_url:
        logger.info("ERROR: OCI_DB_URL environment variable is not set.")
        sys.exit(1)

    logger.info("Connecting to OCI PostgreSQL...")
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            logger.info("📊 OCI Database Summary:")

            def get_count(table_name):
                try:
                    return conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()[0]
                except SQLAlchemyError as e:
                    return f"ERROR ({e})"

            logger.info("\n=== Player Data ===")
            logger.info(f"  player_basic: {get_count('player_basic')}")
            logger.info(f"  player_season_batting: {get_count('player_season_batting')}")
            logger.info(f"  player_season_pitching: {get_count('player_season_pitching')}")

            # Additional 2001 check from check_oci_summary.py
            logger.info(
                f"  2001 Batting: {conn.execute(text('SELECT COUNT(*) FROM player_season_batting WHERE season=2001')).fetchone()[0]}"
            )
            logger.info(
                f"  2001 Pitching: {conn.execute(text('SELECT COUNT(*) FROM player_season_pitching WHERE season=2001')).fetchone()[0]}"
            )

            logger.info("\n=== Game Data ===")
            logger.info(f"  games: {get_count('game')}")
            logger.info(f"  game_metadata: {get_count('game_metadata')}")
            logger.info(f"  game_inning_scores: {get_count('game_inning_scores')}")
            logger.info(f"  game_lineups: {get_count('game_lineups')}")
            logger.info(f"  game_batting_stats: {get_count('game_batting_stats')}")
            logger.info(f"  game_pitching_stats: {get_count('game_pitching_stats')}")
            logger.info(f"  game_summary: {get_count('game_summary')}")

            logger.info("\n=== Other Data ===")
            logger.info(f"  teams: {get_count('teams')}")
            logger.info(f"  kbo_seasons: {get_count('kbo_seasons')}")
            logger.info(f"  awards: {get_count('awards')}")

            logger.info("\n=== Game Years Distribution ===")
            try:
                years = conn.execute(
                    text(
                        "SELECT substr(game_id, 1, 4) as year, COUNT(*) as cnt FROM game GROUP BY substr(game_id, 1, 4) ORDER BY year DESC LIMIT 10"
                    )
                ).fetchall()
                for year, cnt in years:
                    logger.info(f"  {year}: {cnt} games")
            except SQLAlchemyError as e:
                logger.error(f"  Error loading distribution: {e}")

            logger.info("\n✅ OCI summary complete")
    except SQLAlchemyError as e:
        logger.info(f"ERROR connecting to OCI Database: {e}")


async def inspect_gamecenter(
    date: str, game_id: str, section: str, headless: bool, screenshot_path: str | None
) -> None:
    """Scrape KBO GameCenter page and check for tables and text content."""
    if not HAS_PLAYWRIGHT:
        logger.info("ERROR: playwright package is not installed.")
        sys.exit(1)

    url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={date}&gameId={game_id}&section={section}"
    logger.info(f"Navigating to GameCenter: {url}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()

        try:
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await page.wait_for_timeout(5000)

            tables_data = await page.evaluate("""() => {
                const results = [];
                const tables = document.querySelectorAll('table');
                tables.forEach((table, index) => {
                    const id = table.getAttribute('id') || 'No ID';
                    const className = table.className || 'No Class';
                    const caption = table.querySelector('caption') ? table.querySelector('caption').innerText.trim() : 'No Caption';
                    let allText = table.innerText.trim();
                    results.push({
                        index,
                        id,
                        className,
                        caption,
                        allTextSnippet: allText.substring(0, 500)
                    });
                });
                return results;
            }""")

            logger.info(f"Found {len(tables_data)} tables.")
            for table in tables_data:
                logger.info(
                    f"\nTable [{table['index']}] ID: {table['id']}, Class: {table['className']}, Caption: {table['caption']}"
                )
                snippet = table["allTextSnippet"].replace("\n", " ")
                logger.info(f"  Snippet: {snippet[:200]}...")
                if any(k in table["allTextSnippet"] for k in ["홈런", "볼넷", "4사구", "안타", "삼진"]):
                    logger.info("  *** MATCH FOUND (HR/BB/H/SO keywords exist in table) ***")

            # Check for detail links
            detail_links = await page.query_selector_all("a")
            detail_texts = []
            for link in detail_links:
                text = (await link.inner_text()).strip()
                if "상세보기" in text:
                    detail_texts.append(text)
            if detail_texts:
                logger.info(f"\nFound Detail Links: {detail_texts}")

            if screenshot_path:
                await page.screenshot(path=screenshot_path)
                logger.info(f"Screenshot saved to {screenshot_path}")

        except PLAYWRIGHT_EXCEPTIONS as e:
            logger.error(f"Error during GameCenter inspection: {e}")
        finally:
            await browser.close()


async def inspect_player_profile(
    player_id: str,
    player_type: str,
    page_type: str,
    year: str | None,
    click_tab: str | None,
    headless: bool,
    screenshot_path: str | None,
) -> None:
    """Scrape a player's hitter or pitcher page."""

    if not HAS_PLAYWRIGHT:
        logger.info("ERROR: playwright package is not installed.")
        sys.exit(1)

    # Resolve URL path
    type_cap = "Hitter" if player_type.lower() == "hitter" else "Pitcher"
    page_cap = "Game"
    if page_type.lower() == "basic":
        page_cap = "Basic"
    elif page_type.lower() == "daily":
        page_cap = "Daily"

    url = f"https://www.koreabaseball.com/Record/Player/{type_cap}Detail/{page_cap}.aspx?playerId={player_id}"
    logger.info(f"Navigating to Player Profile: {url}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(viewport={"width": 1280, "height": 1024})
        page = await context.new_page()

        try:
            await page.goto(url, wait_until="networkidle")

            # Handle click_tab
            if click_tab:
                logger.info(f"Searching for tab link with text '{click_tab}'...")
                try:
                    tab_locator = page.get_by_role("link", name=click_tab)
                    if await tab_locator.count() > 0:
                        await tab_locator.click()
                        logger.info(f"Clicked '{click_tab}' tab.")
                        await page.wait_for_load_state("networkidle")
                    else:
                        logger.info(f"Tab '{click_tab}' not found by link name. Trying raw selectors...")
                        # Fallback try select text
                        tab_fallback = await page.query_selector(f"text='{click_tab}'")
                        if tab_fallback:
                            await tab_fallback.click()
                            logger.info(f"Clicked tab '{click_tab}' via text selector.")
                            await page.wait_for_load_state("networkidle")
                        else:
                            logger.info(f"Tab '{click_tab}' not found.")
                except PLAYWRIGHT_EXCEPTIONS as e:
                    logger.error(f"Error clicking tab: {e}")

            # Handle select year
            if year:
                logger.info(f"Attempting to select year {year}...")
                try:
                    year_select = await page.query_selector("select[id*='ddlYear']")
                    if year_select:
                        await year_select.select_option(year)
                        logger.info(f"Selected year {year}.")
                        await page.wait_for_load_state("networkidle")
                        await asyncio.sleep(2)  # Buffer for JS
                    else:
                        logger.info("Year select element not found.")
                except PLAYWRIGHT_EXCEPTIONS as e:
                    logger.error(f"Error selecting year: {e}")

            # Extract tables
            tables = await page.query_selector_all("table")
            logger.info(f"Found {len(tables)} tables on the page.")
            for i, table in enumerate(tables):
                cls = await table.get_attribute("class")
                id_attr = await table.get_attribute("id")
                headers = [await th.inner_text() for th in await table.query_selector_all("thead th")]
                logger.info(f"\nTable {i}: ID={id_attr}, Class={cls}")
                logger.info(f"  Headers: {headers}")

                rows = await table.query_selector_all("tbody tr")
                logger.info(f"  Rows count: {len(rows)}")
                if rows:
                    first_row_data = [await td.inner_text() for td in await rows[0].query_selector_all("td")]
                    # Clean newlines in outputs
                    first_row_data = [td.strip().replace("\n", " ") for td in first_row_data]
                    logger.info(f"  First row data: {first_row_data}")

            if screenshot_path:
                await page.screenshot(path=screenshot_path, full_page=True)
                logger.info(f"Screenshot saved to {screenshot_path}")

        except PLAYWRIGHT_EXCEPTIONS as e:
            logger.error(f"Error during player profile inspection: {e}")
        finally:
            await browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Consolidated KBO Inspection and Debugging Tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: db
    db_parser = subparsers.add_parser("db", help="Query local SQLite database")
    db_parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="Path to SQLite db file")
    db_group = db_parser.add_mutually_exclusive_group(required=True)
    db_group.add_argument("--query", type=str, help="Raw SQL query to execute")
    db_group.add_argument("--game-id", type=str, help="Game ID to inspect pitching/batting stats")
    db_group.add_argument("--player-id", type=int, help="Player ID to inspect daily stats")
    db_parser.add_argument(
        "--season",
        type=str,
        default=str(datetime.now().year),
        help="Season to filter player daily stats (default: current year)",
    )

    # Subcommand: gamecenter
    gc_parser = subparsers.add_parser("gamecenter", help="Inspect KBO GameCenter page via Playwright")
    gc_parser.add_argument("--date", type=str, default="20240323", help="Game Date (YYYYMMDD)")
    gc_parser.add_argument("--game-id", type=str, default="20240323HHLG0", help="KBO Game ID")
    gc_parser.add_argument("--section", type=str, default="REVIEW", help="Section (e.g. REVIEW, RECORD)")
    gc_parser.add_argument(
        "--no-headless", action="store_false", dest="headless", help="Run browser in non-headless mode"
    )
    gc_parser.add_argument("--screenshot", type=str, help="Save screenshot to specified path")

    # Subcommand: player
    p_parser = subparsers.add_parser("player", help="Inspect KBO Player details via Playwright")
    p_parser.add_argument("--player-id", type=str, required=True, help="KBO Player ID")
    p_parser.add_argument("--type", choices=["hitter", "pitcher"], default="hitter", help="Player type")
    p_parser.add_argument("--page", choices=["game", "basic", "daily"], default="game", help="Detail page type")
    p_parser.add_argument("--year", type=str, help="Select year option (e.g. 2020)")
    p_parser.add_argument("--click-tab", type=str, help="Link/tab text to click (e.g. '일자별기록')")
    p_parser.add_argument(
        "--no-headless", action="store_false", dest="headless", help="Run browser in non-headless mode"
    )
    p_parser.add_argument("--screenshot", type=str, help="Save full-page screenshot to specified path")

    # Subcommand: summary
    summary_parser = subparsers.add_parser("summary", help="Summarize KBO Database row counts")
    summary_parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="Path to SQLite db file (local)")
    summary_parser.add_argument(
        "--oci", action="store_true", help="Summarize OCI remote PostgreSQL database instead of local SQLite"
    )

    args = parser.parse_args()

    if args.command == "db":
        if args.query:
            run_db_query(args.db_path, args.query)
        elif args.game_id:
            db_inspect_game(args.db_path, args.game_id)
        elif args.player_id:
            db_inspect_player(args.db_path, args.player_id, args.season)

    elif args.command == "gamecenter":
        asyncio.run(
            inspect_gamecenter(
                date=args.date,
                game_id=args.game_id,
                section=args.section,
                headless=args.headless,
                screenshot_path=args.screenshot,
            )
        )

    elif args.command == "player":
        asyncio.run(
            inspect_player_profile(
                player_id=args.player_id,
                player_type=args.type,
                page_type=args.page,
                year=args.year,
                click_tab=args.click_tab,
                headless=args.headless,
                screenshot_path=args.screenshot,
            )
        )

    elif args.command == "summary":
        if args.oci:
            run_oci_summary()
        else:
            run_db_summary(args.db_path)


if __name__ == "__main__":
    main()
