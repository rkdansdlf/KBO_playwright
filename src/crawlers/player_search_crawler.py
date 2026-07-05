"""
Player Search Crawler.

Collects comprehensive player information from KBO Player Search page.

Now refactored into a class as expected by GameDetailCrawler.

"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date as date_type
from datetime import datetime
from typing import Any

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Locator, Page

from src.constants import KST
from src.crawlers.selectors import PLAYER_SEARCH
from src.services.player_status_confirmer import PlayerStatusConfirmer
from src.utils.compliance import compliance
from src.utils.player_classification import PlayerCategory, classify_player
from src.utils.player_validation import normalize_player_name, validate_player_payload
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.playwright_retry import SHORT_TIMEOUT
from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)

PLAYER_SEARCH_EXCEPTIONS = (
    PlaywrightError,
    TimeoutError,
    asyncio.TimeoutError,
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
)

# URL and selectors
SEARCH_URL = "https://www.koreabaseball.com/Player/Search.aspx"
SEARCH_INPUT = PLAYER_SEARCH.input
SEARCH_BTN = PLAYER_SEARCH.search_button
TABLE_ROWS = PLAYER_SEARCH.table_rows
HFPAGE = PLAYER_SEARCH.hidden_page
PAGE_NUMBER_BTNS = PLAYER_SEARCH.page_number_buttons
PAGER_CONTAINER = PLAYER_SEARCH.pager_container
PAGER_NEXT_BTNS = PLAYER_SEARCH.pager_next_buttons

REQUEST_DELAY_SEC = 1.0
TIMEOUT_MS = 15000

POSTBACK_RE = re.compile(r"__doPostBack\('([^']+)'\s*,\s*'([^']*)'\)")
INITIAL_CH_RE = re.compile(r"^[가-힣A-Z]$")
NAME_CLEAN_RE = re.compile(r"[^가-힣a-zA-Z]")

POSTBACK_EVAL = """
([target, arg]) => {
  const form = document.querySelector('form');
  if (!form) return false;
  let et = form.querySelector("input[name='__EVENTTARGET']");
  let ea = form.querySelector("input[name='__EVENTARGUMENT']");
  if (!et) {
    et = document.createElement('input');
    et.type = 'hidden';
    et.name = '__EVENTTARGET';
    form.appendChild(et);
  }
  if (!ea) {
    ea = document.createElement('input');
    ea.type = 'hidden';
    ea.name = '__EVENTARGUMENT';
    form.appendChild(ea);
  }
  et.value = target;
  ea.value = arg || '';
  form.submit();
  return true;
}
"""


@dataclass
class PlayerRow:
    """PlayerRow class."""

    player_id: int
    uniform_no: str | None
    name: str
    team: str | None
    position: str | None
    birth_date: str | None
    height_cm: int | None
    weight_kg: int | None
    career: str | None


class PlayerSearchCrawler:
    """PlayerSearchCrawler class."""

    def __init__(
        self,
        pool: AsyncPlaywrightPool | None = None,
        request_delay: float = REQUEST_DELAY_SEC,
        *,
        headless: bool = True,
    ) -> None:
        """
        Initialize a new instance.

        Args:
            pool: Connection pool for async operations.
            request_delay: Request Delay.
            headless: Whether to run the browser in headless mode.

        """
        self.pool = pool

        self.request_delay = request_delay
        self.headless = headless
        self.policy = RequestPolicy.with_delay(request_delay, request_delay)
        self.failure_counts: Counter = Counter()

    def _record_failure(self, reason: str) -> None:
        self.failure_counts[reason] += 1

    def get_failure_summary(self) -> dict[str, Any]:
        """
        Get failure summary.

        Returns:
            Dictionary result.

        """
        return dict(self.failure_counts)

    async def _navigate_search_page(
        self,
        page: Page,
        *,
        url: str = SEARCH_URL,
        required_selector: str | None = None,
        timeout: int = TIMEOUT_MS,
        selector_timeout: int = TIMEOUT_MS,
    ) -> tuple[bool, str]:
        if not await compliance.is_allowed(url):
            self._record_failure("blocked")
            return False, "blocked"

        async def _navigate() -> None:
            await self.policy.delay_async(host="www.koreabaseball.com")
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            if required_selector:
                await page.wait_for_selector(required_selector, timeout=selector_timeout)

        try:
            await self.policy.run_with_retry_async(_navigate)
        except (PlaywrightError, TimeoutError):
            reason = "selector_timeout" if required_selector else "navigation_failed"
            logger.warning("Player search page navigation failed: %s", reason)
            self._record_failure(reason)
            return False, reason
        else:
            return True, "ok"

    async def search_player(self, player_name: str) -> list[dict]:
        """
        Search for a player and returns matching profiles as dicts.

        Args:
            player_name: Player Name.

        """
        clean_name = NAME_CLEAN_RE.sub("", player_name)

        if not clean_name:
            return []

        active_pool = self.pool or AsyncPlaywrightPool(max_pages=1, headless=self.headless)
        owns_pool = self.pool is None
        if owns_pool:
            await active_pool.start()

        try:
            page = await active_pool.acquire()
            try:
                ok, _ = await self._navigate_search_page(page)
                if not ok:
                    return []
                await page.locator(SEARCH_INPUT).fill(clean_name)
                await page.locator(SEARCH_BTN).click()
                try:
                    await page.wait_for_selector(TABLE_ROWS, timeout=TIMEOUT_MS)
                except TimeoutError:
                    logger.warning("Table rows not found for player search")
                    return []
                rows = await self._paginate_current_tab(page)
                return [self.row_to_dict(r) for r in rows]
            finally:
                await active_pool.release(page)
        finally:
            if owns_pool:
                await active_pool.close()

    async def crawl_all_players(self, max_pages: int | None = None) -> list[PlayerRow]:
        """
        Crawl all players.

        Args:
            max_pages: Max Pages.
            max_pages: Max Pages.

        Returns:
            List of results.

        """
        active_pool = self.pool or AsyncPlaywrightPool(max_pages=1, headless=self.headless)

        owns_pool = self.pool is None
        if owns_pool:
            await active_pool.start()
        try:
            page = await active_pool.acquire()
            try:
                ok, _ = await self._navigate_search_page(page)
                if not ok:
                    return []
                await page.locator(SEARCH_INPUT).fill("%")
                await page.locator(SEARCH_BTN).click()
                await page.wait_for_selector(TABLE_ROWS, timeout=TIMEOUT_MS)

                all_rows: list[PlayerRow] = []
                seen_ids: set[int] = set()
                limit = max_pages * 20 if max_pages is not None else None

                initial_links = await self._list_initial_links(page)
                if not initial_links:
                    await self._merge_rows(page, all_rows, seen_ids, limit)
                else:
                    if await self._merge_rows(page, all_rows, seen_ids, limit):
                        return all_rows
                    index = 0
                    while True:
                        current_links = await self._list_initial_links(page)
                        if index >= len(current_links):
                            break
                        prev_v = await self._get_hfpage_value(page)
                        first_b = await self._get_first_player_name(page)
                        if not await self._trigger_postback(page, current_links[index]):
                            index += 1
                            continue
                        await self._wait_after_nav(page, prev_v, first_b)
                        if await self._merge_rows(page, all_rows, seen_ids, limit):
                            return all_rows
                        index += 1
                return all_rows
            finally:
                await active_pool.release(page)
        finally:
            if owns_pool:
                await active_pool.close()

    async def _merge_rows(
        self,
        page: Page,
        all_rows: list[PlayerRow],
        seen_ids: set[int],
        limit: int | None,
    ) -> bool:
        rows = await self._paginate_current_tab(page)
        for r in rows:
            if r.player_id not in seen_ids:
                seen_ids.add(r.player_id)
                all_rows.append(r)
                if limit and len(all_rows) >= limit:
                    return True
            else:
                self._record_failure("duplicate_player_id")
        return False

    async def _paginate_current_tab(self, page: Page) -> list[PlayerRow]:
        collected: list[PlayerRow] = []
        seen: set[int] = set()

        await self._add_current_page_rows(page, collected, seen)
        while True:
            pager = page.locator(PAGER_CONTAINER).last
            if await pager.count() == 0:
                break
            nums = pager.locator(":is(a, span)").filter(has_text=re.compile(r"^\d+$"))
            count = await nums.count()
            if count == 0:
                break

            curr_idx = await self._current_pager_index(nums, count)
            moved = await self._visit_remaining_numeric_pages(page, curr_idx, count, collected, seen)
            next_moved, next_failed = await self._visit_next_pager_block(page, collected, seen)
            moved = moved or next_moved
            if next_failed:
                break
            if not moved:
                break
        return collected

    async def _add_current_page_rows(self, page: Page, collected: list[PlayerRow], seen: set[int]) -> None:
        for row in await self._collect_page_rows(page):
            if row.player_id not in seen:
                seen.add(row.player_id)
                collected.append(row)
            else:
                self._record_failure("duplicate_player_id")

    @staticmethod
    async def _current_pager_index(nums: Locator, count: int) -> int:
        for idx in range(count):
            if "on" in (await nums.nth(idx).get_attribute("class") or "").lower():
                return idx
        return 0

    async def _visit_remaining_numeric_pages(
        self,
        page: Page,
        curr_idx: int,
        count: int,
        collected: list[PlayerRow],
        seen: set[int],
    ) -> bool:
        moved = False
        for idx in range(curr_idx + 1, count):
            target = (
                page.locator(PAGER_CONTAINER)
                .last.locator(":is(a, span)")
                .filter(has_text=re.compile(r"^\d+$"))
                .nth(idx)
            )
            if (await target.evaluate("el => el.tagName")).lower() != "a":
                continue
            if await self._click_pager_target(page, target, collected, seen):
                moved = True
        return moved

    async def _click_pager_target(
        self, page: Page, target: Locator, collected: list[PlayerRow], seen: set[int]
    ) -> bool:
        prev_v = await self._get_hfpage_value(page)
        first_b = await self._get_first_player_name(page)
        if not await self._trigger_postback(page, target):
            return False
        await self._wait_after_nav(page, prev_v, first_b)
        await self._add_current_page_rows(page, collected, seen)
        return True

    async def _visit_next_pager_block(
        self,
        page: Page,
        collected: list[PlayerRow],
        seen: set[int],
    ) -> tuple[bool, bool]:
        next_btn = page.locator(PAGER_CONTAINER).last.locator(PAGER_NEXT_BTNS).first
        if await next_btn.count() == 0 or (await next_btn.evaluate("el => el.tagName")).lower() != "a":
            return False, False
        if await self._click_pager_target(page, next_btn, collected, seen):
            return True, False
        self._record_failure("pagination_failed")
        return False, True

    async def _collect_page_rows(self, page: Page) -> list[PlayerRow]:
        # Retry on "Execution context was destroyed" which happens when
        # an ASP.NET postback response arrives during evaluate.
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                payload = await page.evaluate(
                    "(sel) => Array.from(document.querySelectorAll(sel)).map(r => ({cells: Array.from(r.querySelectorAll('td')).map(td => td.innerText.trim()), linkHref: r.querySelector('td:nth-child(2) a')?.getAttribute('href')}))",
                    TABLE_ROWS,
                )
                break
            except PLAYER_SEARCH_EXCEPTIONS as e:
                err_msg = str(e)
                if (
                    "Execution context was destroyed" in err_msg or "Target closed" in err_msg
                ) and attempt < max_attempts - 1:
                    await asyncio.sleep(1.0)
                    continue
                raise
        res = []
        for r in payload or []:
            cells = r["cells"]
            if len(cells) < 7:
                self._record_failure("insufficient_columns")
                continue
            pid = self._extract_pid(r["linkHref"])
            if pid is None:
                self._record_failure("missing_player_id")
                continue
            name = normalize_player_name(cells[1] if len(cells) > 1 else None)
            ok, reason = validate_player_payload({"player_id": pid, "name": name})
            if not ok:
                self._record_failure(reason or "invalid_player_payload")
                continue
            h, w = self._parse_hw(cells[5]) or (None, None)
            res.append(
                PlayerRow(
                    player_id=pid,
                    uniform_no=cells[0] if cells[0] != "-" else None,
                    name=name,
                    team=cells[2] if cells[2] != "-" else None,
                    position=cells[3],
                    birth_date=cells[4],
                    height_cm=h,
                    weight_kg=w,
                    career=cells[6],
                ),
            )
        return res

    @staticmethod
    def _extract_pid(href: str | None) -> int | None:
        if not href:
            return None
        m = re.search(r"playerId=(\d+)", href.replace(",", ""))
        return int(m.group(1)) if m else None

    @staticmethod
    def _parse_hw(s: str) -> tuple[int, int] | None:
        m = re.search(r"(\d+)cm.*/(\d+)kg", s.replace(" ", ""))
        return (int(m.group(1)), int(m.group(2))) if m else None

    async def _get_hfpage_value(self, page: Page) -> str:
        return await page.evaluate("(sel) => document.querySelector(sel)?.value || ''", HFPAGE)

    async def _get_first_player_name(self, page: Page) -> str:
        try:
            return (await page.locator(TABLE_ROWS).first.locator("td").nth(1).inner_text()).strip()
        except TimeoutError:
            logger.warning("Could not get first player name from table")
            return ""

    async def _trigger_postback(self, page: Page, anchor: Locator) -> bool:
        # Check href first — javascript:__doPostBack links must use manual evaluation
        # because Playwright click() returns success but does not actually trigger
        # the ASP.NET postback mechanism.
        try:
            href = await anchor.get_attribute("href", timeout=SHORT_TIMEOUT)
        except (PlaywrightError, TimeoutError, AssertionError):
            logger.warning("Timeout getting href from anchor", exc_info=True)
            href = None

        if href and "javascript:__doPostBack" in href:
            m = POSTBACK_RE.search(href)
            if m:
                try:
                    await page.evaluate(POSTBACK_EVAL, [m.group(1), m.group(2)])
                    await page.wait_for_load_state("load", timeout=SHORT_TIMEOUT)
                except PLAYER_SEARCH_EXCEPTIONS:
                    logger.exception("Manual postback evaluate failed")
                    return False
                else:
                    return True
                    logger.exception("Manual postback evaluate failed")
                    return False

        # Normal (non-JS) links: use click()
        try:
            await anchor.click(timeout=SHORT_TIMEOUT)
            await page.wait_for_load_state("load", timeout=SHORT_TIMEOUT)
        except PLAYER_SEARCH_EXCEPTIONS:
            logger.exception("Postback click failed: %s", href)
            return False
        else:
            return True

    async def _wait_after_nav(self, page: Page, prev_v: str, _first_b: str) -> None:
        with contextlib.suppress(TimeoutError):
            await page.wait_for_function(
                "([s, v]) => document.querySelector(s)?.value !== v",
                arg=[HFPAGE, prev_v],
                timeout=SHORT_TIMEOUT,
            )
        await asyncio.sleep(self.request_delay)

    async def _list_initial_links(self, page: Page) -> list[Locator]:
        links = page.locator("a")
        res: list[Locator] = []
        for i in range(await links.count()):
            txt = (await links.nth(i).inner_text()).strip()
            if INITIAL_CH_RE.match(txt):
                res.append(links.nth(i))
        return res

    @staticmethod
    def row_to_dict(row: PlayerRow) -> dict[str, Any]:
        """
        Handle the row to dict operation.

        Args:
            row: Row.
            row: Row.

        Returns:
            Dictionary result.

        """
        return player_row_to_dict(row)


def parse_birth_date(raw: str | None) -> date_type | None:
    """
    Parse birth date.

    Args:
        raw: Raw.
        raw: Raw.

    Returns:
        The result of the operation.

    """
    if not raw:
        return None

    text = raw.strip().replace(" ", "")
    formats = (
        "%Y-%m-%d",
        "%Y.%m.%d",
        "%Y/%m/%d",
        "%Y%m%d",
        "%y-%m-%d",
        "%y.%m.%d",
        "%y/%m/%d",
    )
    for date_format in formats:
        try:
            return datetime.strptime(text, date_format).replace(tzinfo=KST).date()
        except ValueError:
            continue

    try:
        parts = text.replace("-", ".").replace("/", ".").split(".")
        if len(parts) == 3 and all(part.isdigit() for part in parts):
            year, month, day = (int(part) for part in parts)
            if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                return datetime(year, month, day, tzinfo=KST).date()
    except ValueError:
        logger.warning("Failed to parse date from text: %s", text)
        return None
    return None


def player_row_to_dict(row: PlayerRow) -> dict[str, Any]:
    """
    Handle the player row to dict operation.

    Args:
        row: Row.
        row: Row.

    Returns:
        Dictionary result.

    """
    category = classify_player({"team": row.team, "position": row.position})

    status = "active"
    staff_role = None
    if category == PlayerCategory.RETIRED:
        status = "retired"
    elif category in (PlayerCategory.MANAGER, PlayerCategory.COACH, PlayerCategory.STAFF):
        status = "staff"
        staff_role = category.value.lower()

    return {
        "player_id": row.player_id,
        "name": row.name,
        "uniform_no": row.uniform_no,
        "team": row.team,
        "position": row.position,
        "birth_date": row.birth_date,
        "birth_date_date": parse_birth_date(row.birth_date),
        "height_cm": row.height_cm,
        "weight_kg": row.weight_kg,
        "career": row.career,
        "status": status,
        "staff_role": staff_role,
        "status_source": "heuristic",
    }


async def crawl_all_players(
    max_pages: int | None = None,
    *,
    headless: bool = False,
    slow_mo: int = 200,
    request_delay: float = REQUEST_DELAY_SEC,
    pool: AsyncPlaywrightPool | None = None,
) -> list[PlayerRow]:
    """
    Crawl all players.

    Args:
        max_pages: Max Pages.
        headless: Whether to run the browser in headless mode.
        slow_mo: Slow Mo.
        request_delay: Request Delay.
        pool: Connection pool for async operations.
        max_pages: Max Pages.

    Returns:
        List of results.

    """
    crawler = PlayerSearchCrawler(
        pool=pool,
        request_delay=request_delay,
        headless=headless,
    )
    return await crawler.crawl_all_players(max_pages=max_pages)


def _parse_crawl_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="KBO Player Search Crawler")
    parser.set_defaults(save=True, sync_oci=None)
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum pages to crawl (default: all)")
    parser.add_argument("--save", dest="save", action="store_true", help="Save to SQLite database (default)")
    parser.add_argument("--no-save", dest="save", action="store_false", help="Skip saving to SQLite database")
    parser.add_argument(
        "--sync-oci",
        dest="sync_oci",
        action="store_true",
        help="Sync player_basic to OCI after crawling (default when OCI_DB_URL is set)",
    )
    parser.add_argument(
        "--no-sync-oci",
        dest="sync_oci",
        action="store_false",
        help="Skip OCI sync even if OCI_DB_URL is set",
    )
    return parser.parse_args()


async def _crawl_players(max_pages: int | None) -> list[PlayerRow]:
    logger.info("\nCrawling players (max_pages=%s)...", max_pages or "all")
    players = await crawl_all_players(max_pages=max_pages)
    logger.info("\nTotal players collected: %s", len(players))
    if not players:
        logger.info("No players collected")
        return []
    logger.info("\nSample (first 5 players):")
    for player in players[:5]:
        logger.info(
            "  - %s (ID: %s, #%s, %s/%s)",
            player.name,
            player.player_id,
            player.uniform_no,
            player.team,
            player.position,
        )
    return players


async def _crawl_and_save(
    player_dicts: list[dict[str, Any]],
) -> None:
    from src.repositories.player_basic_repository import PlayerBasicRepository

    suspects = [entry for entry in player_dicts if entry.get("status") in {"retired", "staff"}]
    if suspects:
        confirmer = PlayerStatusConfirmer()
        confirm_stats = await confirmer.confirm_entries(suspects)
        logger.info(
            "\nProfile-confirmed statuses: %s (attempted %s)",
            confirm_stats["confirmed"],
            confirm_stats["attempted"],
        )

    parsed_dates = sum(1 for player in player_dicts if player["birth_date_date"] is not None)
    logger.info("\nParsed birth dates: %s/%s", parsed_dates, len(player_dicts))

    logger.info("\nSaving to SQLite...")
    repo = PlayerBasicRepository()
    saved_count = repo.upsert_players(player_dicts)
    logger.info("Saved %s players to SQLite", saved_count)


def _sync_to_oci(oci_url: str) -> None:
    from src.db.engine import SessionLocal
    from src.sync.oci_sync import OCISync

    logger.info("\nSyncing to OCI...")
    with SessionLocal() as sqlite_session:
        sync = OCISync(oci_url, sqlite_session)
        try:
            if not sync.test_connection():
                logger.info("OCI connection failed")
                return
            synced = sync.sync_player_basic()
            logger.info("Synced %s players to OCI", synced)
        finally:
            sync.close()


async def main() -> None:
    """Run the main entry point for this CLI command."""
    args = _parse_crawl_args()

    logger.info("=" * 60)
    logger.info("KBO Player Search Crawler")
    logger.info("=" * 60)

    players = await _crawl_players(args.max_pages)
    if not players:
        return

    oci_url = os.getenv("OCI_DB_URL")
    should_sync = args.sync_oci if args.sync_oci is not None else bool(oci_url)

    if args.save or should_sync:
        from src.db.engine import init_db

        logger.info("\nInitializing database...")
        init_db()

    player_dicts = [player_row_to_dict(player) for player in players]

    if args.save:
        await _crawl_and_save(player_dicts)
    else:
        logger.info("\nSkipping SQLite save (--no-save specified)")
        if should_sync:
            logger.info("Existing SQLite data will be used for OCI sync")

    if should_sync:
        if not oci_url:
            logger.info("\nOCI_DB_URL not set; skipping OCI sync")
        else:
            _sync_to_oci(oci_url)
    elif args.sync_oci is False:
        logger.info("\nSkipping OCI sync (--no-sync-oci specified)")
    elif not oci_url:
        logger.info("\nOCI_DB_URL not set; OCI sync skipped")

    logger.info("%s", "\n" + "=" * 60)
    logger.info("Complete")
    logger.info("%s", "=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
