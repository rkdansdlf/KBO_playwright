"""
Player Search Crawler
Collects comprehensive player information from KBO Player Search page.
Now refactored into a class as expected by GameDetailCrawler.
"""
import asyncio
import os
import re
from dataclasses import dataclass
from datetime import datetime, date as date_type
from typing import List, Optional, Set
from urllib.parse import urlparse, parse_qs

from playwright.async_api import Locator, Page

from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.player_classification import classify_player, PlayerCategory
from src.services.player_status_confirmer import PlayerStatusConfirmer

# URL and selectors
SEARCH_URL = "https://www.koreabaseball.com/Player/Search.aspx"
SEARCH_INPUT = "input[id$='txtSearchPlayerName']"
SEARCH_BTN = "input[id$='btnSearch']"
TABLE_ROWS = "table.tEx tbody tr"
HFPAGE = "input[id$='hfPage']"
PAGE_NUMBER_BTNS = "a[id*='btnNo'], span[id*='btnNo']"
PAGER_CONTAINER = "div.paging"
PAGER_NEXT_BTNS = "a[id$='btnNext'], a:has(img[alt='다음']), a:has-text('다음'), a[id$='btnNext10']"

REQUEST_DELAY_SEC = 1.0
TIMEOUT_MS = 15000

POSTBACK_RE = re.compile(r"__doPostBack\('([^']+)'\s*,\s*'([^']*)'\)")
INITIAL_CH_RE = re.compile(r"^[가-힣A-Z]$")
NAME_CLEAN_RE = re.compile(r'[^가-힣a-zA-Z]')

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
    player_id: int
    uniform_no: Optional[str]
    name: str
    team: Optional[str]
    position: Optional[str]
    birth_date: Optional[str]
    height_cm: Optional[int]
    weight_kg: Optional[int]
    career: Optional[str]

class PlayerSearchCrawler:
    def __init__(
        self,
        pool: Optional[AsyncPlaywrightPool] = None,
        request_delay: float = REQUEST_DELAY_SEC,
        headless: bool = True,
    ):
        self.pool = pool
        self.request_delay = request_delay
        self.headless = headless

    async def search_player(self, player_name: str) -> List[dict]:
        """Searches for a player and returns matching profiles as dicts."""
        clean_name = NAME_CLEAN_RE.sub('', player_name)
        if not clean_name: return []

        active_pool = self.pool or AsyncPlaywrightPool(max_pages=1, headless=self.headless)
        owns_pool = self.pool is None
        if owns_pool: await active_pool.start()

        try:
            page = await active_pool.acquire()
            try:
                await page.goto(SEARCH_URL, wait_until="domcontentloaded")
                await page.locator(SEARCH_INPUT).fill(clean_name)
                await page.locator(SEARCH_BTN).click()
                try:
                    await page.wait_for_selector(TABLE_ROWS, timeout=5000)
                except Exception:
                    return []
                rows = await self._paginate_current_tab(page)
                return [self.row_to_dict(r) for r in rows]
            finally:
                await active_pool.release(page)
        finally:
            if owns_pool: await active_pool.close()

    async def crawl_all_players(self, max_pages: Optional[int] = None) -> List[PlayerRow]:
        active_pool = self.pool or AsyncPlaywrightPool(max_pages=1, headless=self.headless)
        owns_pool = self.pool is None
        if owns_pool: await active_pool.start()
        try:
            page = await active_pool.acquire()
            try:
                await page.goto(SEARCH_URL, wait_until="domcontentloaded")
                await page.locator(SEARCH_INPUT).fill("%")
                await page.locator(SEARCH_BTN).click()
                await page.wait_for_selector(TABLE_ROWS, timeout=TIMEOUT_MS)

                all_rows: List[PlayerRow] = []
                seen_ids: Set[int] = set()
                limit = max_pages * 20 if max_pages is not None else None

                initial_links = await self._list_initial_links(page)
                if not initial_links:
                    await self._merge_rows(page, all_rows, seen_ids, limit)
                else:
                    if await self._merge_rows(page, all_rows, seen_ids, limit): return all_rows
                    index = 0
                    while True:
                        current_links = await self._list_initial_links(page)
                        if index >= len(current_links): break
                        prev_v = await self._get_hfpage_value(page)
                        first_b = await self._get_first_player_name(page)
                        if not await self._trigger_postback(page, current_links[index]):
                            index += 1; continue
                        await self._wait_after_nav(page, prev_v, first_b)
                        if await self._merge_rows(page, all_rows, seen_ids, limit): return all_rows
                        index += 1
                return all_rows
            finally:
                await active_pool.release(page)
        finally:
            if owns_pool: await active_pool.close()

    async def _merge_rows(self, page, all_rows, seen_ids, limit):
        rows = await self._paginate_current_tab(page)
        for r in rows:
            if r.player_id not in seen_ids:
                seen_ids.add(r.player_id)
                all_rows.append(r)
                if limit and len(all_rows) >= limit: return True
        return False

    async def _paginate_current_tab(self, page: Page) -> List[PlayerRow]:
        collected: List[PlayerRow] = []
        seen: Set[int] = set()

        async def add_current():
            for r in await self._collect_page_rows(page):
                if r.player_id not in seen:
                    seen.add(r.player_id); collected.append(r)

        await add_current()
        while True:
            pager = page.locator(PAGER_CONTAINER).last
            if await pager.count() == 0: break
            nums = pager.locator(":is(a, span)").filter(has_text=re.compile(r"^\d+$"))
            count = await nums.count()
            if count == 0: break

            curr_idx = 0
            for i in range(count):
                if "on" in (await nums.nth(i).get_attribute("class") or "").lower():
                    curr_idx = i; break

            moved = False
            for i in range(curr_idx + 1, count):
                target = page.locator(PAGER_CONTAINER).last.locator(":is(a, span)").filter(has_text=re.compile(r"^\d+$")).nth(i)
                if (await target.evaluate("el => el.tagName")).lower() != "a": continue
                prev_v = await self._get_hfpage_value(page)
                first_b = await self._get_first_player_name(page)
                if await self._trigger_postback(page, target):
                    await self._wait_after_nav(page, prev_v, first_b)
                    await add_current(); moved = True

            # Next block
            next_btn = page.locator(PAGER_CONTAINER).last.locator(PAGER_NEXT_BTNS).first
            if await next_btn.count() > 0 and (await next_btn.evaluate("el => el.tagName")).lower() == "a":
                prev_v = await self._get_hfpage_value(page)
                first_b = await self._get_first_player_name(page)
                if await self._trigger_postback(page, next_btn):
                    await self._wait_after_nav(page, prev_v, first_b)
                    await add_current(); moved = True
                else:
                    break
            if not moved: break
        return collected

    async def _collect_page_rows(self, page: Page) -> List[PlayerRow]:
        payload = await page.evaluate("(sel) => Array.from(document.querySelectorAll(sel)).map(r => ({cells: Array.from(r.querySelectorAll('td')).map(td => td.innerText.trim()), linkHref: r.querySelector('td:nth-child(2) a')?.getAttribute('href')}))", TABLE_ROWS)
        res = []
        for r in payload or []:
            cells = r['cells']
            if len(cells) < 7: continue
            pid = self._extract_pid(r['linkHref'])
            if not pid: continue
            h, w = self._parse_hw(cells[5])
            res.append(PlayerRow(player_id=pid, uniform_no=cells[0] if cells[0] != "-" else None, name=cells[1], team=cells[2] if cells[2] != "-" else None, position=cells[3], birth_date=cells[4], height_cm=h, weight_kg=w, career=cells[6]))
        return res

    def _extract_pid(self, href):
        if not href: return None
        m = re.search(r"playerId=(\d+)", href.replace(',', ''))
        return int(m.group(1)) if m else None

    def _parse_hw(self, s):
        m = re.search(r"(\d+)cm.*/(\d+)kg", s.replace(" ", ""))
        return (int(m.group(1)), int(m.group(2))) if m else (None, None)

    async def _get_hfpage_value(self, page):
        return await page.evaluate("(sel) => document.querySelector(sel)?.value || ''", HFPAGE)

    async def _get_first_player_name(self, page):
        try: return (await page.locator(TABLE_ROWS).first.locator("td").nth(1).inner_text()).strip()
        except: return ""

    async def _trigger_postback(self, page, anchor):
        try:
            # Try to use click() which handles both normal links and many JS-based navigations reliably.
            # We use wait_for_load_state as a generic way to ensure navigation finished.
            await anchor.click(timeout=10000)
            await page.wait_for_load_state("networkidle", timeout=10000)
            return True
        except Exception as e:
            # Fallback to manual postback if click fails or times out
            try:
                href = await anchor.get_attribute("href", timeout=5000)
                if href and "javascript:__doPostBack" in href:
                    m = POSTBACK_RE.search(href)
                    if m:
                        try:
                            await page.evaluate(POSTBACK_EVAL, [m.group(1), m.group(2)])
                            await page.wait_for_load_state("networkidle", timeout=10000)
                            return True
                        except:
                            pass
            except:
                pass
            return False

    async def _wait_after_nav(self, page, prev_v, first_b):
        try: await page.wait_for_function("([s, v]) => document.querySelector(s)?.value !== v", [HFPAGE, prev_v], timeout=5000)
        except: pass
        await asyncio.sleep(self.request_delay)

    async def _list_initial_links(self, page):
        links = page.locator("a")
        res = []
        for i in range(await links.count()):
            txt = (await links.nth(i).inner_text()).strip()
            if INITIAL_CH_RE.match(txt): res.append(links.nth(i))
        return res

    @staticmethod
    def row_to_dict(row: PlayerRow) -> dict:
        return player_row_to_dict(row)


def parse_birth_date(raw: Optional[str]) -> Optional[date_type]:
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
            return datetime.strptime(text, date_format).date()
        except ValueError:
            continue

    try:
        parts = text.replace("-", ".").replace("/", ".").split(".")
        if len(parts) == 3 and all(part.isdigit() for part in parts):
            year, month, day = (int(part) for part in parts)
            if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                return datetime(year, month, day).date()
    except Exception:
        return None
    return None


def player_row_to_dict(row: PlayerRow) -> dict:
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
    max_pages: Optional[int] = None,
    headless: bool = False,
    slow_mo=200,
    request_delay: float = REQUEST_DELAY_SEC,
    pool: Optional[AsyncPlaywrightPool] = None,
) -> List[PlayerRow]:
    crawler = PlayerSearchCrawler(
        pool=pool,
        request_delay=request_delay,
        headless=headless,
    )
    return await crawler.crawl_all_players(max_pages=max_pages)


async def main():
    import argparse

    parser = argparse.ArgumentParser(description="KBO Player Search Crawler")
    parser.set_defaults(save=True, sync_supabase=None)
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum pages to crawl (default: all)")
    parser.add_argument("--save", dest="save", action="store_true", help="Save to SQLite database (default)")
    parser.add_argument("--no-save", dest="save", action="store_false", help="Skip saving to SQLite database")
    parser.add_argument(
        "--sync-supabase",
        dest="sync_supabase",
        action="store_true",
        help="Sync to Supabase after crawling (default when SUPABASE_DB_URL is set)",
    )
    parser.add_argument(
        "--no-sync-supabase",
        dest="sync_supabase",
        action="store_false",
        help="Skip Supabase sync even if SUPABASE_DB_URL is set",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("KBO Player Search Crawler")
    print("=" * 60)

    print(f"\nCrawling players (max_pages={args.max_pages or 'all'})...")
    players = await crawl_all_players(max_pages=args.max_pages)
    print(f"\nTotal players collected: {len(players)}")

    if not players:
        print("No players collected")
        return

    print("\nSample (first 5 players):")
    for player in players[:5]:
        print(f"  - {player.name} (ID: {player.player_id}, #{player.uniform_no}, {player.team}/{player.position})")

    supabase_url = os.getenv("SUPABASE_DB_URL")
    should_sync = args.sync_supabase if args.sync_supabase is not None else bool(supabase_url)

    if args.save or should_sync:
        from src.db.engine import init_db

        print("\nInitializing database...")
        init_db()

    player_dicts = [player_row_to_dict(player) for player in players]

    if args.save:
        from src.repositories.player_basic_repository import PlayerBasicRepository

        suspects = [entry for entry in player_dicts if entry.get("status") in {"retired", "staff"}]
        if suspects:
            confirmer = PlayerStatusConfirmer()
            confirm_stats = await confirmer.confirm_entries(suspects)
            print(
                "\nProfile-confirmed statuses: "
                f"{confirm_stats['confirmed']} (attempted {confirm_stats['attempted']})"
            )

        parsed_dates = sum(1 for player in player_dicts if player["birth_date_date"] is not None)
        print(f"\nParsed birth dates: {parsed_dates}/{len(player_dicts)}")

        print("\nSaving to SQLite...")
        repo = PlayerBasicRepository()
        saved_count = repo.upsert_players(player_dicts)
        print(f"Saved {saved_count} players to SQLite")
    else:
        print("\nSkipping SQLite save (--no-save specified)")
        if should_sync:
            print("Existing SQLite data will be used for Supabase sync")

    if should_sync:
        from src.db.engine import SessionLocal
        from src.sync.supabase_sync import SupabaseSync

        if not supabase_url:
            print("\nSUPABASE_DB_URL not set; skipping Supabase sync")
        else:
            print("\nSyncing to Supabase...")
            with SessionLocal() as sqlite_session:
                sync = SupabaseSync(supabase_url, sqlite_session)
                try:
                    if not sync.test_connection():
                        print("Supabase connection failed")
                        return

                    synced = sync.sync_player_basic()
                    print(f"Synced {synced} players to Supabase")
                finally:
                    sync.close()
    else:
        if args.sync_supabase is False:
            print("\nSkipping Supabase sync (--no-sync-supabase specified)")
        elif not supabase_url:
            print("\nSUPABASE_DB_URL not set; Supabase sync skipped")

    print("\n" + "=" * 60)
    print("Complete")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
