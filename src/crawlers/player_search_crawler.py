"""
Player Search Crawler
Collects comprehensive player information from KBO Player Search page
Source: https://www.koreabaseball.com/Player/Search.aspx?searchWord=%25

Based on Docs/PLAYERID_CRAWLING.md design
"""
import asyncio
import os
import re
from dataclasses import dataclass
from datetime import datetime, date as date_type
from typing import List, Optional, Set
from urllib.parse import urlparse, parse_qs

from playwright.async_api import Locator, Page, async_playwright
from src.utils.player_classification import classify_player, PlayerCategory
from src.services.player_status_confirmer import PlayerStatusConfirmer

# URL and selectors
SEARCH_URL = "https://www.koreabaseball.com/Player/Search.aspx"
SEARCH_INPUT = "input[id$='txtSearchPlayerName']"
SEARCH_BTN = "input[id$='btnSearch']"
TABLE_ROWS = "table.tEx tbody tr"
NEXT_BTN = "a[id$='ucPager_btnNext']"
HFPAGE = "input[id$='hfPage']"
PAGE_NUMBER_BTNS = "a[id*='btnNo'], span[id*='btnNo']"
PAGER_CONTAINER = "div.paging"
PAGER_NEXT_BTNS = "a[id$='btnNext'], a:has(img[alt='다음']), a:has-text('다음'), a[id$='btnNext10']"


# Crawler settings
REQUEST_DELAY_SEC = 1.0
TIMEOUT_MS = 15000

# Patterns
POSTBACK_RE = re.compile(r"__doPostBack\('([^']+)'\s*,\s*'([^']*)'\)")
INITIAL_CH_RE = re.compile(r"^[가-힣A-Z]$")
POSTBACK_EVAL = """
(target, arg) => {
  if (typeof window.__doPostBack === 'function') {
    window.__doPostBack(target, arg || '');
    return true;
  }
  const form = document.querySelector('form');
  if (!form) return false;
  const et = form.querySelector("input[name='__EVENTTARGET']");
  const ea = form.querySelector("input[name='__EVENTARGUMENT']");
  if (et) et.value = target;
  if (ea) ea.value = arg || '';
  form.submit();
  return true;
}
"""


@dataclass
class PlayerRow:
    """Data structure for one player from search results"""
    player_id: int
    uniform_no: Optional[str]
    name: str
    team: Optional[str]
    position: Optional[str]
    birth_date: Optional[str]   # Original string format
    height_cm: Optional[int]
    weight_kg: Optional[int]
    career: Optional[str]        # School/origin (출신교)


def _parse_height_weight(s: str) -> tuple[Optional[int], Optional[int]]:
    """
    Parse height and weight from strings like:
    - "180cm/80kg"
    - "180cm / 80kg"
    - "182cm, 76kg" (comma separated)
    - "180/80"
    - "-" (returns None, None)
    """
    if not s:
        return None, None

    # Remove spaces for easier parsing
    s_clean = s.replace(" ", "")

    # Try pattern with comma: "182cm,76kg"
    m = re.search(r"(\d{2,3})cm[,/](\d{2,3})kg", s_clean, re.IGNORECASE)
    if m:
        h = int(m.group(1))
        w = int(m.group(2))
        # Sanity check
        if 140 <= h <= 220 and 45 <= w <= 150:
            return h, w

    # Fallback: more flexible pattern
    m = re.search(r"(\d{2,3})\s*cm?[,/ ]?\s*(\d{2,3})\s*kg?", s, re.IGNORECASE)
    if m:
        h = int(m.group(1))
        w = int(m.group(2))
        # Sanity check
        if 140 <= h <= 220 and 45 <= w <= 150:
            return h, w

    return None, None


def _extract_player_id(href: Optional[str]) -> Optional[int]:
    """Extract playerId from URL like '...?playerId=12345'"""
    if not href:
        return None

    try:
        q = parse_qs(urlparse(href).query)
        pid = q.get("playerId", [None])[0]
        if pid:
            # Remove commas and any non-digit characters
            pid_clean = re.sub(r'[^\d]', '', str(pid))
            return int(pid_clean) if pid_clean.isdigit() else None
    except Exception:
        pass

    # Fallback regex
    try:
        m = re.search(r"playerId=([0-9,]+)", href)
        if m:
            # Remove commas from matched string
            pid_clean = m.group(1).replace(',', '')
            return int(pid_clean) if pid_clean.isdigit() else None
    except Exception:
        pass

    return None


async def _collect_page_rows(page: Page) -> List[PlayerRow]:
    """
    Extract player data from current page table.

    Table structure (td indices):
    0 = 등번호 (uniform number)
    1 = 선수명 (link with playerId)
    2 = 팀명
    3 = 포지션
    4 = 생년월일
    5 = 체격 (키/몸무게)
    6 = 출신교
    """
    rows = page.locator(TABLE_ROWS)
    count = await rows.count()
    results: List[PlayerRow] = []

    for i in range(count):
        r = rows.nth(i)
        tds = r.locator("td")
        tdc = await tds.count()

        if tdc < 7:
            continue

        # Column 0: Uniform number
        uniform_no = (await tds.nth(0).inner_text()).strip() or None

        # Column 1: Player name + link
        name_el = tds.nth(1).locator("a")
        href = await name_el.get_attribute("href")
        name = (await name_el.inner_text()).strip()
        player_id = _extract_player_id(href)

        if player_id is None:
            continue

        # Column 2: Team
        team = (await tds.nth(2).inner_text()).strip() or None

        # Column 3: Position
        position = (await tds.nth(3).inner_text()).strip() or None

        # Column 4: Birth date
        birth = (await tds.nth(4).inner_text()).strip() or None

        # Column 5: Body (height/weight)
        body = (await tds.nth(5).inner_text()).strip() or ""

        # Column 6: Career (school/origin)
        career = (await tds.nth(6).inner_text()).strip() or None

        # Parse height and weight
        h, w = _parse_height_weight(body)

        # Normalize "-" to None
        results.append(PlayerRow(
            player_id=player_id,
            uniform_no=uniform_no if uniform_no != "-" else None,
            name=name,
            team=team if team != "-" else None,
            position=position if position != "-" else None,
            birth_date=birth if birth and birth != "-" else None,
            height_cm=h,
            weight_kg=w,
            career=career if career != "-" else None
        ))

    return results


async def _get_hfpage_value(page: Page) -> str:
    """Return the combined values of all hfPage hidden fields, empty string on failure."""
    try:
        value = await page.evaluate(
            "(selector) => Array.from(document.querySelectorAll(selector)).map(el => el.value || '').join('|')",
            HFPAGE,
        )
        if not value:
            return ""
        if "|" in value:
            for part in value.split("|"):
                if part:
                    return part
        return value
    except Exception:
        return ""


async def _click_postback_link(page: Page, locator: str) -> bool:
    """Click link matching locator via postback when available."""
    el = page.locator(locator)
    try:
        if await el.count() == 0:
            return False
        target_el = el.first
    except Exception:
        return False

    return await _trigger_postback(page, target_el)


async def _trigger_postback(page: Page, anchor: Locator) -> bool:
    """Trigger a postback for the given anchor, falling back to a regular click."""
    try:
        href = await anchor.get_attribute("href")
    except Exception:
        href = None

    if href and "javascript:__doPostBack" in href:
        match = POSTBACK_RE.search(href)
        if match:
            target = match.group(1)
            argument = match.group(2) if len(match.groups()) > 1 else ""
            try:
                success = await page.evaluate(POSTBACK_EVAL, target, argument)
                if success:
                    return True
            except Exception:
                pass

    try:
        try:
            await anchor.scroll_into_view_if_needed()
        except Exception:
            pass
        await anchor.click(timeout=5000)
        return True
    except Exception:
        return False


async def _list_initial_links(page: Page) -> List[Locator]:
    """Return locators for initial filters composed of a single Korean/Latin character."""
    links = page.locator("a")
    total = await links.count()
    result = []
    for idx in range(total):
        candidate = links.nth(idx)
        try:
            text = (await candidate.inner_text()).strip()
        except Exception:
            continue
        if not INITIAL_CH_RE.match(text):
            continue
        try:
            href = await candidate.get_attribute("href")
        except Exception:
            continue
        if href and "__doPostBack" in href:
            result.append(candidate)
    return result


async def _wait_hfpage_change(page: Page, previous: str, timeout: int = TIMEOUT_MS) -> None:
    """Wait until the hfPage hidden field updates to a different value."""
    await page.wait_for_function(
        """(selector, prev) => {
            const values = Array.from(document.querySelectorAll(selector))
                .map(el => (el && typeof el.value === 'string') ? el.value : '')
                .filter(Boolean);
            const current = values.length > 0 ? values[0] : '';
            return current && current !== prev;
        }""",
        (HFPAGE, previous),
        timeout=timeout,
    )


async def _wait_first_row_change(page: Page, previous: str, timeout: int = TIMEOUT_MS) -> None:
    """Fallback wait that checks for first row name change."""
    await page.wait_for_function(
        """(rowsSelector, prev) => {
            const row = document.querySelector(rowsSelector);
            if (!row) {
                return false;
            }
            const link = row.querySelector('td:nth-child(2) a');
            if (!link) {
                return false;
            }
            const text = (link.textContent || '').trim();
            if (!text) {
                return false;
            }
            return text !== prev;
        }""",
        (TABLE_ROWS, previous or "__EMPTY__"),
        timeout=timeout,
    )


async def _get_first_player_name(page: Page) -> str:
    """Return the first player's name on the current table page."""
    try:
        locator = page.locator(TABLE_ROWS).first.locator("td").nth(1).locator("a")
        text = await locator.inner_text()
        return text.strip()
    except Exception:
        return ""


async def _wait_updatepanel_idle(page: Page, timeout: int = TIMEOUT_MS) -> None:
    """Wait for ASP.NET UpdatePanel postback completion."""
    await page.wait_for_function(
        """() => {
            const sys = window.Sys;
            if (!sys || !sys.WebForms || !sys.WebForms.PageRequestManager) {
                return true;
            }
            const mgr = sys.WebForms.PageRequestManager.getInstance();
            if (!mgr) {
                return true;
            }
            return !mgr.get_isInAsyncPostBack();
        }""",
        timeout=timeout,
    )


async def _wait_after_navigation(page: Page, prev_hf: str, first_before: str, request_delay: float) -> bool:
    """Wait for navigation signals and ensure table is ready."""
    changed = False
    try:
        await _wait_hfpage_change(page, prev_hf, timeout=TIMEOUT_MS)
        changed = True
    except Exception:
        try:
            await _wait_first_row_change(page, first_before, timeout=TIMEOUT_MS)
            changed = True
        except Exception:
            changed = False

    await page.wait_for_selector(TABLE_ROWS, timeout=TIMEOUT_MS)
    await asyncio.sleep(request_delay)
    return changed


async def _paginate_current_tab(page: Page, request_delay: float) -> List[PlayerRow]:
    """Enumerate pages in the current initial tab until pagination is exhausted."""
    collected: List[PlayerRow] = []
    seen: Set[int] = set()

    async def collect_current() -> List[PlayerRow]:
        rows = await _collect_page_rows(page)
        fresh: List[PlayerRow] = []
        for row in rows:
            if row.player_id in seen:
                continue
            seen.add(row.player_id)
            fresh.append(row)
        return fresh

    collected.extend(await collect_current())

    while True:
        pager = page.locator(PAGER_CONTAINER).last
        if await pager.count() == 0:
            break

        number_locators = pager.locator(":is(a, span)").filter(
            has_text=re.compile(r"^\d+$")
        )
        total_numbers = await number_locators.count()

        if total_numbers == 0:
            fallback_numbers = pager.locator(PAGE_NUMBER_BTNS)
            fallback_count = await fallback_numbers.count()
            if fallback_count > 0:
                number_locators = fallback_numbers
                total_numbers = fallback_count

        # If still no page number buttons found, try "Next" button
        if total_numbers == 0:
            next_candidates = pager.locator(PAGER_NEXT_BTNS)
            next_anchor = None
            next_count = await next_candidates.count()
            for idx in range(next_count):
                candidate = next_candidates.nth(idx)
                try:
                    tag_name = (await candidate.evaluate("el => el.tagName")).lower()
                except Exception:
                    tag_name = ""
                if tag_name == "a":
                    next_anchor = candidate
                    break

            if next_anchor is None:
                break

            prev_value = await _get_hfpage_value(page)
            first_before = await _get_first_player_name(page)
            if not await _trigger_postback(page, next_anchor):
                break

            await _wait_updatepanel_idle(page, timeout=TIMEOUT_MS)
            changed = await _wait_after_navigation(page, prev_value, first_before, request_delay)
            fresh_rows = await collect_current()
            if fresh_rows:
                collected.extend(fresh_rows)
                continue
            if changed:
                continue
            break

        current_index = 0
        found_current = False
        for idx in range(total_numbers):
            classes = (await number_locators.nth(idx).get_attribute("class") or "").lower()
            if "on" in classes:
                current_index = idx
                found_current = True
                break
        if not found_current:
            hf_snapshot = await _get_hfpage_value(page)
            if hf_snapshot:
                for idx in range(total_numbers):
                    try:
                        label = (await number_locators.nth(idx).inner_text()).strip()
                    except Exception:
                        continue
                    if label == hf_snapshot:
                        current_index = idx
                        break

        moved = False

        for target_idx in range(current_index + 1, total_numbers):
            refreshed_pager = page.locator(PAGER_CONTAINER).last
            # Use same selector as initial search (text-based priority)
            refreshed_numbers = refreshed_pager.locator(":is(a, span)").filter(
                has_text=re.compile(r"^\d+$")
            )
            refreshed_count = await refreshed_numbers.count()
            if refreshed_count == 0:
                refreshed_numbers = refreshed_pager.locator(PAGE_NUMBER_BTNS)
                refreshed_count = await refreshed_numbers.count()

            if target_idx >= refreshed_count:
                break
            target = refreshed_numbers.nth(target_idx)

            try:
                tag_name = (await target.evaluate("el => el.tagName")).lower()
            except Exception:
                tag_name = ""
            if tag_name != "a":
                continue

            prev_value = await _get_hfpage_value(page)
            first_before = await _get_first_player_name(page)

            clicked = await _trigger_postback(page, target)
            if not clicked:
                continue

            await _wait_updatepanel_idle(page, timeout=TIMEOUT_MS)
            changed = await _wait_after_navigation(page, prev_value, first_before, request_delay)
            fresh_rows = await collect_current()
            if fresh_rows:
                collected.extend(fresh_rows)
                moved = True
                continue

            if not changed:
                continue

            moved = True

        prev_value = await _get_hfpage_value(page)
        first_before = await _get_first_player_name(page)

        refreshed_pager = page.locator(PAGER_CONTAINER).last
        next_candidates = refreshed_pager.locator(PAGER_NEXT_BTNS)
        clicked_next = False
        count_next = await next_candidates.count()
        for idx in range(count_next):
            candidate = next_candidates.nth(idx)
            # ensure anchor
            try:
                tag_name = (await candidate.evaluate("el => el.tagName")).lower()
            except Exception:
                tag_name = ""
            if tag_name != "a":
                continue
            if await _trigger_postback(page, candidate):
                clicked_next = True
                break

        if not clicked_next:
            break

        await _wait_updatepanel_idle(page, timeout=TIMEOUT_MS)
        changed = await _wait_after_navigation(page, prev_value, first_before, request_delay)
        fresh_rows = await collect_current()
        if fresh_rows:
            collected.extend(fresh_rows)
            moved = True
        elif not changed:
            break
        else:
            moved = True

        if not moved:
            break

    return collected


async def crawl_all_players(
    max_pages: Optional[int] = None,
    headless: bool = False,
    slow_mo=200,
    request_delay: float = REQUEST_DELAY_SEC,
) -> List[PlayerRow]:
    """
    Crawl all players from KBO search page, traversing all initial filters and pagination.

    Args:
        max_pages: Maximum number of result pages to include (approx., 20 rows per page)
        headless: Run browser in headless mode
        request_delay: Delay between page interactions (seconds)

    Returns:
        List of PlayerRow objects
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        ctx = await browser.new_context()
        page = await ctx.new_page()

        try:
            # 1. Navigate to search page
            await page.goto(SEARCH_URL, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)

            # 2. Enter '%' in search input to get all players
            search_input = page.locator(SEARCH_INPUT)
            await search_input.fill("%")

            # 3. Click search button
            search_btn = page.locator(SEARCH_BTN)
            await search_btn.click()

            # 4. Wait for results table and pagination to appear
            await page.wait_for_selector(TABLE_ROWS, timeout=TIMEOUT_MS)
            await page.wait_for_selector(PAGER_CONTAINER, timeout=TIMEOUT_MS)

            all_rows: List[PlayerRow] = []
            seen_ids: Set[int] = set()
            limit = max_pages * 20 if max_pages is not None else None

            def merge_rows(rows: List[PlayerRow]) -> bool:
                for row in rows:
                    if row.player_id in seen_ids:
                        continue
                    seen_ids.add(row.player_id)
                    all_rows.append(row)
                    if limit is not None and len(all_rows) >= limit:
                        return True
                return False

            initial_links = await _list_initial_links(page)

            if not initial_links:
                merge_rows(await _paginate_current_tab(page, request_delay))
                return all_rows
            else:
                if merge_rows(await _paginate_current_tab(page, request_delay)) and limit is not None:
                    return all_rows
                index = 0
                while True:
                    current_links = await _list_initial_links(page)
                    if index >= len(current_links):
                        break

                    link = current_links[index]
                    prev_value = await _get_hfpage_value(page)
                    first_before = await _get_first_player_name(page)

                    clicked = await _trigger_postback(page, link)
                    if not clicked:
                        index += 1
                        continue

                    await _wait_updatepanel_idle(page, timeout=TIMEOUT_MS)
                    await _wait_after_navigation(page, prev_value, first_before, request_delay)

                    if merge_rows(await _paginate_current_tab(page, request_delay)) and limit is not None:
                        return all_rows

                    index += 1

            return all_rows

        finally:
            await ctx.close()
            await browser.close()


def parse_birth_date(raw: Optional[str]) -> Optional[date_type]:
    """
    Parse birth date from various formats:
    - YYYY-MM-DD, YYYY.MM.DD, YYYY/MM/DD
    - YYYYMMDD
    - Handles non-zero-padded dates (e.g., 1990.7.3)
    """
    if not raw:
        return None

    s = raw.strip().replace(" ", "")

    # Standard formats
    formats = (
        "%Y-%m-%d",
        "%Y.%m.%d",
        "%Y/%m/%d",
        "%Y%m%d",
        "%y-%m-%d",
        "%y.%m.%d",
        "%y/%m/%d",
    )

    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue

    # Handle non-zero-padded dates (e.g., 1990.7.3)
    try:
        parts = s.replace("-", ".").replace("/", ".").split(".")
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
            if 1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
                return datetime(y, m, d).date()
    except Exception:
        pass

    return None


def player_row_to_dict(row: PlayerRow) -> dict:
    """Convert PlayerRow to dictionary for database storage"""
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


async def main():
    """Main entry point for player crawler with database save"""
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

    # Crawl players
    print(f"\n🕷️  Crawling players (max_pages={args.max_pages or 'all'})...")
    players = await crawl_all_players(max_pages=args.max_pages)
    print(f"\n✅ Total players collected: {len(players)}")

    if not players:
        print("❌ No players collected")
        return

    # Show sample
    print("\n📋 Sample (first 5 players):")
    for p in players[:5]:
        print(f"  - {p.name} (ID: {p.player_id}, #{p.uniform_no}, {p.team}/{p.position})")

    supabase_url = os.getenv('SUPABASE_DB_URL')
    should_sync = args.sync_supabase if args.sync_supabase is not None else bool(supabase_url)

    if args.save or should_sync:
        from src.db.engine import init_db

        print("\n📦 Initializing database...")
        init_db()

    if args.save:
        from src.repositories.player_basic_repository import PlayerBasicRepository

        print("\n🔄 Processing player data...")
    player_dicts = [player_row_to_dict(p) for p in players]
    confirmer = PlayerStatusConfirmer()
    confirm_stats = await confirmer.confirm_entries([entry for entry in player_dicts if entry.get("status") in {"retired", "staff"}])
    if confirm_stats.get("confirmed"):
        print(f"\n🔎 Profile-confirmed statuses: {confirm_stats['confirmed']} (attempted {confirm_stats['attempted']})")
        parsed_dates = sum(1 for p in player_dicts if p['birth_date_date'] is not None)
        print(f"   - Parsed birth dates: {parsed_dates}/{len(player_dicts)}")

        print("\n💾 Saving to SQLite...")
        repo = PlayerBasicRepository()
        saved_count = repo.upsert_players(player_dicts)
        print(f"✅ Saved {saved_count} players to SQLite")
    else:
        print("\n⚠️  Skipping SQLite save (--no-save specified)")
        if should_sync:
            print("   - Existing SQLite data will be used for Supabase sync")

    if should_sync:
        from src.db.engine import SessionLocal
        from src.sync.supabase_sync import SupabaseSync

        if not supabase_url:
            print("\n❌ SUPABASE_DB_URL not set; skipping Supabase sync")
        else:
            print("\n🔄 Syncing to Supabase...")
            with SessionLocal() as sqlite_session:
                sync = SupabaseSync(supabase_url, sqlite_session)
                try:
                    if not sync.test_connection():
                        print("❌ Supabase connection failed")
                        return

                    synced = sync.sync_player_basic()
                    print(f"✅ Synced {synced} players to Supabase")
                finally:
                    sync.close()
    else:
        if args.sync_supabase is False:
            print("\n⚠️  Skipping Supabase sync (--no-sync-supabase specified)")
        elif not supabase_url:
            print("\nℹ️  SUPABASE_DB_URL not set; Supabase sync skipped")

    print("\n" + "=" * 60)
    print("✅ Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
