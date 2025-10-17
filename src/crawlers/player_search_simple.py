"""
Simple Player Search Crawler - Collects all 5120 players
1. Navigate to search page
2. Click search button to load all 5120 players
3. Wait for pagination to appear
4. Click "Next" button until no more pages
5. Collect all player data
"""
import asyncio
import re
import time
from dataclasses import dataclass
from datetime import date as date_type
from typing import List, Optional
from urllib.parse import urlparse, parse_qs

from playwright.async_api import async_playwright

# URL and selectors
SEARCH_URL = "https://www.koreabaseball.com/Player/Search.aspx"
SEARCH_INPUT = "input[id$='txtSearchPlayerName']"
SEARCH_BTN = "input[id$='btnSearch']"
TABLE_ROWS = "table.tEx tbody tr"
NEXT_BTN = "a[id$='ucPager_btnNext']"
NEXT_10_BTN = "a[id$='ucPager_btnNext10']"
PAGER_CONTAINER = "div.paging"

# Settings
REQUEST_DELAY = 2.0  # seconds between page clicks
TIMEOUT_MS = 15000


@dataclass
class PlayerRow:
    player_id: int
    name: str
    uniform_no: Optional[str]
    team: Optional[str]
    position: Optional[str]
    birth_date: Optional[str]
    birth_date_date: Optional[date_type]
    height_cm: Optional[int]
    weight_kg: Optional[int]
    career: Optional[str]


def _extract_player_id(href: Optional[str]) -> Optional[int]:
    """Extract player ID from URL, removing commas and non-digits"""
    if not href:
        return None

    try:
        q = parse_qs(urlparse(href).query)
        pid = q.get("playerId", [None])[0]
        if pid:
            # Remove all non-digit characters
            pid_clean = re.sub(r'[^\d]', '', str(pid))
            return int(pid_clean) if pid_clean.isdigit() else None
    except Exception:
        pass

    # Fallback regex
    try:
        m = re.search(r"playerId=([0-9,]+)", href)
        if m:
            pid_clean = m.group(1).replace(',', '')
            return int(pid_clean) if pid_clean.isdigit() else None
    except Exception:
        pass

    return None


def _parse_height_weight(s: str) -> tuple[Optional[int], Optional[int]]:
    """Parse '182cm, 76kg' or similar formats"""
    if not s:
        return None, None

    s_clean = s.replace(" ", "")
    # Try pattern with comma or slash: "182cm,76kg" or "182cm/76kg"
    m = re.search(r"(\d{2,3})cm[,/]?(\d{2,3})kg", s_clean, re.IGNORECASE)
    if m:
        h = int(m.group(1))
        w = int(m.group(2))
        # Sanity check
        if 140 <= h <= 220 and 45 <= w <= 150:
            return h, w

    return None, None


def _parse_birth_date(raw: Optional[str]) -> Optional[date_type]:
    """Parse birth date from various formats"""
    if not raw or raw == "-":
        return None

    # Try multiple formats
    formats = [
        "%Y-%m-%d",
        "%Y.%m.%d",
        "%Y/%m/%d",
        "%Y%m%d",
    ]

    for fmt in formats:
        try:
            dt = time.strptime(raw, fmt)
            return date_type(dt.tm_year, dt.tm_mon, dt.tm_mday)
        except (ValueError, AttributeError):
            continue

    return None


async def collect_page_rows(page) -> List[PlayerRow]:
    """Collect all player rows from current page"""
    rows = page.locator(TABLE_ROWS)
    count = await rows.count()

    results = []
    for i in range(count):
        row = rows.nth(i)
        tds = row.locator("td")

        try:
            # TD[0]: Uniform number
            uniform_no = (await tds.nth(0).inner_text()).strip()

            # TD[1]: Player name + link with playerId
            name_link = tds.nth(1).locator("a")
            name = (await name_link.inner_text()).strip()
            href = await name_link.get_attribute("href")
            player_id = _extract_player_id(href)

            if not player_id:
                continue

            # TD[2]: Team
            team = (await tds.nth(2).inner_text()).strip()

            # TD[3]: Position
            position = (await tds.nth(3).inner_text()).strip()

            # TD[4]: Birth date
            birth = (await tds.nth(4).inner_text()).strip()

            # TD[5]: Height/Weight
            body = (await tds.nth(5).inner_text()).strip()

            # TD[6]: Career
            career = (await tds.nth(6).inner_text()).strip()

            # Parse
            h, w = _parse_height_weight(body)
            birth_date_obj = _parse_birth_date(birth)

            # Normalize "-" to None
            results.append(PlayerRow(
                player_id=player_id,
                uniform_no=uniform_no if uniform_no != "-" and uniform_no != "##" else None,
                name=name,
                team=team if team != "-" else None,
                position=position if position != "-" else None,
                birth_date=birth if birth and birth != "-" else None,
                birth_date_date=birth_date_obj,
                height_cm=h,
                weight_kg=w,
                career=career if career != "-" else None
            ))
        except Exception as e:
            print(f"⚠️  Error parsing row {i}: {e}")
            continue

    return results


async def crawl_all_players(headless: bool = True) -> List[PlayerRow]:
    """
    Crawl all players following the exact steps:
    1. Navigate to search page
    2. Click search button
    3. Wait for pagination
    4. Click Next until no more pages
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        page = await browser.new_page()

        try:
            print("🌐 Step 1: Navigating to search page...")
            await page.goto(SEARCH_URL, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)

            print("🔍 Step 2: Entering '%' and clicking search button...")
            # Enter % to search for all players
            search_input = page.locator(SEARCH_INPUT)
            await search_input.fill("%")

            # Click search
            search_btn = page.locator(SEARCH_BTN)
            await search_btn.click()

            print("⏳ Step 3: Waiting for pagination to appear...")
            # Wait for results and pagination
            await page.wait_for_selector(TABLE_ROWS, timeout=TIMEOUT_MS)
            await page.wait_for_selector(PAGER_CONTAINER, timeout=TIMEOUT_MS)
            await page.wait_for_timeout(2000)

            print("📄 Step 4: Collecting data from all pages...")
            all_players = []
            page_num = 1

            while True:
                # Collect current page
                players = await collect_page_rows(page)
                all_players.extend(players)
                print(f"   Page {page_num}: collected {len(players)} players (total: {len(all_players)})")

                # Find all page number buttons in current pagination block
                pager = page.locator(PAGER_CONTAINER)
                page_buttons = pager.locator("a[id*='btnNo']")
                page_btn_count = await page_buttons.count()

                # Find which button is currently active (has class="on")
                current_btn_index = -1
                for i in range(page_btn_count):
                    btn_class = await page_buttons.nth(i).get_attribute("class")
                    if btn_class and "on" in btn_class:
                        current_btn_index = i
                        break

                # Try to click the next page number button in the current block
                clicked_next_page = False
                if current_btn_index >= 0 and current_btn_index + 1 < page_btn_count:
                    # There's a next page button in this block
                    next_page_btn = page_buttons.nth(current_btn_index + 1)
                    try:
                        # Get first player before click
                        first_player_before = ""
                        try:
                            first_link = page.locator(TABLE_ROWS).first.locator("td").nth(1).locator("a")
                            text = await first_link.inner_text()
                            first_player_before = text.strip()
                        except Exception:
                            pass

                        await next_page_btn.click(timeout=5000)
                        clicked_next_page = True

                        # Wait for page to change
                        await page.wait_for_timeout(2000)
                        page_changed = False
                        for _ in range(10):
                            await asyncio.sleep(1)
                            try:
                                first_link = page.locator(TABLE_ROWS).first.locator("td").nth(1).locator("a")
                                text = await first_link.inner_text()
                                first_player_after = text.strip()
                                if first_player_after != first_player_before:
                                    page_changed = True
                                    break
                            except Exception:
                                pass

                        if not page_changed:
                            print(f"⚠️  Page number click didn't change content")
                            clicked_next_page = False

                    except Exception as e:
                        print(f"⚠️  Failed to click page number button: {e}")
                        clicked_next_page = False

                if clicked_next_page:
                    page_num += 1
                    time.sleep(REQUEST_DELAY)
                    continue

                # No more page buttons in current block, try "Next" button to go to next block
                next_block_btn = page.locator(NEXT_BTN)
                if await next_block_btn.count() == 0:
                    print("✅ No Next button - reached last page")
                    break

                try:
                    # Get first player before click
                    first_player_before = ""
                    try:
                        first_link = page.locator(TABLE_ROWS).first.locator("td").nth(1).locator("a")
                        text = await first_link.inner_text()
                        first_player_before = text.strip()
                    except Exception:
                        pass

                    await next_block_btn.first.click(timeout=5000)

                    # Wait for page to change
                    await page.wait_for_timeout(3000)
                    page_changed = False
                    for _ in range(15):
                        await asyncio.sleep(1)
                        try:
                            first_link = page.locator(TABLE_ROWS).first.locator("td").nth(1).locator("a")
                            text = await first_link.inner_text()
                            first_player_after = text.strip()
                            if first_player_after != first_player_before:
                                page_changed = True
                                break
                        except Exception:
                            pass

                    if not page_changed:
                        print("✅ Next block button didn't change page - reached last page")
                        break

                    page_num += 1
                    time.sleep(REQUEST_DELAY)

                except Exception as e:
                    print(f"✅ Failed to click Next block button: {e} - reached last page")
                    break

            print(f"\n✅ Step 5: Collection complete!")
            print(f"   Total players collected: {len(all_players)}")

            return all_players

        finally:
            await browser.close()


async def main():
    print("=" * 60)
    print("KBO Player Search Crawler (Simple)")
    print("=" * 60)
    print()

    players = await crawl_all_players(headless=True)

    print(f"\n📊 Summary:")
    print(f"   Total players: {len(players)}")
    print(f"   Unique player IDs: {len(set(p.player_id for p in players))}")

    # Sample
    print(f"\n📋 Sample (first 5 players):")
    for p in players[:5]:
        print(f"   - {p.name} (ID: {p.player_id}, #{p.uniform_no or '##'}, {p.team or 'N/A'}/{p.position or 'N/A'})")


if __name__ == "__main__":
    asyncio.run(main())
