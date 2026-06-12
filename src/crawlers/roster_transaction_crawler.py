"""
Crawler for daily roster transactions (call-up / send-down).
Sources:
  - KBO mobile registration page: https://m.koreabaseball.com/Kbo/PlayerAdd.aspx
  - KBO player register page: https://www.koreabaseball.com/Player/Register.aspx
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import date, datetime
from typing import Any
from urllib.parse import urlparse

import httpx
from playwright.async_api import Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.db.engine import SessionLocal
from src.repositories.roster_transaction_repository import RosterTransactionRepository
from src.repositories.source_registry_repository import save_raw_snapshots
from src.urls import REGISTER
from src.utils.http_client import DEFAULT_HEADERS as HEADERS
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.playwright_retry import NAV_TIMEOUT, SHORT_TIMEOUT
from src.utils.throttle import throttle

logger = logging.getLogger(__name__)

TEAM_CODES = [
    ("LG", "LG"),
    ("HH", "HH"),
    ("SS", "SS"),
    ("KT", "KT"),
    ("OB", "OB"),
    ("LT", "LT"),
    ("HT", "HT"),
    ("NC", "NC"),
    ("SK", "SK"),
    ("WO", "WO"),
]


class RosterTransactionCrawler:
    def __init__(self, request_delay: float = 1.0, pool: AsyncPlaywrightPool | None = None) -> None:
        self.mobile_url = "https://m.koreabaseball.com/Kbo/PlayerAdd.aspx"
        self.register_url = REGISTER
        self.request_delay = request_delay
        self.pool = pool
        self._raw_pages: list[dict] = []

    async def run(self, save: bool = False, target_date: str | None = None) -> list[dict[str, Any]]:
        crawl_date = datetime.strptime(target_date, "%Y-%m-%d").date() if target_date else date.today()

        # Try mobile page first (simpler, structured)
        data = await self._crawl_mobile_page(crawl_date)
        if not data:
            # Fallback to desktop page via Playwright
            data = await self._crawl_desktop_page(crawl_date)

        logger.info("[ROSTER] %s: %s transactions found", crawl_date, len(data))
        if save:
            self._save_to_db(data)
        else:
            for d in data[:10]:
                logger.info(d)

        return data

    async def _crawl_mobile_page(self, target_date: date) -> list[dict[str, Any]]:
        url = f"{self.mobile_url}?searchDate={target_date.strftime('%Y-%m-%d')}"
        async with httpx.AsyncClient(headers=HEADERS, timeout=15, follow_redirects=True) as client:
            try:
                host = urlparse(url).hostname or "koreabaseball.com"
                await throttle.wait(host)
                resp = await client.get(url)
                if resp.status_code != 200:
                    return []
                html = resp.text
                self._raw_pages.append(
                    {
                        "source_key": "kbo_today_roster",
                        "url": url,
                        "html": html,
                        "status_code": resp.status_code,
                    },
                )
            except httpx.HTTPError:
                logger.exception("Mobile roster page fetch failed")
                return []

        return self._parse_mobile_html(html, target_date)

    def _parse_mobile_html(self, html: str, target_date: date) -> list[dict[str, Any]]:
        """Parse the mobile KBO registration page."""
        transactions = []

        # Split into registered and deregistered sections
        registered_section = ""
        deregistered_section = ""

        # Find sections: "오늘자 선수 등록현황" and "오늘자 선수 말소현황"
        reg_match = re.search(r"오늘자\s*선수\s*등록현황.*?(?=오늘자\s*선수\s*말소현황|\Z)", html, re.DOTALL)
        if reg_match:
            registered_section = reg_match.group(0)

        dereg_match = re.search(r"오늘자\s*선수\s*말소현황.*?(?=<div\s+(?:class|id)=|$)", html, re.DOTALL)
        if dereg_match:
            deregistered_section = dereg_match.group(0)

        if not registered_section and not deregistered_section:
            # Try alternate layout
            return self._parse_alternate_mobile(html, target_date)

        for section_text, action in [(registered_section, "registered"), (deregistered_section, "deregistered")]:
            if not section_text:
                continue

            # Find team blocks within section
            team_blocks = re.findall(
                r'<strong[^>]*class="team"[^>]*>([^<]+)</strong>\s*<ul[^>]*>(.*?)</ul>',
                section_text,
                re.DOTALL,
            )
            for team_name_raw, list_html in team_blocks:
                team_code = self._map_team_name(team_name_raw.strip())
                if not team_code:
                    continue

                # Extract player names and IDs from list items
                player_items = re.findall(
                    r'<li[^>]*>(?:\s*<a[^>]*href="[^"]*playerId=(\d+)[^"]*"[^>]*>)?\s*([^<]+?)\s*(?:</a>)?\s*</li>',
                    list_html,
                )
                for player_id_str, player_name in player_items:
                    player_name = player_name.strip()
                    if not player_name or player_name == "":
                        continue
                    transactions.append(
                        {
                            "transaction_date": target_date,
                            "team_id": team_code,
                            "player_id": int(player_id_str) if player_id_str and player_id_str.isdigit() else None,
                            "player_name": player_name,
                            "action": action,
                            "roster_level": "first_team",
                            "inferred_to_level": "second_team" if action == "deregistered" else None,
                            "source_type": "kbo_today_page",
                            "confidence": "high",
                            "dedupe_key": f"{target_date}_{team_code}_{player_name}_{action}",
                        },
                    )

        return transactions

    def _parse_alternate_mobile(self, html: str, target_date: date) -> list[dict[str, Any]]:
        """Fallback parser for alternate mobile page layout."""
        transactions = []

        # Look for table-based layout
        current_team = None
        current_action = None

        for line in html.split("\n"):
            line = line.strip()
            team_match = re.search(r'class="team"[^>]*>\s*([^<]+)', line)
            if team_match:
                current_team = self._map_team_name(team_match.group(1).strip())
                continue

            if "등록" in line and ("선수" in line or "현황" in line):
                current_action = "registered"
                continue
            if "말소" in line and ("선수" in line or "현황" in line):
                current_action = "deregistered"
                continue

            if current_team and current_action:
                player_match = re.search(r"playerId=(\d+)[^>]*>\s*([^<]+)", line)
                if player_match:
                    pid, pname = int(player_match.group(1)), player_match.group(2).strip()
                    transactions.append(
                        {
                            "transaction_date": target_date,
                            "team_id": current_team,
                            "player_id": pid,
                            "player_name": pname,
                            "action": current_action,
                            "roster_level": "first_team",
                            "inferred_to_level": "second_team" if current_action == "deregistered" else None,
                            "source_type": "kbo_today_page",
                            "confidence": "high",
                            "dedupe_key": f"{target_date}_{current_team}_{pname}_{current_action}",
                        },
                    )

        return transactions

    async def _crawl_desktop_page(self, target_date: date) -> list[dict[str, Any]]:
        """Fallback: crawl the desktop ASP.NET page."""
        transactions = []

        pool = self.pool or AsyncPlaywrightPool(max_pages=1)
        owns_pool = self.pool is None
        await pool.start()

        try:
            page = await pool.acquire()
            try:
                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(3),
                    wait=wait_exponential(multiplier=1, min=2, max=10),
                    retry=retry_if_exception_type(PlaywrightTimeoutError),
                ):
                    with attempt:
                        await page.goto(self.register_url, wait_until="networkidle", timeout=NAV_TIMEOUT)

                date_str = target_date.strftime("%Y%m%d")
                await page.evaluate(
                    f"document.getElementById('cphContents_cphContents_cphContents_hfSearchDate').value = '{date_str}';",
                )
                try:
                    async with page.expect_response(lambda r: "Register.aspx" in r.url, timeout=SHORT_TIMEOUT):
                        await page.evaluate(
                            "__doPostBack('ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$btnCalendarSelect', '')",
                        )
                except TimeoutError:
                    logger.warning("Calendar select postback timeout, continuing")
                await page.wait_for_timeout(2000)

                desktop_html = await page.content()
                self._raw_pages.append(
                    {
                        "source_key": "kbo_player_register",
                        "url": self.register_url,
                        "html": desktop_html,
                        "status_code": 200,
                    },
                )

                for site_code, db_code in TEAM_CODES:
                    try:
                        await page.evaluate(f"fnSearchChange('{site_code}')")
                        await page.wait_for_timeout(500)
                        daily = await self._extract_desktop_roster(page, db_code, target_date)
                        transactions.extend(daily)
                    except Exception:
                        logger.exception("Desktop roster team %s failed", site_code)

            finally:
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()

        return transactions

    async def _extract_desktop_roster(self, page: Page, team_code: str, roster_date: date) -> list[dict[str, Any]]:
        script = """
        () => {
            const results = [];
            const tables = document.querySelectorAll('#cphContents_cphContents_cphContents_udpRecord table.tNData');
            if (tables.length === 0) return [];
            tables.forEach(table => {
                const rows = table.querySelectorAll('tbody tr');
                rows.forEach(tr => {
                    const cells = tr.querySelectorAll('td');
                    if (cells.length < 4) return;
                    const nameLink = cells[1].querySelector('a');
                    const name = cells[1].innerText.trim();
                    if (!nameLink || !name) return;
                    const href = nameLink.getAttribute('href');
                    const url = new URL(href, window.location.origin);
                    const playerId = url.searchParams.get('playerId');
                    if (playerId) {
                        results.push({ player_id: playerId, player_name: name });
                    }
                });
            });
            return results;
        }
        """
        data = await page.evaluate(script)
        transactions = []
        for item in data:
            transactions.append(  # noqa: PERF401
                {
                    "transaction_date": roster_date,
                    "team_id": team_code,
                    "player_id": int(item["player_id"]),
                    "player_name": item["player_name"],
                    "action": "registered",
                    "roster_level": "first_team",
                    "source_type": "kbo_today_page",
                    "confidence": "high",
                    "dedupe_key": f"{roster_date}_{team_code}_{item['player_name']}_registered",
                },
            )
        return transactions

    def _map_team_name(self, name: str) -> str | None:
        mapping = {
            "LG": "LG",
            "lg": "LG",
            "엘지": "LG",
            "HH": "HH",
            "한화": "HH",
            "SS": "SS",
            "삼성": "SS",
            "KT": "KT",
            "kt": "KT",
            "OB": "OB",
            "두산": "OB",
            "LT": "LT",
            "롯데": "LT",
            "HT": "HT",
            "KIA": "HT",
            "기아": "HT",
            "NC": "NC",
            "SK": "SK",
            "SSG": "SK",
            "WO": "WO",
            "키움": "WO",
        }
        return mapping.get(name)

    def _save_to_db(self, data: list[dict]) -> None:
        with SessionLocal() as session:
            try:
                saved_snaps = save_raw_snapshots(session, self._raw_pages)
                repo = RosterTransactionRepository(session)
                count = 0
                for item in self._dedupe_transactions(data):
                    try:
                        repo.save(item)
                        count += 1
                    except Exception:
                        logger.exception("Roster transaction save failed: %s", item.get("dedupe_key", ""))
                session.commit()
                logger.info("[ROSTER] Saved %s transaction records, %s snapshots.", count, saved_snaps)
            except Exception:
                session.rollback()
                logger.exception("Roster batch save error")
            finally:
                self._raw_pages.clear()

    def _dedupe_transactions(self, data: list[dict]) -> list[dict]:
        seen = set()
        deduped = []
        for item in data:
            key = item.get("dedupe_key")
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            deduped.append(item)
        return deduped


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    parser.add_argument("--date", type=str, default=None, help="Target date (YYYY-MM-DD)")
    args = parser.parse_args()
    asyncio.run(RosterTransactionCrawler().run(save=args.save, target_date=args.date))
