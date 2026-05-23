"""
KBO Staff Register Crawler
Collects manager (감독) and coach (코치) data from
https://www.koreabaseball.com/Player/Register.aspx

This page shows the current day's 1st-team registered roster by team,
with separate tables for 감독, 코치, and players by position.
Each 감독/코치 row contains a playerId link from which we extract the KBO player ID.

Usage:
    python3 -m src.cli.crawl_staff_register --all-teams
    python3 -m src.cli.crawl_staff_register --team LG
"""
from __future__ import annotations

import asyncio
import re
from datetime import datetime, date as date_type
from typing import Dict, List, Optional, Tuple

from playwright.async_api import async_playwright, Page, BrowserContext

from src.utils.safe_print import safe_print as print
from src.utils.team_codes import resolve_team_code
from src.utils.request_policy import RequestPolicy

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REGISTER_URL = "https://www.koreabaseball.com/Player/Register.aspx"

# Team codes used by KBO site -> canonical team code mapping seed
KBO_TEAM_MAP: Dict[str, str] = {
    "SS": "SS",  # 삼성
    "KT": "KT",
    "LG": "LG",
    "HT": "HT",  # KIA
    "SK": "SK",  # SSG
    "OB": "OB",  # 두산
    "HH": "HH",  # 한화
    "WO": "WO",  # 키움
    "LT": "LT",  # 롯데
    "NC": "NC",
}

# Tables: index 0 = 감독, index 1 = 코치
TABLE_ROLE_MAP: Dict[int, str] = {0: "manager", 1: "coach"}

# Regex to extract playerId from URLs like
# /Record/Player/HitterDetail/Basic.aspx?playerId=91350
# /Record/Retire/Hitter.aspx?playerId=96340
PLAYERID_RE = re.compile(r"playerId=(\d+)", re.IGNORECASE)

# Physical stat patterns
HW_RE = re.compile(r"(\d+)cm,\s*(\d+)kg")
BIRTH_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

# Hand parsing
HAND_MAP = {"우": "R", "좌": "L", "양": "S"}
HAND_RE = re.compile(r"(.)[투](.)타")


def _parse_player_id(href: Optional[str]) -> Optional[int]:
    if not href:
        return None
    m = PLAYERID_RE.search(href)
    return int(m.group(1)) if m else None


def _parse_hw(text: str) -> Tuple[Optional[int], Optional[int]]:
    m = HW_RE.search(text.replace(" ", ""))
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


def _parse_birth_date(text: str) -> Optional[date_type]:
    m = BIRTH_RE.search(text)
    if m:
        try:
            return date_type(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    return None


def _parse_hands(text: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse throws/bats from '우투우타' style string."""
    m = HAND_RE.search(text)
    if m:
        throws = HAND_MAP.get(m.group(1))
        bats = HAND_MAP.get(m.group(2))
        return throws, bats
    return None, None


# ---------------------------------------------------------------------------
# Core extraction JS – runs in browser context
# ---------------------------------------------------------------------------
_EXTRACT_JS = """
(teamCode) => {
    const results = [];

    // Locate the registration list title for this team
    const h4s = Array.from(document.querySelectorAll('h4'));
    const listTitle = h4s.find(h => h.innerText.includes('선수등록명단'));
    if (!listTitle) return results;

    // Walk siblings to collect tables in order
    const tables = [];
    let next = listTitle.nextElementSibling;
    while (next && next.tagName !== 'H4') {
        if (next.tagName === 'TABLE') {
            tables.push(next);
        }
        next = next.nextElementSibling;
    }

    // Table index 0 = 감독, index 1 = 코치
    [0, 1].forEach(idx => {
        const table = tables[idx];
        if (!table) return;

        const role = idx === 0 ? 'manager' : 'coach';
        const rows = Array.from(table.rows);

        // Skip header row (rows[0])
        for (let i = 1; i < rows.length; i++) {
            const cells = Array.from(rows[i].cells);
            if (cells.length < 2) continue;

            const nameCell = cells[1];
            const link = nameCell.querySelector('a');
            const href = link ? link.getAttribute('href') : null;
            const name = nameCell.innerText.trim();

            // Skip "no registered" placeholder rows
            if (name.includes('없습니다') || name.includes('등록된')) continue;

            const uniformNo = cells[0] ? cells[0].innerText.trim() : null;
            const handText = cells[2] ? cells[2].innerText.trim() : '';
            const birthText = cells[3] ? cells[3].innerText.trim() : '';
            const physicalText = cells[4] ? cells[4].innerText.trim() : '';

            results.push({
                staff_role: role,
                player_id_str: null,  // filled in Python
                href: href,
                name: name,
                uniform_no: uniformNo,
                hand_text: handText,
                birth_text: birthText,
                physical_text: physicalText,
            });
        }
    });

    return results;
}
"""


class StaffRegisterCrawler:
    """
    Scrapes Register.aspx to collect manager and coach records.

    Returns a list of dicts suitable for upsert into player_basic:
    {
        player_id, name, uniform_no, team, birth_date, birth_date_date,
        height_cm, weight_kg, throws, bats,
        status, staff_role, status_source
    }
    """

    def __init__(self, headless: bool = True, request_delay: float = 1.5):
        self.headless = headless
        self.policy = RequestPolicy(min_delay=request_delay, max_delay=request_delay)

    async def crawl_team(
        self,
        page: Page,
        kbo_team_code: str,
        team_display_name: Optional[str] = None,
    ) -> List[Dict]:
        """Crawl a single team's staff registration page."""
        await self.policy.delay_async(host="www.koreabaseball.com")
        await page.evaluate(f"fnSearchChange('{kbo_team_code}')")
        await page.wait_for_timeout(1500)

        raw_rows = await page.evaluate(_EXTRACT_JS, kbo_team_code)

        records = []
        for row in raw_rows:
            player_id = _parse_player_id(row.get("href"))
            name = (row.get("name") or "").strip()
            if not name or name.startswith("당일"):
                continue

            uniform_no = (row.get("uniform_no") or "").strip() or None
            hand_text = row.get("hand_text", "")
            birth_text = row.get("birth_text", "")
            physical_text = row.get("physical_text", "")

            throws, bats = _parse_hands(hand_text)
            birth_date_obj = _parse_birth_date(birth_text)
            height_cm, weight_kg = _parse_hw(physical_text)

            # Resolve team code to canonical
            canonical_team = resolve_team_code(kbo_team_code) or kbo_team_code

            records.append(
                {
                    "player_id": player_id,
                    "name": name,
                    "uniform_no": uniform_no,
                    "team": canonical_team,
                    "birth_date": birth_text if birth_text else None,
                    "birth_date_date": birth_date_obj,
                    "height_cm": height_cm,
                    "weight_kg": weight_kg,
                    "throws": throws,
                    "bats": bats,
                    "status": "staff",
                    "staff_role": row["staff_role"],  # 'manager' | 'coach'
                    "status_source": "register",
                }
            )

        print(
            f"  [{kbo_team_code}] Found {len(records)} staff "
            f"({'→'.join(str(r['player_id']) for r in records[:3])}...)"
        )
        return records

    async def crawl_all_teams(
        self, team_codes: Optional[List[str]] = None
    ) -> List[Dict]:
        """Crawl all (or specified) teams and return combined staff records."""
        targets = team_codes or list(KBO_TEAM_MAP.keys())
        all_records: List[Dict] = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context: BrowserContext = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
                locale="ko-KR",
                timezone_id="Asia/Seoul",
            )
            page = await context.new_page()

            try:
                print(f"🌍 Navigating to {REGISTER_URL} ...")
                await page.goto(
                    REGISTER_URL, wait_until="domcontentloaded", timeout=30000
                )
                await page.wait_for_timeout(1500)

                for code in targets:
                    try:
                        records = await self.crawl_team(page, code)
                        all_records.extend(records)
                    except Exception as exc:
                        print(f"  ⚠️  Error crawling team {code}: {exc}")

            finally:
                await browser.close()

        return all_records

    def save_to_db(
        self,
        records: List[Dict],
        *,
        dry_run: bool = False,
    ) -> int:
        """
        Upsert staff records into player_basic.

        - Records WITHOUT a player_id (no profile link found): skipped with warning.
        - Records WITH a player_id: upsert with status='staff', staff_role,
          status_source='register'.  Physical/bio fields are also updated.
        """
        from src.repositories.player_basic_repository import PlayerBasicRepository

        valid = [r for r in records if r.get("player_id")]
        skipped = [r for r in records if not r.get("player_id")]

        if skipped:
            print(
                f"  ⚠️  {len(skipped)} record(s) skipped (no player_id): "
                + ", ".join(r["name"] for r in skipped)
            )

        if dry_run:
            print(f"  [DRY-RUN] Would upsert {len(valid)} staff record(s) into player_basic.")
            for r in valid:
                print(f"    → {r['name']} ({r['staff_role']}) pid={r['player_id']} team={r['team']}")
            return len(valid)

        if not valid:
            print("  ℹ️  No valid staff records to save.")
            return 0

        repo = PlayerBasicRepository()
        count = repo.upsert_players(valid)
        print(f"  ✅ Upserted {count} staff record(s) into player_basic.")
        return count


async def main():
    """Quick standalone test – print staff for LG and Kiwoom."""
    crawler = StaffRegisterCrawler(headless=True)
    records = await crawler.crawl_all_teams(team_codes=["LG", "WO"])
    print(f"\nTotal staff records collected: {len(records)}")
    for r in records:
        print(
            f"  [{r['staff_role'].upper()}] {r['name']} "
            f"(pid={r['player_id']}, team={r['team']}, "
            f"birth={r['birth_date']}, {r['height_cm']}cm/{r['weight_kg']}kg)"
        )


if __name__ == "__main__":
    asyncio.run(main())
