"""
Futures League Pitching Stats Crawler
Fetches year-by-year Futures pitching statistics from player profile pages.
"""
import asyncio
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.compliance import compliance
from src.utils.throttle import throttle
from src.utils.team_codes import resolve_kbo_legacy_team_code

FUTURES_PITCHER_KEYS = [
    "season", "era", "games", "complete_games", "shutouts", "wins", "losses", "saves", "holds",
    "tbf", "innings_pitched", "innings_outs", "hits_allowed", "home_runs_allowed",
    "walks_allowed", "hit_batters", "strikeouts", "runs_allowed", "earned_runs", "team_code"
]

HEADER_MAP = {
    "연도": "season", "년도": "season", "시즌": "season", "year": "season",
    "팀명": "team_name", "팀": "team_name",
    "era": "era", "평균자책": "era", "평균자책점": "era",
    "경기": "games", "g": "games",
    "완투": "complete_games", "cg": "complete_games",
    "완봉": "shutouts", "sho": "shutouts",
    "승": "wins", "w": "wins",
    "패": "losses", "l": "losses",
    "세": "saves", "sv": "saves", "세이브": "saves",
    "홀드": "holds", "hld": "holds",
    "타자": "tbf", "tbf": "tbf",
    "이닝": "IP", "ip": "IP",
    "피안타": "hits_allowed", "h": "hits_allowed",
    "피홈런": "home_runs_allowed", "hr": "home_runs_allowed",
    "볼넷": "walks_allowed", "bb": "walks_allowed",
    "사구": "hit_batters", "hbp": "hit_batters",
    "삼진": "strikeouts", "so": "strikeouts",
    "실점": "runs_allowed", "r": "runs_allowed",
    "자책": "earned_runs", "er": "earned_runs",
}


def _norm_header(txt: str) -> str:
    """Normalize header text to standard key."""
    t = re.sub(r"\s+", "", txt).lower()
    return HEADER_MAP.get(t, txt.strip())


def _to_int(x: Optional[str]) -> Optional[int]:
    """Convert string to integer, handling commas and dashes."""
    if x is None:
        return None
    t = x.strip().replace(",", "")
    if t in ("", "-", "—", "null"):
        return None
    try:
        return int(re.sub(r"[^\d-]", "", t))
    except (ValueError, AttributeError):
        return None


def _to_float(x: Optional[str]) -> Optional[float]:
    """Convert string to float, handling commas and dashes."""
    if x is None:
        return None
    t = x.strip().replace(",", "")
    if t in ("", "-", "—", "null"):
        return None
    t = re.sub(r"[^\d\.]", "", t)
    try:
        return float(t) if t else None
    except (ValueError, AttributeError):
        return None


def parse_innings_to_outs(text: Optional[str]) -> Optional[int]:
    """
    Parse pitching innings string to total outs.
    Supports fractions (e.g. "4 2/3", "2/3"), unicode ("4 ⅓"), decimals ("4.1", "4.2") or integers ("4").
    """
    if not text:
        return None
    cleaned = str(text).strip()
    if cleaned in ("", "-", "—"):
        return None

    # Replace unicode fractions with spaces
    cleaned = cleaned.replace('⅓', ' 1/3').replace('⅔', ' 2/3')

    # 1. Match whole number + fraction: e.g. "4 2/3"
    m_frac = re.match(r"^(\d+)\s+(\d+)/(\d+)$", cleaned)
    if m_frac:
        whole = int(m_frac.group(1))
        num = int(m_frac.group(2))
        den = int(m_frac.group(3))
        frac_outs = int(round(num * 3 / den))
        return whole * 3 + frac_outs

    # 2. Match fraction only: e.g. "2/3"
    m_frac_only = re.match(r"^(\d+)/(\d+)$", cleaned)
    if m_frac_only:
        num = int(m_frac_only.group(1))
        den = int(m_frac_only.group(2))
        return int(round(num * 3 / den))

    # 3. Match KBO style decimal: "4.1" or "4.2" or integer "4"
    match = re.match(r"^(\d+)(?:\.(\d))?$", cleaned)
    if match:
        whole = int(match.group(1))
        frac = int(match.group(2)) if match.group(2) else 0
        return whole * 3 + frac

    # 4. Fallback to normal float conversion
    try:
        value = float(cleaned)
        return int(round(value * 3))
    except ValueError:
        return None


def _parse_table(table) -> List[Dict]:
    """Parse a table element into list of season pitching records."""
    headers = [_norm_header(th.get_text(strip=True)) for th in table.select("thead th, thead td")]

    if not headers:
        first_row = table.find("tr")
        if first_row:
            headers = [_norm_header(cell.get_text(strip=True)) for cell in first_row.find_all(["th", "td"])]

    out = []
    for tr in table.select("tbody tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if not cells:
            continue

        # Skip total/summary rows
        if any(keyword in " ".join(cells).lower() for keyword in ["통산", "합계", "career", "total"]):
            continue

        row = {}
        for h, v in zip(headers, cells):
            key = _norm_header(h)

            if key == "season":
                m = re.search(r"\d{4}", v)
                if not m:
                    row["season"] = None
                else:
                    row["season"] = int(m.group())
            elif key == "team_name":
                row["team_name"] = v
            elif key == "era":
                row["era"] = _to_float(v)
            elif key == "IP":
                row["IP"] = v
            elif key in ("games", "complete_games", "shutouts", "wins", "losses", "saves", "holds",
                         "tbf", "hits_allowed", "home_runs_allowed", "walks_allowed", "hit_batters",
                         "strikeouts", "runs_allowed", "earned_runs"):
                row[key] = _to_int(v)

        # Skip rows without valid season
        season = row.get("season")
        if not season:
            continue

        # Parse innings and outs
        ip_str = row.get("IP")
        outs = parse_innings_to_outs(ip_str)
        row["innings_outs"] = outs
        row["innings_pitched"] = round(outs / 3.0, 3) if outs is not None else None

        # Resolve team code
        team_name = row.get("team_name")
        row["team_code"] = resolve_kbo_legacy_team_code(team_name, season_year=season)

        out.append(row)

    return out


def _pick_futures_pitching_table(soup: BeautifulSoup):
    """Find the Futures pitching record table."""
    # Method 1: Find by ID
    tbl = soup.find("table", id="tblPitcherRecord")
    if tbl:
        return tbl

    # Method 2: Header-based heuristic
    for t in soup.find_all("table"):
        headers = [_norm_header(th.get_text(strip=True)) for th in t.select("thead th, thead td")]
        if not headers:
            continue
        if {"season", "era", "games", "wins", "losses"}.issubset(set(headers)):
            return t

    return None


async def fetch_and_parse_futures_pitching(
    player_id: str,
    profile_url: str,
    pool: Optional[AsyncPlaywrightPool] = None,
) -> List[Dict]:
    """
    Fetch Futures pitching stats from player profile page.
    """
    active_pool = pool or AsyncPlaywrightPool(max_pages=1, context_kwargs={"locale": "ko-KR"})
    owns_pool = pool is None
    await active_pool.start()
    try:
        page = await active_pool.acquire()
        try:
            if not await compliance.is_allowed(profile_url):
                print(f"[COMPLIANCE] Blocked futures pitching: {profile_url}")
                return []

            await throttle.wait()
            await page.goto(profile_url, wait_until="domcontentloaded", timeout=60000)
            await throttle.wait()

            try:
                futures_tab = await page.wait_for_selector('text="퓨처스"', timeout=3000)
                if futures_tab:
                    await futures_tab.click()
                    await throttle.wait()
            except Exception:
                pass

            html = await page.content()
        finally:
            await active_pool.release(page)
    finally:
        if owns_pool:
            await active_pool.close()

    soup = BeautifulSoup(html, "lxml")
    table = _pick_futures_pitching_table(soup)

    if not table:
        return []

    rows = _parse_table(table)

    # Trim to only requested keys, filling missing with None
    trimmed = []
    for r in rows:
        item = {k: r.get(k) for k in FUTURES_PITCHER_KEYS}
        trimmed.append(item)

    return trimmed
