"""Wikipedia-backed KBO rookie draft crawler (2차 지명 tables).

Yearly pages (``{YYYY}년 KBO 리그 신인 드래프트``) carry one sortable
wikitable: rows are 2차 rounds, columns are teams, cells hold
``이름(학교,포지션)(계약금:...)``. Reuses the rowspan-safe table renderer
from the award crawler.
"""

from __future__ import annotations

import logging
import re

import httpx
from bs4 import BeautifulSoup

from src.crawlers.award_crawler import WIKI_USER_AGENT, AwardCrawler

logger = logging.getLogger(__name__)

WIKI_API_URL = "https://ko.wikipedia.org/w/api.php"
WIKI_TIMEOUT_SECONDS = 30.0

MIN_ROUND_TABLE_COLUMNS = 5
MIN_FIRST_PICK_COLUMNS = 4
FIRST_PICK_FEE_COLUMN = 4

_CELL_RE = re.compile(r"^(?P<name>.+?)\((?P<school>[^,()]+),(?P<position>[^()]+)\)(?:\(계약금:(?P<fee>[^)]+)\))?$")
_DISAMBIGUATION_RE = re.compile(r"\s*\(\d{4}년\)\s*$")
_ROUND_RE = re.compile(r"^\d+$")


def _clean_name(raw: str) -> str:
    """Strip parenthetical disambiguation markers from a player name."""
    return _DISAMBIGUATION_RE.sub("", raw).strip()


def parse_draft_cell(cell: str) -> dict[str, str | None] | None:
    """Parse a draft table cell into name/school/position/fee fields."""
    text = " ".join(cell.split())
    if not text or text in {"-", "지명권 없음"}:
        return None
    match = _CELL_RE.match(text)
    if not match:
        return {"player_name": _clean_name(text), "school": None, "position": None, "sign_fee": None}
    return {
        "player_name": _clean_name(match.group("name")),
        "school": match.group("school").strip(),
        "position": match.group("position").strip(),
        "sign_fee": match.group("fee").strip() if match.group("fee") else None,
    }


def _round_order_even_reversed(round_num: int, n_teams: int) -> list[int]:
    """Return column indexes in pick order (snake: even rounds reversed)."""
    order = list(range(n_teams))
    if round_num % 2 == 0:
        order.reverse()
    return order


def _is_round_table(headers: list[str], rows: list[list[str]]) -> bool:
    """Detect a 2차-style table (round numbers in the first column)."""
    if len(headers) < MIN_ROUND_TABLE_COLUMNS or not rows:
        return False
    return bool(_ROUND_RE.match(rows[0][0].strip()))


def _is_first_pick_table(headers: list[str]) -> bool:
    """Detect a 1차-style table (team/school/position columns)."""
    joined = "".join(headers)
    return "구단" in joined


class DraftWikiCrawler:
    """Crawl yearly KBO rookie draft 2차 tables from Korean Wikipedia."""

    def __init__(self, request_delay: float = 1.0) -> None:
        """Initialize the crawler."""
        self.request_delay = request_delay

    async def fetch_season_table(self, season: int) -> tuple[list[str], list[list[str]]]:
        """Fetch and render the 2차 draft table for a season."""
        headers, rows, _ = await self.fetch_season_tables(season)
        return headers, rows

    async def fetch_season_tables(
        self,
        season: int,
    ) -> tuple[list[str], list[list[str]], list[list[str]]]:
        """Fetch 2차 table plus 1차 rows (both may be absent per season)."""
        titles = [
            f"{season}년 KBO 리그 신인 드래프트",
            f"{season}년 한국프로야구 신인 드래프트",
        ]
        async with httpx.AsyncClient(headers={"User-Agent": WIKI_USER_AGENT}, timeout=WIKI_TIMEOUT_SECONDS) as client:
            payload: dict = {"error": {"info": "no title attempted"}}
            for title in titles:
                response = await client.get(
                    WIKI_API_URL,
                    params={"action": "parse", "page": title, "format": "json", "prop": "text", "redirects": 1},
                )
                response.raise_for_status()
                payload = response.json()
                if "error" not in payload:
                    break
        if "error" in payload:
            logger.warning("Wikipedia page missing for season %d.", season)
            return [], [], []
        soup = BeautifulSoup(payload["parse"]["text"]["*"], "html.parser")
        tables = soup.find_all("table", class_="wikitable")
        second_headers: list[str] = []
        second_rows: list[list[str]] = []
        first_rows: list[list[str]] = []
        for table in tables:
            headers, rows = AwardCrawler._render_table(table)  # noqa: SLF001
            if not headers or not rows:
                continue
            if _is_round_table(headers, rows):
                second_headers, second_rows = headers, rows
            elif _is_first_pick_table(headers):
                first_rows = rows
        return second_headers, second_rows, first_rows

    def parse_season_records(
        self, season: int, headers: list[str], rows: list[list[str]], team_resolver: object = None
    ) -> list[dict[str, object]]:
        """Convert a rendered draft table into repository-ready records."""
        from src.utils.team_codes import resolve_team_code

        records: list[dict[str, object]] = []
        team_names = headers[1:]
        for row in rows:
            if not row or not _ROUND_RE.match(row[0].strip()):
                continue
            round_num = int(row[0].strip())
            order = _round_order_even_reversed(round_num, len(team_names))
            pick_base = (round_num - 1) * len(team_names)
            for seq, col in enumerate(order, start=1):
                if col + 1 >= len(row):
                    continue
                parsed = parse_draft_cell(row[col + 1])
                if not parsed or not parsed["player_name"]:
                    continue
                team_code = None
                if team_resolver is not None:
                    team_code = team_resolver(team_names[col], season)
                else:
                    team_code = resolve_team_code(team_names[col], season)
                if not team_code:
                    logger.warning("Unmapped draft team %r for season %d.", team_names[col], season)
                    continue
                records.append(
                    {
                        "season": season,
                        "draft_type": "2차",
                        "round_num": round_num,
                        "pick_seq": pick_base + seq,
                        "team_code": team_code,
                        "player_name": parsed["player_name"],
                        "position": parsed["position"],
                        "school": parsed["school"],
                        "sign_fee": parsed["sign_fee"],
                    }
                )
        return records

    async def crawl_season(self, season: int) -> list[dict[str, object]]:
        """Crawl all 2차 draft records for a season."""
        headers, rows, first_rows = await self.fetch_season_tables(season)
        records: list[dict[str, object]] = []
        if headers and rows:
            records.extend(self.parse_season_records(season, headers, rows))
        if first_rows:
            records.extend(self.parse_first_pick_records(season, first_rows))
        return records

    def parse_first_pick_records(self, season: int, rows: list[list[str]]) -> list[dict[str, object]]:
        """Convert 1차-style rows ([team, school, position, name, fee]) to records."""
        from src.utils.team_codes import resolve_team_code

        records: list[dict[str, object]] = []
        for seq, row in enumerate(rows, start=1):
            if len(row) < MIN_FIRST_PICK_COLUMNS:
                continue
            team_code = resolve_team_code(row[0].strip(), season)
            name = _clean_name(row[3].strip())
            if not team_code or not name:
                continue
            records.append(
                {
                    "season": season,
                    "draft_type": "1차",
                    "round_num": 1,
                    "pick_seq": seq,
                    "team_code": team_code,
                    "player_name": name,
                    "position": row[2].strip() or None,
                    "school": row[1].strip() or None,
                    "sign_fee": row[FIRST_PICK_FEE_COLUMN].strip()
                    if len(row) > FIRST_PICK_FEE_COLUMN and row[FIRST_PICK_FEE_COLUMN].strip()
                    else None,
                }
            )
        return records
