"""Wikipedia-backed KBO rookie draft crawler (2차 지명 tables).

Yearly pages (``{YYYY}년 KBO 리그 신인 드래프트``) carry one sortable
wikitable: rows are 2차 rounds, columns are teams, cells hold
``이름(학교,포지션)(계약금:...)``. Reuses the rowspan-safe table renderer
from the award crawler, and fetches through `CrawlerHttpClient` for the same
reason that crawler does.

A season with no Wikipedia page is `CrawlOutcome.EMPTY`, not a failure:
the parse API answers a missing page with HTTP 200 and an `error` key, so
treating it as an error would report every unrecorded season as an outage.
Only transport faults and body shapes that are not a JSON object come back
as failures, each carrying its classification.
"""

from __future__ import annotations

import logging
import re
from typing import cast

from bs4 import BeautifulSoup

from src.crawlers.award_crawler import WIKI_USER_AGENT, AwardCrawler
from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.http_client import CrawlerHttpClient, HttpPolicy
from src.crawlers.result import CrawlOutcome, CrawlResult

logger = logging.getLogger(__name__)

DRAFT_WIKI_CRAWLER_NAME = "draft_wiki_crawler"

WIKI_API_URL = "https://ko.wikipedia.org/w/api.php"
WIKI_TIMEOUT_SECONDS = 30.0

#: The rendered 2차 table, its rows, and the 1차 rows, in that order.
DraftTables = tuple[list[str], list[list[str]], list[list[str]]]

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

    def __init__(self, request_delay: float = 1.0, http_client: CrawlerHttpClient | None = None) -> None:
        """Initialize the crawler.

        Args:
            request_delay: Minimum spacing between requests, in seconds.
            http_client: Transport to use. Defaults to a client named for this
                crawler. Tests inject a client backed by a mock transport.

        """
        self.request_delay = request_delay
        self._http = http_client or CrawlerHttpClient(
            name=DRAFT_WIKI_CRAWLER_NAME,
            policy=HttpPolicy(base_delay_seconds=request_delay, timeout_seconds=WIKI_TIMEOUT_SECONDS),
        )

    async def fetch_season_table(self, season: int) -> CrawlResult[tuple[list[str], list[list[str]]]]:
        """Fetch and render the 2차 draft table for a season.

        Args:
            season: Target draft season year.

        Returns:
            The rendered 2차 table, an empty result when the season has none,
            or the classified transport failure.

        """
        result = await self.fetch_season_tables(season)
        if not result.ok:
            return cast("CrawlResult[tuple[list[str], list[list[str]]]]", result)
        headers, rows, _ = result.data
        if not headers or not rows:
            return CrawlResult.empty(url=WIKI_API_URL)
        return CrawlResult.success((headers, rows), http_status=result.http_status, url=WIKI_API_URL)

    async def fetch_season_tables(self, season: int) -> CrawlResult[DraftTables]:
        """Fetch 2차 table plus 1차 rows (both may be absent per season).

        Wikipedia answers a missing page with HTTP 200 and an `error` key in
        the body rather than a 404, so a season without a draft page is an
        empty result and not a failure. Only a transport fault is returned as
        a failure, and it keeps the classification `CrawlerHttpClient` gave
        it so the caller can tell a missing season from a broken request.

        Args:
            season: Target draft season year.

        Returns:
            The rendered tables, an empty result when neither title exists,
            or the classified transport failure.

        """
        payload: dict[str, object] | None = None
        for title in (
            f"{season}년 KBO 리그 신인 드래프트",
            f"{season}년 한국프로야구 신인 드래프트",
        ):
            result = await self._http.fetch_json(
                WIKI_API_URL,
                params={"action": "parse", "page": title, "format": "json", "prop": "text", "redirects": 1},
                headers={"User-Agent": WIKI_USER_AGENT},
            )
            if not result.ok:
                # A failed fetch carries no payload and the caller reads
                # `.data` only behind `.ok`. The same object is returned so
                # `error`, `error_code`, `http_status`, `attempts`, and
                # `retry_after` all survive; rebuilding it through
                # `CrawlResult.failure` would drop the timing ledger.
                return cast("CrawlResult[DraftTables]", result)
            if not isinstance(result.data, dict):
                # `fetch_json` decodes the body but does not promise its shape.
                # A 200 carrying something other than an object is drift, and
                # letting the `TypeError` escape below would report it as an
                # unclassified crash instead of a schema change.
                return CrawlResult.failure(
                    CrawlOutcome.SCHEMA_CHANGED,
                    error=f"expected a JSON object, got {type(result.data).__name__}",
                    error_code=FailureCode.PARSE_INVALID_FORMAT.value,
                    url=WIKI_API_URL,
                )
            if "error" not in result.data:
                payload = result.data
                break
        if payload is None:
            logger.warning("Wikipedia page missing for season %d.", season)
            return CrawlResult.empty(url=WIKI_API_URL)
        return CrawlResult.success(self._render_season_tables(payload), url=WIKI_API_URL)

    @staticmethod
    def _render_season_tables(payload: dict[str, object]) -> DraftTables:
        """Split a rendered Wikipedia page into its 2차 and 1차 draft tables.

        Args:
            payload: Decoded `action=parse` response.

        Returns:
            The 2차 headers and rows plus the 1차 rows, each empty when the
            page does not carry that table.

        """
        soup = BeautifulSoup(payload["parse"]["text"]["*"], "html.parser")
        second_headers: list[str] = []
        second_rows: list[list[str]] = []
        first_rows: list[list[str]] = []
        for table in soup.find_all("table", class_="wikitable"):
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

    async def crawl_season(self, season: int) -> CrawlResult[list[dict[str, object]]]:
        """Crawl all 2차 draft records for a season.

        Args:
            season: Target draft season year.

        Returns:
            The draft records, an empty result when the season has no draft
            page or no records, or the classified transport failure.

        """
        tables = await self.fetch_season_tables(season)
        if not tables.ok:
            return cast("CrawlResult[list[dict[str, object]]]", tables)
        headers, rows, first_rows = tables.data
        records: list[dict[str, object]] = []
        if headers and rows:
            records.extend(self.parse_season_records(season, headers, rows))
        if first_rows:
            records.extend(self.parse_first_pick_records(season, first_rows))
        if not records:
            logger.info("No draft records parsed for season %d.", season)
            return CrawlResult.empty(url=WIKI_API_URL)
        return CrawlResult.success(records, url=WIKI_API_URL)

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
