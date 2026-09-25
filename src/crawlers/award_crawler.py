"""KBO award history crawler (wikipedia + yagoonara).

Sources:
- ko.wikipedia.org: KBO MVP / KBO 신인상 / KBO 골든글러브 / KBO 수비상 tables
- yagoonara.com/awards: 최근 연도 별 올스타전 MVP / 한국시리즈 MVP / 수상 보강

Award rows are normalized (year, award_type, category, player_name, team_name)
and stored in the `awards` table (idempotent by unique key).

"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from typing import Any

import httpx
from bs4 import BeautifulSoup, Tag

from src.crawlers.failure_taxonomy import (
    CrawlPersistError,
    FailureCode,
    FailureStage,
    classify_failure,
    classify_persist_failure,
)
from src.db.engine import SessionLocal
from src.models.crawl_execution import RUN_STATUS_PARTIAL
from src.repositories.award_repository import AwardRepository
from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
from src.repositories.crawl_execution_repository import CrawlRunSpec
from src.repositories.source_registry_repository import save_raw_snapshots
from src.services.crawl_dead_letter_service import enqueue_failure
from src.services.crawl_run_service import track_crawl_run
from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)

AWARD_FETCH_EXCEPTIONS = (httpx.HTTPError, TimeoutError, ValueError, TypeError, RuntimeError, OSError)

WIKI_API_URL = "https://ko.wikipedia.org/w/api.php"
WIKI_USER_AGENT = "KBOPlaywrightBot/1.0 (RAG research; contact: kbo@example.com)"
YAGOONARA_URL = "https://www.yagoonara.com/awards"
YAGOONARA_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
YAGOO_VIDEO_SUFFIX = " 영상"

WIKI_PAGES: dict[str, str] = {
    "MVP": "KBO MVP",
    "신인상": "KBO 신인상",
    "골든글러브": "KBO 골든글러브",
    "수비상": "KBO 수비상",
}

# 야구나라 팀 축약명 -> 표준 풀네임
YAGOO_TEAM_MAP: dict[str, str] = {
    "한화": "한화 이글스",
    "두산": "두산 베어스",
    "LG": "LG 트윈스",
    "SSG": "SSG 랜더스",
    "NC": "NC 다이노스",
    "KIA": "KIA 타이거즈",
    "롯데": "롯데 자이언츠",
    "삼성": "삼성 라이온즈",
    "kt": "kt 위즈",
    "KT": "kt 위즈",
    "키움": "키움 히어로즈",
}

_PLAYER_TEAM_RE = re.compile(r"([가-힣]{2,6})\s*\(\s*([^()]{1,12})\s*\)")
_DECEASED_PREFIX_RE = re.compile(r"^\(?故?\)?")

_MIN_YAGOO_TABLE_ROWS = 2
_YAGOO_CELL_COUNT = 4
_AWARD_HAS_CATEGORY_PREFIXES = ("골든글러브", "수비상")
AWARD_PARSER_VERSION = "award-crawler-v1"
AWARD_CRAWLER_NAME = "awards"
AWARD_TARGET_TYPE = "award_history"
WIKI_SOURCE_KEY = "kbo_awards_wikipedia"
YAGOONARA_SOURCE_KEY = "kbo_awards_yagoonara"


@dataclass(frozen=True)
class AwardRecord:
    """Normalized award record from any source."""

    year: int
    award_type: str
    category: str | None
    player_name: str
    team_name: str


@dataclass(frozen=True)
class AwardSourceRun:
    """Summarize one source fetch and parse attempt."""

    source_key: str
    source_url: str
    fetched: bool
    parsed_records: int
    error: str | None = None
    error_code: str | None = None
    failure_stage: str | None = None


def _extract_year(text: str) -> int | None:
    """Extract the first 4-digit year from text.

    Args:
        text: Source text.

    Returns:
        Year or None.

    """
    match = re.search(r"(1|2)\d{3}", text)
    return int(match.group(0)) if match else None


def _split_pairs(cell_text: str) -> list[tuple[str, str]]:
    """Split multiple 'name (team)' pairs inside a cell.

    Args:
        cell_text: Golden glove style cell.

    Returns:
        List of (name, team).

    """
    pairs = [(m.group(1), m.group(2).strip()) for m in _PLAYER_TEAM_RE.finditer(cell_text)]
    if pairs:
        return pairs
    cleaned = cell_text.strip()
    return [(cleaned.strip(), "")] if cleaned else []


class AwardCrawler:
    """Crawl KBO award history from wikipedia tables and yagoonara.com.

    Collects: MVP, Rookie of the Year, Golden Glove, Defensive Award,
    All-Star MVP, Korean Series MVP.

    """

    def __init__(self) -> None:
        """Initialize a new instance."""
        self.policy = RequestPolicy()
        self._raw_snapshots: list[dict[str, Any]] = []
        self._source_runs: list[AwardSourceRun] = []
        self._client: httpx.AsyncClient | None = None

    @property
    def source_runs(self) -> tuple[AwardSourceRun, ...]:
        """Return source fetch/parse results from the most recent crawl."""
        return tuple(self._source_runs)

    @property
    def raw_snapshots(self) -> tuple[dict[str, Any], ...]:
        """Return raw snapshots captured by the most recent crawl."""
        return tuple(dict(snapshot) for snapshot in self._raw_snapshots)

    async def _get_client(self) -> httpx.AsyncClient:
        """Return the shared lazily-created httpx client.

        Returns:
            AsyncClient.

        """
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=30,
                follow_redirects=True,
                headers={"User-Agent": WIKI_USER_AGENT},
            )
        return self._client

    async def close(self) -> None:
        """Close network resources."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _fetch(self, url: str, params: dict[str, str | int] | None = None) -> tuple[str, int]:
        """Fetch a page.

        Args:
            url: URL.
            params: Optional query parameters.

        Returns:
            A tuple of (response text, HTTP status code).

        """
        client = await self._get_client()
        headers = {"User-Agent": WIKI_USER_AGENT}
        if YAGOONARA_URL in url:
            headers["User-Agent"] = YAGOONARA_USER_AGENT
        resp = await client.get(url, params=params, headers=headers)
        resp.raise_for_status()
        return resp.text, resp.status_code

    async def _fetch_wiki_page(self, title: str) -> BeautifulSoup:
        """Fetch a wikipedia page via the parse API and cache the snapshot.

        Args:
            title: Page title.

        Returns:
            Parsed HTML.

        """
        raw, status_code = await self._fetch(
            WIKI_API_URL,
            {"action": "parse", "page": title, "format": "json", "prop": "text", "redirects": 1},
        )
        data = json.loads(raw)
        html = data["parse"]["text"]["*"]
        self._raw_snapshots.append(
            {
                "url": f"{WIKI_API_URL}?page={title}",
                "html": html,
                "source_key": "kbo_awards_wikipedia",
                "status_code": status_code,
            },
        )
        return BeautifulSoup(html, "html.parser")

    async def _fetch_yagoonara(self) -> BeautifulSoup:
        """Fetch the yagoonara awards page.

        Returns:
            Parsed HTML.

        """
        html, status_code = await self._fetch(YAGOONARA_URL)
        self._raw_snapshots.append(
            {
                "url": YAGOONARA_URL,
                "html": html,
                "source_key": "kbo_awards_yagoonara",
                "status_code": status_code,
            },
        )
        return BeautifulSoup(html, "html.parser")

    async def crawl(
        self,
        types: set[str] | None = None,
        source_key: str | None = None,
    ) -> list[AwardRecord]:
        """Crawl all (or one) award sources and merge duplicates.

        Args:
            types: Optional award type filter (e.g. {"MVP", "골든글러브"}).
            source_key: Optional single source filter used by replay.

        Returns:
            Unique normalized award records.

        """
        records: list[AwardRecord] = []
        self._source_runs = []
        for award_type, title in WIKI_PAGES.items():
            if types is not None and award_type not in types:
                continue
            if source_key is not None and source_key != WIKI_SOURCE_KEY:
                continue
            source_url = f"{WIKI_API_URL}?page={title}"
            try:
                soup = await self._fetch_wiki_page(title)
                parsed = self._parse_wiki_soup(soup, award_type)
                self._mark_snapshot_parse_status(WIKI_SOURCE_KEY, source_url, parsed_records=len(parsed))
                self._source_runs.append(
                    AwardSourceRun(
                        source_key=WIKI_SOURCE_KEY,
                        source_url=source_url,
                        fetched=True,
                        parsed_records=len(parsed),
                    ),
                )
                logger.info("wikipedia %-8s -> %s records", award_type, len(parsed))
                records.extend(parsed)
            except AWARD_FETCH_EXCEPTIONS as exc:
                stage, code = classify_failure(exc)
                self._mark_snapshot_parse_status(
                    WIKI_SOURCE_KEY,
                    source_url,
                    parsed_records=0,
                    error=f"{type(exc).__name__}: {exc}",
                )
                self._source_runs.append(
                    AwardSourceRun(
                        source_key=WIKI_SOURCE_KEY,
                        source_url=source_url,
                        fetched=any(snapshot.get("source_key") == WIKI_SOURCE_KEY for snapshot in self._raw_snapshots),
                        parsed_records=0,
                        error=f"{type(exc).__name__}: {exc}",
                        error_code=code.value,
                        failure_stage=stage.value,
                    ),
                )
                logger.exception("wikipedia %s failed (skipped)", title)
            await self.policy.delay_async(host="ko.wikipedia.org")

        if source_key is None or source_key == YAGOONARA_SOURCE_KEY:
            try:
                soup = await self._fetch_yagoonara()
                parsed = self._parse_yagoonara(soup)
                if types is not None:
                    parsed = [rec for rec in parsed if rec.award_type in types]
                self._mark_snapshot_parse_status(
                    YAGOONARA_SOURCE_KEY,
                    YAGOONARA_URL,
                    parsed_records=len(parsed),
                )
                self._source_runs.append(
                    AwardSourceRun(
                        source_key=YAGOONARA_SOURCE_KEY,
                        source_url=YAGOONARA_URL,
                        fetched=True,
                        parsed_records=len(parsed),
                    ),
                )
                logger.info("yagoonara -> %s records", len(parsed))
                records.extend(parsed)
            except AWARD_FETCH_EXCEPTIONS as exc:
                stage, code = classify_failure(exc)
                self._mark_snapshot_parse_status(
                    YAGOONARA_SOURCE_KEY,
                    YAGOONARA_URL,
                    parsed_records=0,
                    error=f"{type(exc).__name__}: {exc}",
                )
                self._source_runs.append(
                    AwardSourceRun(
                        source_key=YAGOONARA_SOURCE_KEY,
                        source_url=YAGOONARA_URL,
                        fetched=any(
                            snapshot.get("source_key") == YAGOONARA_SOURCE_KEY for snapshot in self._raw_snapshots
                        ),
                        parsed_records=0,
                        error=f"{type(exc).__name__}: {exc}",
                        error_code=code.value,
                        failure_stage=stage.value,
                    ),
                )
                logger.warning("yagoonara fetch failed (skipped)")
            await self.policy.delay_async(host="yagoonara.com")

        return self._dedup(records)

    def _mark_snapshot_parse_status(
        self,
        source_key: str,
        source_url: str,
        *,
        parsed_records: int,
        error: str | None = None,
    ) -> None:
        """Attach parser outcome metadata to the matching raw snapshot."""
        for snapshot in reversed(self._raw_snapshots):
            if snapshot.get("source_key") != source_key or snapshot.get("url") != source_url:
                continue
            snapshot["parse_status"] = "failed" if error else "done"
            snapshot["parser_version"] = AWARD_PARSER_VERSION
            snapshot["error_message"] = error
            snapshot["capture_metadata"] = {"parsed_records": parsed_records}
            return

    # ─── 표 렌더링 헬퍼 ───────────────────────────────────────────────────────

    @staticmethod
    def _render_table(table: Tag) -> tuple[list[str], list[list[str]]]:
        """Render a table into (headers, rows) with rowspan reporting.

        Rowspan cells keep propagating for ``rowspan - 1`` following rows;
        their text is emitted once per covered row and only dropped after the
        last covered row, so multi-row spans never leave gaps in later rows.

        Args:
            table: Table element.

        Returns:
            (header labels, data rows).

        """
        all_rows = table.find_all("tr")
        if not all_rows:
            return [], []
        headers = [re.sub(r"\s+", " ", cell.get_text(strip=True)) for cell in all_rows[0].find_all(["th", "td"])]
        header_cells = len(headers)
        if header_cells == 0:
            headers = [f"col{i}" for i in range(max(len(r.find_all(["th", "td"])) for r in all_rows[1:]))]
            header_cells = len(headers)

        grid: list[list[str]] = []
        pending: dict[int, tuple[str, int]] = {}
        for tr in all_rows[1:]:
            row: list[str] = []
            next_pending: dict[int, tuple[str, int]] = {}
            cells = tr.find_all(["th", "td"])
            cell_idx = 0
            for col in range(header_cells):
                if col in pending:
                    text, remaining = pending[col]
                    row.append(text)
                    if remaining > 1:
                        next_pending[col] = (text, remaining - 1)
                elif cell_idx < len(cells):
                    cell = cells[cell_idx]
                    cell_idx += 1
                    text = re.sub(r"\s+", " ", cell.get_text(strip=True))
                    row.append(text)
                    rowspan = int(str(cell.get("rowspan") or 1))
                    if rowspan > 1:
                        next_pending[col] = (text, rowspan - 1)
                else:
                    row.append("")
            pending = next_pending
            grid.append(row)
        return headers, grid

    @staticmethod
    def _column_index(headers: list[str], *keywords: str) -> int:
        """Find the index of the header containing any of the keywords.

        Args:
            headers: Header labels.
            keywords: Candidate keywords.

        Returns:
            Column index or -1.

        """
        for i, label in enumerate(headers):
            if any(keyword in label for keyword in keywords):
                return i
        return -1

    # ─── wikipedia 파싱 ───────────────────────────────────────────────────────

    def _parse_wiki_soup(self, soup: BeautifulSoup, award_type: str) -> list[AwardRecord]:
        """Parse one wikipedia award page.

        Args:
            soup: Parsed page.
            award_type: Award type label.

        Returns:
            Records.

        """
        if award_type in ("골든글러브", "수비상"):
            return self._parse_positional_wiki(soup, award_type)
        return self._parse_mvp_style_wiki(soup, award_type)

    def _parse_mvp_style_wiki(self, soup: BeautifulSoup, award_type: str) -> list[AwardRecord]:
        """Parse MVP / 신인상 tables (연도/수상자/소속구단/위치).

        Args:
            soup: Page soup.
            award_type: Award type.

        Returns:
            Records.

        """
        table = soup.find("table", class_="wikitable")
        if not table:
            return []
        headers, rows = self._render_table(table)
        year_i = self._column_index(headers, "연도")
        player_i = self._column_index(headers, "수상자", "선수명")
        team_i = self._column_index(headers, "소속", "팀")
        position_i = self._column_index(headers, "포지션", "위치")
        year_i = max(year_i, 0)

        records: list[AwardRecord] = []
        for row in rows:
            year = _extract_year(row[year_i] if year_i < len(row) else "")
            player_raw = row[player_i] if player_i >= 0 and player_i < len(row) else ""
            team = row[team_i] if team_i >= 0 and team_i < len(row) else ""
            position_raw = row[position_i] if position_i >= 0 and position_i < len(row) else ""
            if year is None or not player_raw:
                continue
            player = _DECEASED_PREFIX_RE.sub("", player_raw).strip()
            if not player:
                continue
            records.append(
                AwardRecord(
                    year=year,
                    award_type=award_type,
                    category=position_raw.strip() or None,
                    player_name=player,
                    team_name=team.strip(),
                ),
            )
        return records

    def _parse_positional_wiki(self, soup: BeautifulSoup, award_type: str) -> list[AwardRecord]:
        """Parse 골든글러브/수비상 tables ((연도, 포지션...) 열).

        Args:
            soup: Page soup.
            award_type: Award type.

        Returns:
            Records.

        """
        table = soup.find("table", class_="wikitable")
        if not table:
            return []
        headers, rows = self._render_table(table)
        if not headers:
            return []
        records: list[AwardRecord] = []
        for row in rows:
            if not row:
                continue
            year = _extract_year(row[0])
            if year is None:
                continue
            for col in range(1, min(len(row), len(headers))):
                cell_text = row[col]
                if not cell_text or cell_text in ("-", "—"):
                    continue
                position = headers[col]
                if position.startswith("col"):
                    continue
                for name, team in _split_pairs(cell_text):
                    records.append(
                        AwardRecord(
                            year=year,
                            award_type=award_type,
                            category=position,
                            player_name=name,
                            team_name=team,
                        ),
                    )
        return records

    # ─── yagoonara 파싱 ───────────────────────────────────────────────────────

    @staticmethod
    def _detect_yagoonara_year(table: Tag) -> int | None:
        """Find the season year in the nearest preceding heading.

        Args:
            table: Table element.

        Returns:
            Year or None.

        """
        node: Tag | None = table
        for _ in range(10):
            if node is None:
                return None
            prev = node.find_previous()
            if prev is None:
                return None
            text = prev.get_text(" ", strip=True)
            year = _extract_year(text)
            if year is not None:
                return year
            if prev.name == "h2":
                break
            node = prev
        return None

    def _parse_yagoonara(self, soup: BeautifulSoup) -> list[AwardRecord]:
        """Parse 연도별 수상 테이블 (수상/선수/팀/포지션).

        Args:
            soup: Page soup.

        Returns:
            Records.

        """
        records: list[AwardRecord] = []
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < _MIN_YAGOO_TABLE_ROWS:
                continue
            header = [th.get_text(" ", strip=True) for th in rows[0].find_all(["th", "td"])]
            if header[:2] != ["수상", "선수"]:
                continue
            year = self._detect_yagoonara_year(table)
            if year is None:
                continue
            for tr in rows[1:]:
                cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                if len(cells) < _YAGOO_CELL_COUNT or not cells[1]:
                    continue
                award_text = cells[0]
                player = cells[1].replace(YAGOO_VIDEO_SUFFIX, "").strip()
                if not player:
                    continue
                award_type, category = self._map_yagoonara_award(award_text, cells[3].strip())
                if award_type is None:
                    continue
                records.append(
                    AwardRecord(
                        year=year,
                        award_type=award_type,
                        category=category,
                        player_name=player,
                        team_name=YAGOO_TEAM_MAP.get(cells[2].strip(), cells[2].strip()),
                    ),
                )
        return records

    @staticmethod
    def _map_yagoonara_award(award_text: str, position: str) -> tuple[str | None, str | None]:
        """Map a yagoonara award label to (award_type, category).

        Args:
            award_text: Raw award label.
            position: Position column text.

        Returns:
            (type, category) or (None, None).

        """
        if award_text.strip().startswith(_AWARD_HAS_CATEGORY_PREFIXES[0]):
            return "골든글러브", position or None
        if award_text.strip().startswith(_AWARD_HAS_CATEGORY_PREFIXES[1]):
            return "수비상", position or None
        if "MVP" not in award_text:
            return ("신인상", None) if "신인" in award_text else (None, None)
        if "올스타" in award_text:
            return "올스타전MVP", None
        if "한국시리즈" in award_text:
            return "한국시리즈MVP", None
        return "MVP", None

    # ─── 저장 ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _dedup(records: list[AwardRecord]) -> list[AwardRecord]:
        """Deduplicate by (year, award_type, category, player).

        Args:
            records: Raw records.

        Returns:
            Deduplicated records.

        """
        seen: set[tuple[int, str, str | None, str]] = set()
        deduped: list[AwardRecord] = []
        for rec in records:
            if not rec.player_name:
                continue
            key = (rec.year, rec.award_type, rec.category, rec.player_name)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(rec)
        return deduped

    async def save(self, records: list[AwardRecord], *, raise_on_error: bool = False) -> tuple[int, int]:
        """Persist new award records (unique: year+type+player).

        Args:
            records: Award rows.
            raise_on_error: Re-raise persistence failures as ``CrawlPersistError``.

        Returns:
            (saved count, skipped count).

        """
        saved = 0
        skipped = 0
        with SessionLocal() as session:
            try:
                repo = AwardRepository(session)
                for rec in records:
                    existing = repo.save_award(
                        {
                            "year": rec.year,
                            "award_type": rec.award_type,
                            "category": rec.category,
                            "player_name": rec.player_name,
                            "team_name": rec.team_name,
                        },
                    )
                    if existing.id is None:
                        saved += 1
                    else:
                        skipped += 1
                if self._raw_snapshots:
                    save_raw_snapshots(session, self._raw_snapshots)
                session.commit()
                self._raw_snapshots = []
            except Exception as exc:
                session.rollback()
                logger.exception("Error saving awards")
                if raise_on_error:
                    stage, code = classify_persist_failure(exc)
                    message = f"{type(exc).__name__}: {exc}"
                    raise CrawlPersistError(message, error_code=code, failure_stage=stage) from exc
        return saved, skipped

    async def run(  # noqa: PLR0913
        self,
        *,
        save: bool = False,
        types: set[str] | None = None,
        source_key: str | None = None,
        run_spec: CrawlRunSpec | None = None,
        record_dead_letters: bool = True,
        raise_on_persist_error: bool = False,
    ) -> int:
        """Crawl award sources and optionally persist.

        Args:
            save: Whether to persist into the DB.
            types: Optional award type filter.
            source_key: Optional single source filter used by replay.
            run_spec: Optional pre-built ledger spec (replay supplies one).
            record_dead_letters: Whether failed sources enqueue DLQ entries.
            raise_on_persist_error: Re-raise persistence failures (replay).

        Returns:
            Number of unique records.

        """
        spec = run_spec or CrawlRunSpec(
            crawler=AWARD_CRAWLER_NAME,
            target_type=AWARD_TARGET_TYPE,
            source_url=f"{WIKI_API_URL}|{YAGOONARA_URL}",
            parser_version=AWARD_PARSER_VERSION,
        )
        with track_crawl_run(spec) as run:
            records = await self.crawl(types=types, source_key=source_key)
            run.records_read = len(records)
            if save:
                saved, skipped = await self.save(records, raise_on_error=raise_on_persist_error)
                run.records_written = saved
                logger.info("Awards saved=%s skipped(existing)=%s total=%s", saved, skipped, len(records))
            else:
                logger.info("Awards parsed (dry-run): %s records", len(records))
                for rec in records[:12]:
                    logger.info(
                        "  %s %s %s -> %s (%s)",
                        rec.year,
                        rec.award_type,
                        rec.category or "-",
                        rec.player_name,
                        rec.team_name or "-",
                    )

            failed_sources = [source_run for source_run in self._source_runs if source_run.error]
            if failed_sources:
                run.status = RUN_STATUS_PARTIAL
                run.error_code = FailureCode.SOURCE_PARTIAL.value
                run.error_message = "; ".join(
                    f"{source_run.source_key}: {source_run.error}" for source_run in failed_sources[:5]
                )
                if record_dead_letters:
                    self._enqueue_source_failures(run.run_id, failed_sources)
            return len(records)

    @staticmethod
    def _enqueue_source_failures(original_run_id: str, failed_sources: list[AwardSourceRun]) -> None:
        """Enqueue one DLQ entry per distinct failed source, never breaking the crawl."""
        seen: set[str] = set()
        for source_run in failed_sources:
            if source_run.source_key in seen:
                continue
            seen.add(source_run.source_key)
            try:
                enqueue_failure(
                    DeadLetterSpec(
                        original_run_id=original_run_id,
                        crawler=AWARD_CRAWLER_NAME,
                        target_type=AWARD_TARGET_TYPE,
                        target_id=source_run.source_key,
                        source_url=source_run.source_url,
                        failure_stage=source_run.failure_stage or FailureStage.FETCH.value,
                        error_code=source_run.error_code or FailureCode.UNKNOWN.value,
                        error_message=source_run.error,
                    ),
                )
            except Exception:
                logger.exception("Failed to enqueue dead letter for source %s", source_run.source_key)


if __name__ == "__main__":

    async def _main() -> None:
        crawler = AwardCrawler()
        try:
            count = await crawler.run(save=False)
            logger.info("total %s", count)
        finally:
            await crawler.close()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(_main())
