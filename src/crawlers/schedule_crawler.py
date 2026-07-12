"""KBO Schedule Crawler POC.

Collects game IDs from the KBO schedule page.

"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page

from src.constants import KST
from src.urls import SCHEDULE
from src.utils.compliance import compliance
from src.utils.game_status import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DELAYED,
    GAME_STATUS_LIVE,
    GAME_STATUS_POSTPONED,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_SUSPENDED,
    normalize_game_status,
)
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.playwright_retry import SEL_TIMEOUT, SHORT_TIMEOUT
from src.utils.request_policy import RequestPolicy
from src.utils.schedule_validation import validate_schedule_game_payload
from src.utils.stadium_codes import STADIUM_SHORT_NAME_MAP
from src.utils.team_codes import normalize_kbo_game_id, resolve_team_code, team_code_from_game_id_segment

logger = logging.getLogger(__name__)
SCHEDULE_CRAWLER_EXCEPTIONS = (PlaywrightError, TimeoutError, RuntimeError, ValueError, TypeError, KeyError, OSError)


class ScheduleCrawler:
    """KBO 공식 사이트의 월별 경기 일정 페이지에서 경기 정보를 크롤링하는 클래스.

    주요 기능:
    - 특정 연도와 월에 해당하는 경기 일정 페이지에 접근합니다.
    - 페이지 내의 모든 경기 링크를 분석하여 고유 ID(gameId)를 추출합니다.
    - gameId를 바탕으로 경기 날짜, 홈/어웨이 팀 코드 등의 상세 정보를 파싱합니다.
    - 수집된 경기 정보 리스트를 반환합니다.

    """

    def __init__(
        self,
        request_delay: float = 1.5,
        pool: AsyncPlaywrightPool | None = None,
        policy: RequestPolicy | None = None,
    ) -> None:
        """Initialize a new instance.

        Args:
            request_delay: Request Delay.
            pool: Connection pool for async operations.
            policy: Policy.
            request_delay: Request Delay.
            pool: Connection pool for async operations.
            policy: Policy.

        """
        self.base_url = SCHEDULE

        self.request_delay = request_delay
        self.pool = pool
        self.policy = policy or RequestPolicy.with_delay(request_delay)
        self._last_failure_reason: dict[str, str] = {}

    def get_last_failure_reason(self, key: str) -> str | None:
        """Get last failure reason.

        Args:
            key: Key.
            key: Key.
            key: Key.

        Returns:
            The result of the operation.

        """
        return self._last_failure_reason.get(key)

    def _schedule_key(self, year: int, month: int, series_id: str | None = None) -> str:
        suffix = series_id if series_id is not None else "all"
        return f"{year}-{month:02d}:{suffix}"

    async def crawl_schedule(self, year: int, month: int, series_id: str | None = None) -> list[dict]:
        """지정된 연도와 월의 경기 일정을 크롤링하는 메인 메서드.

        Args:
            year: Season year.
            month: Month.
            series_id: Series ID.
            year: Season year.
            month: Month.
            series_id: Series ID.
            year: 시즌 연도 (예: 2024)
            month: 월 (1-12)
            series_id: 시리즈 ID (옵션)

        Returns:
            경기 정보 딕셔너리가 담긴 리스트.

        """
        logger.info("🔍 Crawling schedule for %s-%02d (Series: %s)...", year, month, series_id)

        pool = self.pool or AsyncPlaywrightPool(max_pages=1)
        owns_pool = self.pool is None
        await pool.start()
        try:
            page = await pool.acquire()
            try:
                games = await self._crawl_month(page, year, month, series_id=series_id)
            except SCHEDULE_CRAWLER_EXCEPTIONS:
                logger.exception("❌ Error crawling schedule")
                return []
            else:
                logger.info("✅ Found %s games", len(games))
                return games
            finally:
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()

    async def crawl_season(
        self,
        year: int,
        months: list[int] | None = None,
        series_id: str | None = None,
    ) -> list[dict]:
        """주어진 시즌의 여러 달에 걸쳐 경기 일정을 크롤링합니다.

        Args:
            year: Season year.
            months: Months.
            series_id: Series ID.
            year: Season year.
            months: Months.
            series_id: Series ID.
            year: 시즌 연도
            months: 크롤링할 월 목록 (기본값: 3월-10월)
            series_id: 시리즈 ID (옵션)

        """
        months = months or list(range(3, 11))

        all_games: list[dict] = []

        pool = self.pool or AsyncPlaywrightPool(max_pages=1)
        owns_pool = self.pool is None
        await pool.start()
        try:
            page = await pool.acquire()
            try:
                for month in months:
                    await self.policy.delay_async(host="www.koreabaseball.com")
                    month_games = await self._crawl_month(page, year, month, series_id=series_id)
                    all_games.extend(month_games)
                return all_games
            finally:
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()

    async def _navigate_schedule_page(
        self,
        page: Page,
        *,
        required_selector: str = "#ddlYear, #ddlMonth, #ddlSeries, .tbl",
        timeout: int = 30000,  # noqa: ASYNC109
        selector_timeout: int = 10000,
    ) -> tuple[bool, str]:
        if not await compliance.is_allowed(self.base_url):
            logger.info("[COMPLIANCE] Navigation to %s aborted.", self.base_url)
            return False, "blocked"

        async def _navigate() -> None:
            await self.policy.delay_async(host="www.koreabaseball.com")
            if page.url != self.base_url:
                await page.goto(self.base_url, wait_until="networkidle", timeout=timeout)
            await page.wait_for_selector(required_selector, timeout=selector_timeout)

        try:
            await self.policy.run_with_retry_async(_navigate)
        except SCHEDULE_CRAWLER_EXCEPTIONS:
            logger.exception("[WARN] Schedule page navigation failed")
            return False, "schedule_navigation_failed"

        return True, "ok"

    async def _wait_for_schedule_table(self, page: Page, *, timeout: int = 10000) -> tuple[bool, str]:  # noqa: ASYNC109
        try:
            await page.wait_for_selector(".tbl tbody tr", timeout=timeout)
        except SCHEDULE_CRAWLER_EXCEPTIONS:
            logger.exception("[WARN] Schedule table wait failed")
            return False, "schedule_empty"
        else:
            return True, "ok"
            logger.error("[WARN] Schedule table wait failed")
            return False, "schedule_empty"

    async def _select_option_with_retry(
        self,
        page: Page,
        selector: str,
        value: str,
        *,
        label: str,
    ) -> tuple[bool, str]:
        async def _select() -> None:
            await self.policy.delay_async(host="www.koreabaseball.com")
            await page.select_option(selector, value)
            await page.wait_for_load_state("networkidle", timeout=SEL_TIMEOUT)
            await page.wait_for_timeout(500)
            await page.wait_for_selector(".tbl", timeout=SHORT_TIMEOUT)

        try:
            await self.policy.run_with_retry_async(_select)
        except SCHEDULE_CRAWLER_EXCEPTIONS:
            logger.exception("[WARN] Schedule %s select failed (%s)", label, value)
            return False, "schedule_navigation_failed"

        return True, "ok"

    async def _crawl_month(self, page: Page, year: int, month: int, series_id: str | None = None) -> list[dict]:
        """특정 월의 경기 일정 페이지에서 정보를 추출합니다.

        series_id가 지정되지 않은 경우 전 시리즈(시범/정규/포스트)를 순회합니다.

        Args:
            page: Page.
            year: Season year.
            month: Month.
            series_id: Series ID.
            page: Page.
            year: Season year.
            month: Month.
            series_id: Series ID.

        """
        crawl_key = self._schedule_key(year, month, series_id)

        self._last_failure_reason.pop(crawl_key, None)

        ok, failure_reason = await self._navigate_schedule_page(page)
        if not ok:
            self._last_failure_reason[crawl_key] = failure_reason
            return []

        # 1. 연도 및 월 선택 (Postback 발생 가능)
        ok, failure_reason = await self._select_year_month(page, year, month)
        if not ok:
            self._last_failure_reason[crawl_key] = failure_reason
            return []

        ok, failure_reason = await self._wait_for_schedule_table(page)
        if not ok:
            self._last_failure_reason[crawl_key] = failure_reason
            return []

        # 2. 시리즈 목록 확인
        all_series_options = await page.eval_on_selector_all(
            "#ddlSeries option",
            "elements => elements.map(el => ({text: el.innerText, value: el.value}))",
        )

        target_series = [series_id] if series_id else [opt["value"] for opt in all_series_options if opt["value"]]

        all_games = []
        seen_game_ids = set()

        # Mapping from numeric series ID to canonical season_type
        series_id_to_key = {
            "0": "regular",
            "1": "exhibition",
            "3": "semi_playoff",
            "4": "wildcard",
            "5": "playoff",
            "7": "korean_series",
        }

        for sid in target_series:
            logger.info("[NAV] Selecting Series: %s for %s-%02d", sid, year, month)
            try:
                ok, failure_reason = await self._select_option_with_retry(
                    page,
                    "#ddlSeries",
                    sid,
                    label="series",
                )
                if not ok:
                    self._last_failure_reason[crawl_key] = failure_reason
                    continue

                season_type = series_id_to_key.get(sid, "regular")
                month_games = await self._extract_games(page, year, month, season_type=season_type)
                for g in month_games:
                    gid = g.get("game_id")
                    if gid and gid not in seen_game_ids:
                        all_games.append(g)
                        seen_game_ids.add(gid)
            except SCHEDULE_CRAWLER_EXCEPTIONS:
                logger.exception("[WARN] Error crawling series %s", sid)

        if not all_games and not self._last_failure_reason.get(crawl_key):
            self._last_failure_reason[crawl_key] = "schedule_empty"

        return all_games

    async def _select_year_month(self, page: Page, year: int, month: int) -> tuple[bool, str]:
        """연도와 월 드롭다운을 선택하고 페이지 갱신을 기다립니다.

        Args:
            page: Page.
            year: Season year.
            month: Month.
            page: Page.
            year: Season year.
            month: Month.

        """
        current_year = await page.eval_on_selector("#ddlYear", "el => el.value")

        if current_year != str(year):
            ok, failure_reason = await self._select_option_with_retry(
                page,
                "#ddlYear",
                str(year),
                label="year",
            )
            if not ok:
                return False, failure_reason

        current_month = await page.eval_on_selector("#ddlMonth", "el => el.value")
        target_month_str = f"{month:02d}"
        if current_month != target_month_str:
            ok, failure_reason = await self._select_option_with_retry(
                page,
                "#ddlMonth",
                target_month_str,
                label="month",
            )
            if not ok:
                return False, failure_reason

        return True, "ok"

    @staticmethod
    def _normalize_schedule_status(status: object) -> str:
        normalized = normalize_game_status(str(status or "").strip())
        if normalized:
            return normalized

        labels = {
            "경기종료": GAME_STATUS_COMPLETED,
            "종료": GAME_STATUS_COMPLETED,
            "경기중": GAME_STATUS_LIVE,
            "진행중": GAME_STATUS_LIVE,
            "지연": GAME_STATUS_DELAYED,
            "서스펜디드": GAME_STATUS_SUSPENDED,
            "일시정지": GAME_STATUS_SUSPENDED,
            "취소": GAME_STATUS_CANCELLED,
            "우천취소": GAME_STATUS_CANCELLED,
            "경기취소": GAME_STATUS_CANCELLED,
            "순연": GAME_STATUS_POSTPONED,
            "연기": GAME_STATUS_POSTPONED,
        }
        text = str(status or "").strip()
        return labels.get(text, GAME_STATUS_SCHEDULED)

    async def _extract_games(self, page: Page, year: int, month: int, season_type: str = "regular") -> list[dict]:
        """페이지에서 경기 관련 데이터를 추출합니다.

            (JS Fast Path).

        `gameId`가 포함된 모든 링크를 찾아, 각 링크에서 경기 ID, 날짜, 팀 정보 등을 파싱합니다.

        Args:
            page: Page.
            year: Season year.
            month: Month.
            season_type: Season Type.
            page: Page.
            year: Season year.
            month: Month.
            season_type: Season Type.

        """
        # JS를 사용하여 모든 게임 정보를 한 번에 추출

        extraction_script = r"""
        ([{year, season_type}, STADIUM_SHORT_NAME_MAP]) => {
            const results = [];
            const rows = document.querySelectorAll('.tbl tbody tr');
            let currentDateString = ""; // To handle rowspan or implicit date
            const stadiumNames = new Set(Object.keys(STADIUM_SHORT_NAME_MAP));

            function inferStatus(text) {
                if (/우천|취소|콜드취소|경기취소/.test(text)) return "CANCELLED";
                if (/순연|연기/.test(text)) return "POSTPONED";
                if (/서스펜디드|일시정지/.test(text)) return "SUSPENDED";
                if (/지연/.test(text)) return "DELAYED";
                if (/경기중|진행중/.test(text)) return "LIVE";
                if (/경기종료|종료/.test(text)) return "COMPLETED";
                if (/\d+\s*vs\s*\d+/.test(text)) return "COMPLETED";
                return "SCHEDULED";
            }

            function findGameTime(cells) {
                for (const cell of cells) {
                    const txt = cell.innerText.trim();
                    const match = txt.match(/\b\d{1,2}:\d{2}\b/);
                    if (match) return match[0];
                }
                return null;
            }

            function findStadium(cells, matchCellIndex) {
                for (let i = Math.max(0, matchCellIndex + 1); i < cells.length; i++) {
                    const txt = cells[i].innerText.trim();
                    if (stadiumNames.has(txt)) return txt;
                }
                for (const cell of cells) {
                    const txt = cell.innerText.trim();
                    if (stadiumNames.has(txt)) return txt;
                }
                return "";
            }

            rows.forEach(tr => {
                // If it's a "No Game" row, skip
                if (tr.innerText.includes("데이터가 없습니다")) return;

                const cells = Array.from(tr.querySelectorAll('td'));
                if (cells.length < 3) return;

                let firstCellText = cells[0].innerText.trim();
                let timeCellIndex = 1;
                let matchCellIndex = 2;
                let stadiumCellIndex = 7;

                // heuristic: Date like "03.28" or "03.28(토)"
                const dateMatch = firstCellText.match(/(\d{2})\.(\d{2})/);
                if (dateMatch) {
                    currentDateString = dateMatch[0];
                } else if (/^\d{1,2}:\d{2}$/.test(firstCellText)) {
                    timeCellIndex = 0;
                    matchCellIndex = 1;
                    stadiumCellIndex = 6;
                }

                if (!currentDateString) return;

                const timeText = cells[timeCellIndex] ? cells[timeCellIndex].innerText.trim() : "";
                if (!/^\d{1,2}:\d{2}$/.test(timeText)) return;

                const matchText = cells[matchCellIndex] ? cells[matchCellIndex].innerText.trim() : "";
                if (!matchText.includes("vs")) return;

                const teams = matchText.split("vs");
                if (teams.length !== 2) return;

                // Strip trailing/leading numbers and whitespace (e.g., "삼성 0" -> "삼성")
                const awayName = teams[0].replace(/[\d\s]+$/, "").replace(/^[\d\s]+/, "").trim();
                const homeName = teams[1].replace(/[\d\s]+$/, "").replace(/^[\d\s]+/, "").trim();

                const stadium = findStadium(cells, matchCellIndex);
                const status = inferStatus(tr.innerText);

                // Construct Game ID only if link is missing
                const link = tr.querySelector('a[href*="gameId="]');
                if (link) return;

                const [mm, dd] = currentDateString.split(".");
                const fullDate = `${year}${mm}${dd}`;

                results.push({
                    game_id: null,
                    game_date: fullDate,
                    season_year: year,
                    season_type: season_type,
                    away_name: awayName,
                    home_name: homeName,
                    doubleheader_no: 0,
                    game_status: status,
                    crawl_status: 'text_parsed',
                    url_suffix: '',
                    game_time: timeText,
                    stadium: stadium
                });
            });

            const linkSet = new Set();
            const links = document.querySelectorAll('a[href*="gameId="]');
            links.forEach(link => {
                const href = link.getAttribute('href');
                const match = href.match(/gameId=([^&]+)/);
                if (!match) return;
                const gameId = match[1];
                if (linkSet.has(gameId)) return;
                linkSet.add(gameId);

                const gameDate = gameId.substring(0, 8);

                // Flexible segment extraction: search for team codes in the remaining string
                const suffix = gameId.substring(8);
                let away_segment = "";
                let home_segment = "";
                let dh = 0;

                const m = suffix.match(/^([A-Z]{2,3})([A-Z]{2,3})(\d)?$/);
                if (m) {
                    away_segment = m[1];
                    home_segment = m[2];
                    dh = m[3] ? parseInt(m[3]) : 0;
                }

                let gameTime = null;
                let stadium = "";
                let status = "SCHEDULED";
                try {
                    const row = link.closest('tr');
                    if (row) {
                        const cells = Array.from(row.querySelectorAll('td'));
                        const linkCell = link.closest('td');
                        const matchCellIndex = linkCell ? cells.indexOf(linkCell) : 2;
                        gameTime = findGameTime(cells);
                        stadium = findStadium(cells, matchCellIndex);
                        status = inferStatus(row.innerText);
                    }
                } catch(e) {}

                results.push({
                    game_id: gameId,
                    game_date: gameDate,
                    season_year: year,
                    season_type: season_type,
                    away_segment: away_segment,
                    home_segment: home_segment,
                    doubleheader_no: dh,
                    game_status: status,
                    crawl_status: 'link_parsed',
                    url_suffix: href,
                    game_time: gameTime,
                    stadium: stadium
                });
            });

            return results;
        }
        """

        try:
            raw_games = await page.evaluate(
                extraction_script,
                [{"year": year, "season_type": season_type}, STADIUM_SHORT_NAME_MAP],
            )
            games = []

            for g in raw_games:
                away_code = team_code_from_game_id_segment(g.get("away_segment"), year)
                home_code = team_code_from_game_id_segment(g.get("home_segment"), year)

                # Fallback Construction if game_id is missing (future games or link not found)
                if not g.get("game_id"):
                    away_name = g.get("away_name")
                    home_name = g.get("home_name")

                    # Pass 'year' to ensure history-aware resolution
                    away_code = resolve_team_code(away_name, year)
                    home_code = resolve_team_code(home_name, year)

                    if not away_code or not home_code:
                        logger.info("[WARN] Skipping game due to unresolved team names: %s vs %s", away_name, home_name)
                        continue

                    # KBO Website uses LEGACY codes in Game IDs.
                    # We must map our canonical codes (KH, DB, SSG, KIA) to KBO legacy (WO, OB, SK, HT).
                    kbo_legacy_codes = {
                        "KH": "WO",  # Kiwoom -> Woori
                        "DB": "OB",  # Doosan -> OB
                        "SSG": "SK",  # SSG -> SK (Wyverns)
                        "KIA": "HT",  # KIA -> Haitai
                        "LT": "LT",
                        "LG": "LG",
                        "NC": "NC",
                        "HH": "HH",
                        "KT": "KT",
                        "SS": "SS",
                    }

                    kbo_away_code = kbo_legacy_codes.get(away_code, away_code)
                    kbo_home_code = kbo_legacy_codes.get(home_code, home_code)

                    if g.get("game_date") and kbo_away_code and kbo_home_code:
                        # Construct ID: YYYYMMDD + AWAY + HOME + DH
                        dh = g.get("doubleheader_no", 0)
                        constructed_id = f"{g['game_date']}{kbo_away_code}{kbo_home_code}{dh}"
                        g["game_id"] = constructed_id

                schedule_game = {
                    "game_id": normalize_kbo_game_id(g["game_id"]),
                    "game_date": g["game_date"],
                    "season_year": g["season_year"],
                    "season_type": g["season_type"],
                    "away_team_code": away_code,
                    "home_team_code": home_code,
                    "doubleheader_no": g["doubleheader_no"],
                    "game_status": ScheduleCrawler._normalize_schedule_status(g.get("game_status")),
                    "crawl_status": g["crawl_status"],
                    "game_time": g.get("game_time"),
                    "stadium": g.get("stadium"),
                    "url": f"https://www.koreabaseball.com{g['url_suffix']}"
                    if g.get("url_suffix") and g["url_suffix"].startswith("/")
                    else g.get("url_suffix"),
                }
                is_valid, failure_reason = validate_schedule_game_payload(
                    schedule_game,
                    expected_year=year,
                    expected_month=month,
                )
                if not is_valid:
                    logger.warning(
                        "Filtered schedule row: %s reason=%s",
                        schedule_game.get("game_id") or "<missing>",
                        failure_reason,
                    )
                    continue

                games.append(schedule_game)

        except SCHEDULE_CRAWLER_EXCEPTIONS:
            logger.exception("[WARN] Error extracting game (JS)")
            return []

        if not games:
            # Debugging: Check if table exists or content
            content = await page.content()
            logger.debug("No games found. Page content len: %d", len(content))
            if "gameId=" in content:
                logger.debug("'gameId=' string FOUND in HTML but extraction failed.")
            else:
                logger.debug("'gameId=' string NOT found in HTML.")
                # Dump first few rows of the table to see structure
                debug_script = """
                 () => {
                     const rows = document.querySelectorAll('.tbl tbody tr');
                     const data = [];
                     for(let i=0; i<Math.min(rows.length, 5); i++) {
                         data.push(rows[i].innerText);
                     }
                     return data;
                 }
                 """
                try:
                    rows_text = await page.evaluate(debug_script)
                    logger.info("Table rows sample: %s", rows_text)
                except (PlaywrightError, TimeoutError):
                    logger.info("Debug evaluate failed")

        return games

    @staticmethod
    def _extract_game_id(href: str) -> str:
        """URL(href)에서 game_id를 안전하게 추출합니다.

        Args:
            href: Href.
            href: Href.

        """
        try:
            if "gameId=" in href:
                return href.split("gameId=")[1].split("&", maxsplit=1)[0]
        except (IndexError, ValueError):
            logger.warning("Failed to parse game_id from href")
        return ""


async def main() -> None:
    """Test the schedule crawler."""
    crawler = ScheduleCrawler()

    # Crawl current month schedule
    now = datetime.now(KST)
    games = await crawler.crawl_schedule(now.year, now.month)

    logger.info("\n📊 Schedule Summary:")
    logger.info("Total games found: %s", len(games))

    if games:
        logger.info("\n📝 First 5 games:")
        for game in games[:5]:
            logger.info("  - %s | %s", game["game_id"], game["game_date"])


if __name__ == "__main__":
    asyncio.run(main())
