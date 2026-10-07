"""KBO Schedule Crawler POC.

Collects game IDs from the KBO schedule page.

"""

from __future__ import annotations

import asyncio
import calendar
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page

from src.constants import DATE_STR_LEN, KST
from src.crawlers.base import BasePlaywrightCrawler
from src.crawlers.failure_taxonomy import FailureCode, stage_for_code
from src.crawlers.http_client import CrawlerHttpClient, HttpPolicy
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.models.crawl_execution import RUN_STATUS_FAILED
from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
from src.repositories.crawl_execution_repository import CrawlRunSpec
from src.services.crawl_dead_letter_service import enqueue_failure
from src.services.crawl_run_service import track_crawl_run
from src.services.schedule_collection_service import save_schedule_games
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

if TYPE_CHECKING:
    from src.services.game_write_contract import GameWriteContract
    from src.utils.playwright_pool import AsyncPlaywrightPool
    from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)
SCHEDULE_CRAWLER_NAME = "schedule"
SCHEDULE_TARGET_TYPE = "schedule_month"

SCHEDULE_CRAWLER_EXCEPTIONS = (PlaywrightError, TimeoutError, RuntimeError, ValueError, TypeError, KeyError, OSError)

NAVER_SCHEDULE_API_URL = "https://api-gw.sports.naver.com/schedule/today-games"
NAVER_SPORTS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    "Origin": "https://m.sports.naver.com",
    "Referer": "https://m.sports.naver.com/",
}
NAVER_STATUS_TO_GAME_STATUS = {
    "BEFORE": GAME_STATUS_SCHEDULED,
    "LIVE": GAME_STATUS_LIVE,
    "RESULT": GAME_STATUS_COMPLETED,
    "CANCEL": GAME_STATUS_CANCELLED,
}
NAVER_DATETIME_MIN_LEN = 16
KBO_HOME_STADIUM_BY_TEAM: dict[str, str] = {
    "OB": "잠실",
    "LG": "잠실",
    "WO": "고척",
    "SK": "문학",
    "KT": "수원",
    "HH": "한밭",
    "HT": "광주",
    "SS": "대구",
    "LT": "사직",
    "NC": "창원",
    "MBC": "잠실",
    "TH": "광주",
    "BG": "한밭",
    "CW": "인천",
}


class ScheduleCrawler(BasePlaywrightCrawler):
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
        http_client: CrawlerHttpClient | None = None,
    ) -> None:
        """Initialize a new instance.

        Args:
            request_delay: Request Delay.
            pool: Connection pool for async operations.
            policy: Policy.
            http_client: Naver API transport. Defaults to a client that owns
                throttling, retry, and the circuit breaker. Tests inject a client
                backed by a mock transport.

        """
        super().__init__(request_delay=request_delay, pool=pool, policy=policy)
        self.base_url = SCHEDULE
        self._last_failure_reason: dict[str, str] = {}
        # Only the Naver API goes through this client; the KBO schedule page is
        # still a browser crawl, so it keeps using the Playwright policy.
        self._http = http_client or CrawlerHttpClient(
            name=SCHEDULE_CRAWLER_NAME,
            headers=dict(NAVER_SPORTS_HEADERS),
            policy=HttpPolicy(timeout_seconds=20.0, max_attempts=2),
        )

    def get_last_failure_reason(self, key: str) -> str | None:
        """Get last failure reason.

        Args:
            key: Key.

        Returns:
            The result of the operation.

        """
        return self._last_failure_reason.get(key)

    def _schedule_key(self, year: int, month: int, series_id: str | None = None) -> str:
        suffix = series_id if series_id is not None else "all"
        return f"{year}-{month:02d}:{suffix}"

    async def lookup_month(self, year: int, month: int, series_id: str | None = None) -> CrawlResult[list[dict]]:
        """Read one month of schedule without recording a run or a dead letter.

        This exists for callers that need the schedule as a *dependency* rather
        than as work of their own. The live polling loop is the motivating case:
        it asks "which games are today" every cycle, and routing that through
        :meth:`crawl_schedule` made each cycle a ledger row and, on failure, a
        dead letter -- so one day of a two-minute polling loop produced 422 runs
        of the same month and up to 422 x 31 upstream API calls.

        Neither record is truthful here. The loop did not do a month's worth of
        work, so the ledger said it had; and a source that is merely unavailable
        to a dependency read is not a unit of work that needs reprocessing, so
        the DLQ said it did.

        The result still carries its classification, so the caller can decide
        what an unreadable month means for it -- the live loop falls back to
        games already in the database.

        Args:
            year: Season year.
            month: Month (1-12).
            series_id: Optional series filter, which skips the Naver path.

        Returns:
            The classified month result, with no ledger or DLQ side effects.

        """
        return await self._resolve_month(year, month, series_id)

    async def crawl_schedule(  # noqa: PLR0913
        self,
        year: int,
        month: int,
        series_id: str | None = None,
        *,
        save: bool = False,
        write_contract: GameWriteContract | None = None,
        run_spec: CrawlRunSpec | None = None,
        record_dead_letters: bool = True,
    ) -> list[dict]:
        """지정된 연도와 월의 경기 일정을 크롤링하는 메인 메서드.

        The unit of work is a month, so the ledger row is a month. A month fails
        only when neither the Naver API nor the KBO page could answer it; the
        schedule feeds nearly every other crawl, so a silent gap here would go on
        to look like missing data everywhere downstream.

        When ``save`` is set, persistence happens **inside** this ledger context so
        ``records_written`` describes what actually landed. Previously every caller
        saved after the context had closed, so the row always reported ``written=0``
        and a run that persisted nothing looked identical to one that persisted all.

        Args:
            year: 시즌 연도 (예: 2024)
            month: 월 (1-12)
            series_id: 시리즈 ID (옵션)
            save: 이 달의 경기를 이 원장 안에서 저장할지.
            write_contract: 저장 시 사용할 쓰기 계약 (호출자의 라벨 유지).
            run_spec: 사전에 만들어 둔 ledger 명세 (replay가 전달).
            record_dead_letters: 실패한 달을 DLQ에 넣을지.

        Returns:
            경기 정보 딕셔너리가 담긴 리스트.

        """
        logger.info("🔍 Crawling schedule for %s-%02d (Series: %s)...", year, month, series_id)
        target = f"{year}-{month:02d}"
        spec = run_spec or CrawlRunSpec(
            crawler=SCHEDULE_CRAWLER_NAME,
            target_type=SCHEDULE_TARGET_TYPE,
            target_id=target,
            season=year,
            source_url=self.base_url,
        )

        with track_crawl_run(spec) as run:
            result = await self._resolve_month(year, month, series_id)
            games = result.data or []
            run.records_read = len(games)
            if not result.ok and result.outcome is not CrawlOutcome.EMPTY:
                run.status = RUN_STATUS_FAILED
                run.error_code = result.error_code or FailureCode.UNKNOWN.value
                run.error_message = result.error
                if record_dead_letters:
                    self._enqueue_dead_letter(run.run_id, target, result)
                return games
            if save:
                save_result = save_schedule_games(
                    games,
                    log=logger.info,
                    write_contract=write_contract,
                    source_reason=f"schedule_refresh:{target}",
                )
                run.records_written = save_result.saved
                # A filtered row is not merely unsaved, it was rejected as invalid;
                # counting it as failed keeps read/accounted-for reconcilable.
                run.records_failed = save_result.failed + save_result.filtered
            return games

    async def _resolve_month(
        self,
        year: int,
        month: int,
        series_id: str | None = None,
    ) -> CrawlResult[list[dict]]:
        """Resolve one month from the Naver API, falling back to the KBO page.

        The browser is for a source that could not answer, never for one that
        legitimately has no games.

        Args:
            year: Season year.
            month: Month (1-12).
            series_id: Optional series filter, which skips the Naver path.

        Returns:
            The resolved result for the month.

        """
        naver: CrawlResult[list[dict]] | None = None
        if series_id in (None, "0"):
            naver = await self._crawl_naver_month(year, month)
            if naver.ok:
                logger.info("✅ Found %s games (Naver API)", len(naver.data or []))
                return naver
            if naver.outcome is CrawlOutcome.EMPTY:
                # Every day answered and none of them had a game: an off-season
                # month is data, not an outage. The browser would only confirm it.
                logger.info("Naver reports no games for %s-%02d; not falling back", year, month)
                return naver

        schedule_key = self._schedule_key(year, month, series_id)
        if not await self._kbo_fallback_allowed(schedule_key):
            blocked = CrawlResult.failure(
                CrawlOutcome.PERMANENT_ERROR,
                error="kbo schedule fallback blocked by robots policy",
                error_code=FailureCode.FETCH_BLOCKED.value,
            )
            return self._merge_failures(naver, blocked) if naver is not None else blocked
        logger.info("Naver schedule unusable for %s-%02d, falling back to KBO page", year, month)

        try:
            async with self.page_context() as page:
                games = await self._crawl_month(page, year, month, series_id=series_id)
        except SCHEDULE_CRAWLER_EXCEPTIONS as exc:
            logger.exception("❌ Error crawling schedule")
            failure = CrawlResult.failure(
                CrawlOutcome.PERMANENT_ERROR,
                error=f"kbo schedule page failed: {type(exc).__name__}: {exc}",
                error_code=FailureCode.FETCH_HTTP_ERROR.value,
            )
            return self._merge_failures(naver, failure) if naver is not None else failure
        logger.info("✅ Found %s games", len(games))
        # A page that renders with no rows is an off-season month, not a failure.
        return CrawlResult.success(games) if games else CrawlResult.empty()

    @staticmethod
    def _merge_failures(
        primary: CrawlResult[Any] | None,
        fallback: CrawlResult[Any],
    ) -> CrawlResult[list[dict]]:
        """Combine two failed sources into one, keeping the primary's cause.

        The Naver API is the primary attempt, so a replay starts from it and its
        code is the canonical cause. The fallback's code is only promoted when
        the primary had nothing meaningful to say, and both are always preserved
        in the message so neither is lost.
        """
        if primary is None or primary.ok or primary.outcome is CrawlOutcome.EMPTY:
            return fallback
        code = primary.error_code
        meaningless = {None, FailureCode.UNKNOWN.value}
        if code in meaningless:
            code = fallback.error_code
        return CrawlResult.failure(
            CrawlOutcome.PERMANENT_ERROR,
            error=(
                f"naver[{primary.error_code or primary.outcome}]: {primary.error}; "
                f"kbo[{fallback.error_code or fallback.outcome}]: {fallback.error}"
            ),
            error_code=code or FailureCode.UNKNOWN.value,
        )

    def _enqueue_dead_letter(
        self,
        original_run_id: str,
        target: str,
        result: CrawlResult[Any],
    ) -> None:
        """Enqueue one dead letter for a month that could not be obtained."""
        code = result.error_code or FailureCode.UNKNOWN.value
        try:
            enqueue_failure(
                DeadLetterSpec(
                    original_run_id=original_run_id,
                    crawler=SCHEDULE_CRAWLER_NAME,
                    target_type=SCHEDULE_TARGET_TYPE,
                    # The month is the replay unit, so it is the target identity.
                    target_id=target,
                    source_url=self.base_url,
                    # Derived from the code, never supplied beside it.
                    failure_stage=stage_for_code(code).value,
                    error_code=code,
                    error_message=result.error,
                ),
            )
        except Exception:
            logger.exception("Failed to enqueue dead letter for schedule %s", target)

    async def crawl_season(
        self,
        year: int,
        months: list[int] | None = None,
        series_id: str | None = None,
        *,
        save: bool = False,
        write_contract: GameWriteContract | None = None,
    ) -> list[dict]:
        """주어진 시즌의 여러 달에 걸쳐 경기 일정을 크롤링합니다.

        Every month goes through :meth:`crawl_schedule`, so each one gets its own
        ledger row and — with ``save`` — its own persistence. The month is the unit the
        ledger, the DLQ and the retry policy all agree on, and this method previously
        bypassed the ledger entirely by accumulating the whole season itself.

        Trade-off: the browser fallback used to be opened once per season. It is now
        opened per month that actually needs it, which is bounded by the months Naver
        could not answer.

        Args:
            year: 시즌 연도
            months: 크롤링할 월 목록 (기본값: 3월-10월)
            series_id: 시리즈 ID (옵션)
            save: 각 달의 경기를 해당 원장 안에서 저장할지.
            write_contract: 저장 시 사용할 쓰기 계약.

        """
        months = months or list(range(3, 11))

        all_games: list[dict] = []
        for month in months:
            all_games.extend(
                await self.crawl_schedule(
                    year,
                    month,
                    series_id,
                    save=save,
                    write_contract=write_contract,
                ),
            )
        return all_games

    async def _kbo_fallback_allowed(self, key: str) -> bool:
        """Check whether a KBO page fallback is permitted by robots policy."""
        if await compliance.is_allowed(self.base_url):
            return True
        self._last_failure_reason[key] = "kbo_robots_blocked"
        logger.info("[COMPLIANCE] KBO schedule fallback blocked for %s", key)
        return False

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

    async def _crawl_naver_month(self, year: int, month: int) -> CrawlResult[list[dict]]:
        """Fetch a month of games from the Naver sports schedule API.

        The API is queried day by day, so a month is only trustworthy when every
        day answered. That is what separates the three outcomes the caller cares
        about:

        * games found -> ``SUCCESS``
        * every day answered with no games -> ``EMPTY``, a real off-season month
        * any day failed -> a failure, because the month is incomplete and the KBO
          page should be tried instead of reporting a partial month as fact

        Args:
            year: Season year.
            month: Month (1-12).

        Returns:
            A classified result carrying the schedule payloads.

        """
        crawl_key = self._schedule_key(year, month, "naver")
        self._last_failure_reason.pop(crawl_key, None)

        games: list[dict] = []
        dh_counts: dict[tuple[str, str, str], int] = {}
        total_days = calendar.monthrange(year, month)[1]
        failed_days = 0
        first_code: str | None = None

        for day in range(1, total_days + 1):
            result = await self._http.fetch_json(
                NAVER_SCHEDULE_API_URL,
                params={
                    "sectionId": "kbaseball",
                    "categoryId": "kbo",
                    "seasonYear": str(year),
                    "date": f"{year}-{month:02d}-{day:02d}",
                },
            )
            if result.outcome is CrawlOutcome.EMPTY:
                # A date with no games is an answer, not an outage. Treating it
                # as a failure would send every off-season day to the browser.
                continue
            if not result.ok:
                failed_days += 1
                if first_code is None:
                    first_code = result.error_code
                logger.warning(
                    "Naver schedule day %s-%02d-%02d failed: %s",
                    year,
                    month,
                    day,
                    result.error,
                )
                continue
            raw_games = (result.data.get("result") or {}).get("games") or []
            for raw in raw_games:
                game = self._naver_game_to_payload(raw, year, month, dh_counts)
                if game is not None:
                    games.append(game)

        if failed_days:
            self._last_failure_reason[crawl_key] = "naver_api_failure"
            return CrawlResult.failure(
                CrawlOutcome.PERMANENT_ERROR,
                error=f"{failed_days} of {total_days} days failed (first: {first_code})",
                error_code=first_code or FailureCode.FETCH_HTTP_ERROR.value,
            )
        if not games:
            self._last_failure_reason[crawl_key] = "naver_api_empty"
            return CrawlResult.empty()
        return CrawlResult.success(games)

    def _naver_game_to_payload(
        self,
        raw: dict[str, Any],
        year: int,
        month: int,
        dh_counts: dict[tuple[str, str, str], int],
    ) -> dict[str, Any] | None:
        """Map a Naver schedule entry to the KBO schedule payload schema.

        Args:
            raw: Naver schedule game entry.
            year: Season year.
            month: Month.
            dh_counts: Double-header counter keyed by (date, away, home).

        Returns:
            Schedule payload or None when the entry is not a valid KBO game.

        """
        if raw.get("cancel") or raw.get("suspended"):
            return None
        away_code = str(raw.get("awayTeamCode") or "").strip()
        home_code = str(raw.get("homeTeamCode") or "").strip()
        if not away_code or not home_code:
            return None
        game_date = str(raw.get("gameDate") or "").replace("-", "")
        if len(game_date) != DATE_STR_LEN or not game_date.isdigit():
            return None

        dh_key = (game_date, away_code, home_code)
        dh_no = dh_counts.get(dh_key, 0)
        dh_counts[dh_key] = dh_no + 1

        game_id = f"{game_date}{away_code}{home_code}{dh_no}"
        status = NAVER_STATUS_TO_GAME_STATUS.get(
            str(raw.get("statusCode") or "").upper(),
            GAME_STATUS_SCHEDULED,
        )
        game_time = None
        date_time = str(raw.get("gameDateTime") or "")
        if len(date_time) >= NAVER_DATETIME_MIN_LEN:
            game_time = date_time[11:16]

        schedule_game = {
            "game_id": normalize_kbo_game_id(game_id),
            "game_date": f"{game_date[:4]}-{game_date[4:6]}-{game_date[6:]}",
            "season_year": year,
            "season_type": "regular",
            "away_team_code": away_code,
            "home_team_code": home_code,
            "doubleheader_no": dh_no,
            "game_status": status,
            "crawl_status": "naver_api",
            "game_time": game_time,
            "stadium": KBO_HOME_STADIUM_BY_TEAM.get(home_code),
        }
        is_valid, failure_reason = validate_schedule_game_payload(
            schedule_game,
            expected_year=year,
            expected_month=month,
        )
        if not is_valid:
            logger.warning("Filtered Naver schedule row: %s reason=%s", game_id, failure_reason)
            return None
        return schedule_game

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
