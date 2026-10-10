"""KBO Preview Crawler.

Fetch Pre-game information (Starting Pitchers, Lineups) for LLM context generation.

Uses KBO's internal XHR APIs (GetKboGameList, GetLineUpAnalysis) for stability.

"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, ClassVar

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from src.crawlers.base import BasePlaywrightCrawler
from src.crawlers.failure_taxonomy import FailureCode, stage_for_code
from src.crawlers.http_client import CrawlerHttpClient, HttpPolicy
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.models.crawl_execution import RUN_STATUS_FAILED
from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
from src.repositories.crawl_execution_repository import CrawlRunSpec
from src.services.crawl_dead_letter_service import enqueue_failure
from src.services.crawl_run_service import track_crawl_run
from src.utils.compliance import compliance, log_source_limited
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.playwright_retry import NAV_TIMEOUT
from src.utils.request_policy import RequestPolicy
from src.utils.team_codes import normalize_kbo_game_id

logger = logging.getLogger(__name__)
HTTP_API_EXCEPTIONS = (RuntimeError, ValueError, TypeError, OSError)
PLAYWRIGHT_API_EXCEPTIONS = (PlaywrightError, PlaywrightTimeoutError, RuntimeError, ValueError, TypeError, OSError)
LINEUP_PARSE_EXCEPTIONS = (json.JSONDecodeError, TypeError, ValueError, KeyError, IndexError)
PREVIEW_CRAWL_EXCEPTIONS = (*HTTP_API_EXCEPTIONS, *PLAYWRIGHT_API_EXCEPTIONS)
HOME_LINEUP_ROW_INDEX = 3
AWAY_LINEUP_ROW_INDEX = 4
MIN_LINEUP_GRID_CELLS = 3

PREVIEW_CRAWLER_NAME = "preview"
PREVIEW_TARGET_TYPE = "preview_date"
PREVIEW_PARSER_VERSION = "preview-v1"


class PreviewCrawler(BasePlaywrightCrawler):
    """PreviewCrawler class."""

    GAME_LIST_URL = "https://www.koreabaseball.com/ws/Main.asmx/GetKboGameList"
    LINEUP_URL = "https://www.koreabaseball.com/ws/Schedule.asmx/GetLineUpAnalysis"
    BASE_REFERER = "https://www.koreabaseball.com/"
    BASE_HEADERS: ClassVar[dict[str, str]] = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }

    def __init__(
        self,
        request_delay: float = 1.0,
        pool: AsyncPlaywrightPool | None = None,
        policy: RequestPolicy | None = None,
        http_client: CrawlerHttpClient | None = None,
    ) -> None:
        """Initialize PreviewCrawler.

        Args:
            request_delay: Request Delay.
            pool: Connection pool for async operations.
            policy: Optional request policy.
            http_client: Governed transport used for direct API requests. Tests
                may inject a client backed by a mock transport.

        """
        super().__init__(request_delay=request_delay, pool=pool, policy=policy or RequestPolicy())
        self._http = http_client or CrawlerHttpClient(
            name="preview_crawler",
            policy=HttpPolicy(
                base_delay_seconds=request_delay,
                timeout_seconds=30.0,
                max_attempts=max(1, self.policy.max_retries),
            ),
            headers=dict(self.BASE_HEADERS),
        )

        #: Set when the compliance policy refuses a URL during this run.
        #:
        #: The refusal used to be indistinguishable from an unreachable host:
        #: `_fetch_preview_game_list` returned None, the empty result read as
        #: "no preview data", and the run was recorded as `FETCH_HTTP_ERROR` with
        #: a retryable dead letter. No HTTP request had been made at all, and no
        #: retry can succeed while robots.txt disallows the site -- measured as
        #: 459 failed runs and 89 dead letters, 77 of them exhausted (BUG-015).
        #:
        #: Reset per run: the crawler outlives one date, and a block on one date
        #: must not be reported as the outcome of the next.
        self._source_limited_reason: str | None = None

    @staticmethod
    def _coerce_api_payload(payload: object) -> object | None:
        """Normalize API payloads from ASP.NET/JSON wrappers to a Python object.

        Args:
            payload: Payload.

        """
        if payload is None:
            return None
        if isinstance(payload, str):
            payload = payload.strip()
            if not payload:
                return None
            try:
                return PreviewCrawler._coerce_api_payload(json.loads(payload))
            except (json.JSONDecodeError, TypeError) as e:
                logger.debug("Failed to parse JSON payload: %s", e)
                return None
        if isinstance(payload, dict) and "d" in payload:
            return PreviewCrawler._coerce_api_payload(payload.get("d"))
        return payload

    @staticmethod
    def _extract_list_payload(payload: object) -> list[object]:
        """Get list-like payload from various KBO API shapes.

        Args:
            payload: Payload.

        """
        payload = PreviewCrawler._coerce_api_payload(payload)

        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ("game", "games", "result", "data"):
                value = PreviewCrawler._coerce_api_payload(payload.get(key))
                if isinstance(value, list):
                    return value
                nested = PreviewCrawler._extract_list_payload(value)
                if nested:
                    return nested
        return []

    @staticmethod
    def _clean_text(value: object) -> str:
        """Return a stripped string for nullable KBO API fields.

        Args:
            value: Value.

        """
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _to_flag(value: object) -> bool:
        """Interpret API flags (0/1/numeric/string) as bool.

        Args:
            value: Value.

        """
        try:
            return bool(int(str(value)))
        except (TypeError, ValueError):
            if isinstance(value, str):
                return value.strip() not in ("", "0", "false", "False", "FALSE")
            return bool(value)

    @staticmethod
    def _first_non_empty_text(payload: dict[str, Any], keys: tuple[str, ...]) -> str:
        for key in keys:
            value = PreviewCrawler._clean_text(payload.get(key))
            if value:
                return value
        return ""

    @staticmethod
    def _first_non_empty_value(payload: dict[str, Any], keys: tuple[str, ...]) -> object | None:
        for key in keys:
            value = payload.get(key)
            if value not in (None, ""):
                return value
        return None

    @staticmethod
    def _extract_starter_name(game: dict[str, Any], side: str) -> str:
        """Extract announced starter names from KBO game-list variants.

        Args:
            game: Game.
            side: Side.

        """
        if side == "away":
            return PreviewCrawler._first_non_empty_text(
                game,
                (
                    "T_PIT_P_NM",
                    "T_D_PIT_P_NM",
                    "AWAY_PIT_P_NM",
                    "AWAY_PITCHER_NM",
                    "AWAY_START_PIT_P_NM",
                    "W_PIT_P_NM",
                ),
            )
        return PreviewCrawler._first_non_empty_text(
            game,
            (
                "B_PIT_P_NM",
                "B_D_PIT_P_NM",
                "HOME_PIT_P_NM",
                "HOME_PITCHER_NM",
                "HOME_START_PIT_P_NM",
                "L_PIT_P_NM",
            ),
        )

    @staticmethod
    @staticmethod
    def _extract_starter_id(game: dict[str, Any], side: str) -> object | None:
        """Resolve the starter ID for the requested side using multiple key variants.

        Args:
            game: Game.
            side: Side.

        """
        if side == "away":
            return PreviewCrawler._first_non_empty_value(
                game,
                (
                    "T_PIT_P_ID",
                    "T_D_PIT_P_ID",
                    "AWAY_PIT_P_ID",
                    "AWAY_PITCHER_ID",
                    "AWAY_START_PIT_P_ID",
                    "W_PIT_P_ID",
                ),
            )
        return PreviewCrawler._first_non_empty_value(
            game,
            (
                "B_PIT_P_ID",
                "B_D_PIT_P_ID",
                "HOME_PIT_P_ID",
                "HOME_PITCHER_ID",
                "HOME_START_PIT_P_ID",
                "L_PIT_P_ID",
            ),
        )

    @staticmethod
    def _extract_lineup_announced(lineup_rows: list[Any], *, fallback: bool) -> bool:
        """Read LINEUP_CK from GetLineUpAnalysis when present.

        Args:
            lineup_rows: Lineup Rows.
            fallback: Fallback.

        """
        if not lineup_rows:
            return fallback
        first = lineup_rows[0]
        if isinstance(first, list) and first and isinstance(first[0], dict) and "LINEUP_CK" in first[0]:
            return PreviewCrawler._to_flag(first[0].get("LINEUP_CK"))
        if isinstance(first, dict) and "LINEUP_CK" in first:
            return PreviewCrawler._to_flag(first.get("LINEUP_CK"))
        return fallback

    @staticmethod
    def _extract_embedded_game_ids(payload: object) -> set[str]:
        """Collect normalized G_ID values embedded in lineup analysis payloads.

        Args:
            payload: Payload.

        """
        game_ids: set[str] = set()

        if isinstance(payload, dict):
            raw_game_id = payload.get("G_ID")
            game_id = normalize_kbo_game_id(raw_game_id) if raw_game_id else None
            if game_id:
                game_ids.add(game_id)
            for value in payload.values():
                game_ids.update(PreviewCrawler._extract_embedded_game_ids(value))
        elif isinstance(payload, list):
            for item in payload:
                game_ids.update(PreviewCrawler._extract_embedded_game_ids(item))
        return game_ids

    @staticmethod
    def _lineup_rows_match_game(lineup_rows: list[Any], game_id: str) -> bool:
        """Return False when KBO returns stale lineup rows for a different game.

        Args:
            lineup_rows: Lineup Rows.
            game_id: Game ID.

        """
        expected_game_id = normalize_kbo_game_id(game_id)

        if not expected_game_id:
            return False
        embedded_game_ids = PreviewCrawler._extract_embedded_game_ids(lineup_rows)
        return not embedded_game_ids or embedded_game_ids == {expected_game_id}

    async def _fetch_api_json(
        self,
        url: str,
        form: dict[str, Any],
        referer: str,
        page: Page | None = None,
    ) -> dict[str, object] | list[object] | None:
        """Try direct HTTP first (fast/fewer dependencies), then fallback to Playwright.

        request when a page is available.

        Args:
            url: Url.
            form: Form.
            referer: Referer.
            page: Page.

        """
        headers = dict(self.BASE_HEADERS)

        if not await compliance.is_allowed(url):
            # Recorded, not merely logged: the caller otherwise reads this None
            # as "no preview data" and reports a fetch error for a request that
            # was never made.
            self._source_limited_reason = log_source_limited(PREVIEW_CRAWLER_NAME, url)
            logger.info("[COMPLIANCE] Navigation to %s aborted.", url)
            return None

        headers["Referer"] = referer

        # 1) Direct API call through the shared transport. Its result is typed,
        # so an empty but valid response remains distinct from a failed request.
        try:
            result = await self._http.post_json(url, data=form, headers=headers)
            if result.outcome is CrawlOutcome.EMPTY:
                return []
            if not result.ok:
                logger.warning(
                    "HTTP API call failed for %s: %s",
                    url,
                    result.error_code or result.outcome.value,
                )
            else:
                payload = PreviewCrawler._coerce_api_payload(result.data)
                if isinstance(payload, (dict, list)):
                    return payload
                logger.warning("⚠️ Unexpected response type from %s: %s", url, type(payload).__name__)
        except HTTP_API_EXCEPTIONS:
            # Keep logs concise; caller may still recover via Playwright.
            logger.exception("⚠️ HTTP API call failed for %s", url)

        # 2) Fallback via Playwright request, when a page is available.
        if page is None:
            return None

        try:
            response = await self.policy.run_with_retry_async(
                page.request.post,  # type: ignore[arg-type]
                url,
                form=form,
                headers=headers,
            )
            if response.ok:  # type: ignore[attr-defined]
                payload = PreviewCrawler._coerce_api_payload(await response.json())
                if isinstance(payload, (dict, list)):
                    return payload
                logger.warning("⚠️ Unexpected Playwright response type from %s: %s", url, type(payload).__name__)
        except PLAYWRIGHT_API_EXCEPTIONS:
            logger.exception("⚠️ Playwright API call failed for %s", url)
        return None

    async def run(
        self,
        game_date: str,
        *,
        run_spec: CrawlRunSpec | None = None,
        record_dead_letters: bool = True,
    ) -> list[dict[str, Any]]:
        """Crawl one date's pregame data under a tracked run.

        The unit of work is a date: the batch asks for every game on a day, so a
        date that yields nothing is a legitimate answer and a date that could
        not be obtained at all is what the dead letter queue is for.

        Persistence stays with the caller. ``daily_preview_batch`` owns the write
        because it also writes the manifest and decides which previews are
        storable, so this records what was read and leaves ``records_written``
        to the owner.

        Args:
            game_date: Target date as ``YYYYMMDD``.
            run_spec: Optional pre-built ledger spec (replay supplies one).
            record_dead_letters: Whether an unresolved date enqueues a DLQ entry.

        Returns:
            The pregame documents for the date, or an empty list when it failed.

        """
        spec = run_spec or CrawlRunSpec(
            crawler=PREVIEW_CRAWLER_NAME,
            target_type=PREVIEW_TARGET_TYPE,
            target_id=game_date,
            game_id=game_date,
            source_url=self.GAME_LIST_URL,
            parser_version=PREVIEW_PARSER_VERSION,
        )

        with track_crawl_run(spec) as run:
            # Reset before the crawl, not after: the crawler is reused across
            # dates, and a block recorded for one date must not become the
            # reported outcome of the next.
            self._source_limited_reason = None
            previews = await self.crawl_preview_for_date(game_date)
            run.records_read = len(previews)

            # The block can surface at either fetch site: the crawl itself, or
            # the confirmation read that only happens for an empty result. So the
            # confirmation is attempted first and the reason is checked after
            # both, not between them -- checking it here instead left the common
            # path (empty crawl, block found while confirming) still reporting a
            # fetch error.
            confirmed_empty = False
            if not previews:
                confirmed_empty = await self._date_is_confirmed_empty(game_date)

            if self._source_limited_reason is not None:
                # A policy refusal is not a data outcome and not a failure: the
                # source was never consulted. Recorded the same way
                # `roster_transaction_crawler` records it, so the ledger, the
                # alerting projection and the DLQ all agree about what happened.
                #
                # Without this the run landed in the branch below as
                # `FETCH_HTTP_ERROR` with a retryable dead letter, which was
                # wrong twice: no HTTP request was made, and no retry can
                # succeed while robots.txt disallows the site. Measured as 77
                # exhausted letters from runs that could never have worked
                # (BUG-015).
                run.records_written = 0
                run.checkpoint = {
                    "outcome": "source_limited",
                    "reason": self._source_limited_reason,
                    "game_date": game_date,
                }
                logger.info(
                    "[PREVIEW] %s skipped: blocked by policy (%s)",
                    game_date,
                    self._source_limited_reason,
                )
                return []

            run.checkpoint = {"game_date": game_date, "previews": len(previews)}

            if not previews and not confirmed_empty:
                failed = CrawlResult.failure(
                    CrawlOutcome.RETRYABLE_ERROR,
                    error=f"no preview data obtained for {game_date}",
                    error_code=FailureCode.FETCH_HTTP_ERROR.value,
                    url=self.GAME_LIST_URL,
                )
                run.status = RUN_STATUS_FAILED
                run.error_code = failed.error_code
                run.error_message = failed.error
                if record_dead_letters:
                    self._enqueue_dead_letter(run.run_id, game_date, failed)
                return []

            return previews

    async def _date_is_confirmed_empty(self, game_date: str) -> bool:
        """Return whether the date genuinely holds no games to preview.

        An empty result is the normal state before first pitch, so it must not
        be recorded as a failure. An empty result is also what an unreadable
        page produces, so the two are told apart by the game's own list: a day
        with no games is confirmed empty, and a day whose games were listed but
        whose pregame data could not be read is not.

        Read without the fallback chain, so a compliance block or an
        unreachable host reads as "cannot confirm" rather than as a quiet day.

        Args:
            game_date: Target date as ``YYYYMMDD``.

        Returns:
            Whether the date is confirmed to hold no games.

        """
        if not await compliance.is_allowed(self.GAME_LIST_URL):
            # `False` means "cannot confirm", which is shared with an unreachable
            # host and an unreadable page. The block is the one case where the
            # answer will not change on retry, so it is recorded separately
            # rather than left to the caller's classification.
            self._source_limited_reason = log_source_limited(PREVIEW_CRAWLER_NAME, self.GAME_LIST_URL)
            return False
        try:
            payload = await self._http.post_json(
                self.GAME_LIST_URL,
                data={"leId": "1", "srId": "0,1,3,4,5,7,9", "date": game_date},
                headers={**self.BASE_HEADERS, "Referer": self.BASE_REFERER},
            )
        except PREVIEW_CRAWL_EXCEPTIONS:
            logger.exception("[PREVIEW] Could not confirm an empty date for %s", game_date)
            return False
        if not payload.ok:
            logger.info("[PREVIEW] Empty date for %s is unconfirmed: %s", game_date, payload.error)
            return False
        return not PreviewCrawler._extract_list_payload(payload.data)

    def _enqueue_dead_letter(self, original_run_id: str, game_date: str, result: CrawlResult[Any]) -> None:
        """Enqueue one dead letter for a date whose pregame data is missing."""
        error_code = result.error_code or FailureCode.UNKNOWN.value
        try:
            enqueue_failure(
                DeadLetterSpec(
                    original_run_id=original_run_id,
                    crawler=PREVIEW_CRAWLER_NAME,
                    target_type=PREVIEW_TARGET_TYPE,
                    # The date is the replay unit, so it is the target identity.
                    target_id=game_date,
                    game_id=game_date,
                    source_url=result.url or self.GAME_LIST_URL,
                    # Derived from the code, never supplied beside it.
                    failure_stage=stage_for_code(error_code).value,
                    error_code=error_code,
                    error_message=result.error,
                ),
            )
        except Exception:
            logger.exception("Failed to enqueue preview dead letter for %s", game_date)

    async def crawl_preview_for_date(self, game_date: str) -> list[dict[str, Any]]:
        """주어진 날짜(game_date: 'YYYYMMDD')의 모든 경기에 대해.

        선발투수와 선발 라인업(발표되었을 경우) 정보를 수집합니다.

        Returns an empty list both for a date with nothing to preview and for a
        date that could not be read. `run` is what tells those apart, and this
        stays a plain fetch so its callers are unaffected.

        Args:
            game_date: Game Date.

        """
        logger.info("🔍 Fetching Pre-game preview data for %s...", game_date)

        pool = self.pool
        owns_pool = False
        page = None
        results: list[dict[str, Any]] = []

        try:
            pool, page, owns_pool, list_data = await self._fetch_preview_game_list(
                game_date,
                pool,
                page,
                owns_pool=owns_pool,
            )

            games = PreviewCrawler._extract_list_payload(list_data)
            if not games:
                logger.info("[info] No games found or no starting pitchers announced for %s.", game_date)
                return []

            results = await self._build_preview_results(games, game_date, page)

        except PREVIEW_CRAWL_EXCEPTIONS:
            logger.exception("❌ PreviewCrawler error")
            return []
        else:
            return results
        finally:
            if page is not None and pool is not None:
                try:
                    await pool.release(page)
                except PLAYWRIGHT_API_EXCEPTIONS:
                    logger.exception("Pool release failed")
            if owns_pool and pool is not None:
                try:
                    await pool.close()
                except PLAYWRIGHT_API_EXCEPTIONS:
                    logger.exception("Pool close failed")

    async def _build_preview_results(
        self,
        games: list[Any],
        game_date: str,
        page: Page | None,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for game_row in games:
            if not isinstance(game_row, dict):
                continue
            preview_data = PreviewCrawler._build_preview_payload(game_row, game_date)
            if not preview_data:
                continue
            await self._enrich_preview_lineups(game_row, preview_data, page)
            results.append(preview_data)
            self._log_preview_result(preview_data)
        return results

    async def _fetch_preview_game_list(
        self,
        game_date: str,
        pool: AsyncPlaywrightPool | None,
        page: Page | None,
        *,
        owns_pool: bool,
    ) -> tuple[AsyncPlaywrightPool | None, Page | None, bool, dict[str, Any] | list[Any]]:
        list_payload = {"leId": "1", "srId": "0,1,3,4,5,7,9", "date": game_date}
        list_data = await self._fetch_api_json(self.GAME_LIST_URL, list_payload, self.BASE_REFERER)
        if list_data is None:
            if not await compliance.is_allowed(self.GAME_LIST_URL):
                logger.info("[COMPLIANCE] Preview fallback blocked for %s.", game_date)
                return pool, page, owns_pool, {}
            pool, page, owns_pool, list_data = await self._fetch_game_list_with_playwright(
                list_payload,
                pool,
                owns_pool=owns_pool,
            )
        if list_data is None:
            msg = f"HTTP API and Playwright fallback both failed to fetch game list for {game_date}"
            raise RuntimeError(msg)
        return pool, page, owns_pool, list_data

    async def _fetch_game_list_with_playwright(
        self,
        list_payload: dict[str, str],
        pool: AsyncPlaywrightPool | None,
        *,
        owns_pool: bool,
    ) -> tuple[AsyncPlaywrightPool, Page, bool, dict[str, Any] | list[Any] | None]:
        active_pool = pool or AsyncPlaywrightPool(max_pages=1)
        owns_pool = owns_pool or pool is None
        try:
            await active_pool.start()
        except PLAYWRIGHT_API_EXCEPTIONS as e:
            logger.exception("⚠️ Playwright fallback failed")
            msg = "Failed to start Playwright fallback pool"
            raise RuntimeError(msg) from e
        page = await active_pool.acquire()
        await page.goto(self.BASE_REFERER, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        await asyncio.sleep(self.request_delay)
        list_data = await self._fetch_api_json(self.GAME_LIST_URL, list_payload, self.BASE_REFERER, page=page)
        return active_pool, page, owns_pool, list_data

    @staticmethod
    def _build_preview_payload(game_row: dict[str, Any], game_date: str) -> dict[str, Any] | None:
        raw_id = game_row.get("G_ID")
        game_id = normalize_kbo_game_id(raw_id) if raw_id else None
        if not game_id:
            return None
        away_starter = PreviewCrawler._extract_starter_name(game_row, "away")
        home_starter = PreviewCrawler._extract_starter_name(game_row, "home")
        return {
            "game_id": game_id,
            "game_date": game_date,
            "stadium": PreviewCrawler._clean_text(game_row.get("S_NM")) or None,
            "start_time": PreviewCrawler._clean_text(game_row.get("G_TM")) or None,
            "away_team_name": PreviewCrawler._clean_text(game_row.get("AWAY_NM")),
            "home_team_name": PreviewCrawler._clean_text(game_row.get("HOME_NM")),
            "away_starter": away_starter,
            "away_starter_id": PreviewCrawler._extract_starter_id(game_row, "away"),
            "home_starter": home_starter,
            "home_starter_id": PreviewCrawler._extract_starter_id(game_row, "home"),
            "start_pitcher_announced": PreviewCrawler._to_flag(game_row.get("START_PIT_CK"))
            or bool(away_starter and home_starter),
            "lineup_announced": PreviewCrawler._to_flag(game_row.get("LINEUP_CK")),
            "away_lineup": [],
            "home_lineup": [],
        }

    async def _enrich_preview_lineups(
        self,
        game_row: dict[str, Any],
        preview_data: dict[str, Any],
        page: Page | None,
    ) -> None:
        await asyncio.sleep(self.request_delay)
        lineup_payload = {
            "leId": str(game_row.get("LE_ID", 1)),
            "srId": str(game_row.get("SR_ID", 0)),
            "seasonId": str(game_row.get("SEASON_ID", str(preview_data["game_date"])[:4])),
            "gameId": preview_data["game_id"],
        }
        lineup_data = await self._fetch_api_json(
            self.LINEUP_URL,
            lineup_payload,
            "https://www.koreabaseball.com/Schedule/GameCenter/Preview/LineUp.aspx",
            page=page,
        )
        if not lineup_data:
            return
        try:
            extracted = PreviewCrawler._extract_list_payload(lineup_data)
            PreviewCrawler._apply_lineup_payload(
                preview_data,
                list(extracted),
            )
        except LINEUP_PARSE_EXCEPTIONS:
            logger.exception("⚠️ Error parsing lineup for %s", preview_data["game_id"])

    @staticmethod
    def _apply_lineup_payload(preview_data: dict[str, Any], lineup_rows: list[Any]) -> None:
        preview_data["lineup_announced"] = PreviewCrawler._extract_lineup_announced(
            lineup_rows,
            fallback=bool(preview_data["lineup_announced"]),
        )
        if not PreviewCrawler._lineup_rows_match_game(lineup_rows, preview_data["game_id"]):
            logger.warning("⚠️ Ignoring stale lineup payload for %s", preview_data["game_id"])
            return
        if not preview_data["lineup_announced"]:
            return
        if len(lineup_rows) > HOME_LINEUP_ROW_INDEX:
            preview_data["home_lineup"] = PreviewCrawler._parse_lineup_grid(lineup_rows[HOME_LINEUP_ROW_INDEX])
        if len(lineup_rows) > AWAY_LINEUP_ROW_INDEX:
            preview_data["away_lineup"] = PreviewCrawler._parse_lineup_grid(lineup_rows[AWAY_LINEUP_ROW_INDEX])

    @staticmethod
    def _log_preview_result(preview_data: dict[str, Any]) -> None:
        logger.info(
            "✅ Preview Extracted: %s (Starter: %s vs %s) [Lineups: %d vs %d]",
            preview_data["game_id"],
            preview_data["away_starter"],
            preview_data["home_starter"],
            len(preview_data["away_lineup"]),
            len(preview_data["home_lineup"]),
        )

    @staticmethod
    def _parse_lineup_grid(grid_str_list: list[Any]) -> list[dict[str, Any]]:
        """Parse the nested KBO Lineup grid JSON string into a structured list.

        Args:
            grid_str_list: Grid Str List.

        """
        lineup: list[dict[str, Any]] = []

        if not grid_str_list or not isinstance(grid_str_list, list):
            return lineup

        try:
            grid_data = json.loads(grid_str_list[0])
            rows = grid_data.get("rows", [])
            for row in rows:
                cells = row.get("row", [])
                if len(cells) >= MIN_LINEUP_GRID_CELLS:
                    order = str(cells[0].get("Text", "")).strip()
                    pos = str(cells[1].get("Text", "")).strip()
                    name = str(cells[2].get("Text", "")).strip()

                    if order.isdigit():
                        lineup.append(
                            {
                                "batting_order": int(order),
                                "position": pos,
                                "player_name": name,
                            },
                        )
        except (json.JSONDecodeError, TypeError, KeyError, IndexError) as e:
            logger.debug("Failed to parse lineup grid row: %s", e)
        return lineup
