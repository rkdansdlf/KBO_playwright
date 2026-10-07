"""Crawler for Naver Sports news and MLBPark Bullpen discussions."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.crawlers.failure_taxonomy import (
    CrawlPersistError,
    FailureCode,
    FailureStage,
    classify_failure,
    classify_persist_failure,
    stage_for_code,
)
from src.crawlers.http_client import CrawlerHttpClient, HttpPolicy
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.db.engine import get_db_session
from src.models.crawl_execution import RUN_STATUS_FAILED, CrawlExecutionRun
from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
from src.repositories.crawl_execution_repository import CrawlRunSpec
from src.repositories.source_registry_repository import save_raw_snapshots
from src.services.crawl_dead_letter_service import enqueue_failure
from src.services.crawl_run_service import track_crawl_run
from src.utils.async_bridge import run_coro_blocking

logger = logging.getLogger(__name__)

REALTIME_ISSUE_CRAWLER_NAME = "realtime_issue"
REALTIME_ISSUE_TARGET_TYPE = "realtime_issue_source"
REALTIME_ISSUE_PARSER_VERSION = "realtime-issue-v1"
NAVER_NEWS_TARGET_ID = "naver_news"
MLBPARK_BULLPEN_TARGET_ID = "mlbpark_bullpen"
NAVER_NEWS_HTML_URL = "https://sports.news.naver.com/kbaseball/news/index"
MLBPARK_BULLPEN_URL = "https://mlbpark.donga.com/mp/b.php?b=bullpen"
REALTIME_FETCH_EXCEPTIONS = (httpx.HTTPError, OSError, RuntimeError, ValueError, TypeError, KeyError)
SourceFetch = Callable[[], Awaitable[CrawlResult[list[dict[str, Any]]]]]


class RealtimeIssueCrawler:
    """Scrape and track real-time baseball headlines and forum discussions.

    Naver news and MLBPark are independent source units. Each gets its own run
    ledger row and dead letter, so a healthy source is not replayed because the
    other one failed. The public fetch methods remain synchronous for existing
    callers; the tracked ``run`` entrypoint is asynchronous for replay and new
    callers.
    """

    def __init__(self, timeout: int = 15, *, http_client: CrawlerHttpClient | None = None) -> None:
        """Initialize the crawler and its optional test transport.

        Args:
            timeout: Per-request timeout in seconds.
            http_client: Optional shared transport. Production clients are
                created per host so one source's circuit cannot block another.

        """
        self.timeout = timeout
        self._injected_http_client = http_client
        self._http_clients: dict[str, CrawlerHttpClient] = {}
        self._raw_pages: list[dict[str, Any]] = []
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        }

    def _http_for(self, url: str) -> CrawlerHttpClient:
        """Return a host-isolated governed HTTP client."""
        if self._injected_http_client is not None:
            return self._injected_http_client
        host = urlparse(url).hostname or "unknown"
        if host not in self._http_clients:
            self._http_clients[host] = CrawlerHttpClient(
                name=f"{REALTIME_ISSUE_CRAWLER_NAME}:{host}",
                policy=HttpPolicy(timeout_seconds=float(self.timeout), max_attempts=1),
                headers=self.headers,
            )
        return self._http_clients[host]

    def fetch_naver_news_headlines(
        self,
        *,
        save: bool = False,
        run_spec: CrawlRunSpec | None = None,
        record_dead_letters: bool = True,
    ) -> list[dict[str, Any]]:
        """Fetch Naver news, falling back to its HTML page when the API fails.

        Args:
            save: Persist captured raw source pages.
            run_spec: Optional run description used by a replay.
            record_dead_letters: Whether an unresolved source failure is queued.

        Returns:
            Parsed article documents, or an empty list when the source failed or
            answered with no articles.

        """
        return run_coro_blocking(
            self.run(
                target_id=NAVER_NEWS_TARGET_ID,
                save=save,
                run_spec=run_spec,
                record_dead_letters=record_dead_letters,
            ),
        )

    def fetch_mlbpark_bullpen_posts(
        self,
        *,
        save: bool = False,
        run_spec: CrawlRunSpec | None = None,
        record_dead_letters: bool = True,
    ) -> list[dict[str, Any]]:
        """Fetch popular MLBPark Bullpen threads.

        Args:
            save: Persist captured raw source pages.
            run_spec: Optional run description used by a replay.
            record_dead_letters: Whether an unresolved source failure is queued.

        Returns:
            Parsed post documents, or an empty list when the source failed or
            answered with no matching threads.

        """
        return run_coro_blocking(
            self.run(
                target_id=MLBPARK_BULLPEN_TARGET_ID,
                save=save,
                run_spec=run_spec,
                record_dead_letters=record_dead_letters,
            ),
        )

    async def run(
        self,
        *,
        target_id: str | None = None,
        save: bool = False,
        run_spec: CrawlRunSpec | None = None,
        record_dead_letters: bool = True,
        raise_on_persist_error: bool = False,
    ) -> list[dict[str, Any]]:
        """Run one source unit, or both independent units when no target is given.

        Args:
            target_id: Source unit to replay. ``None`` runs both sources.
            save: Persist newly captured raw pages.
            run_spec: Optional ledger spec. A single spec cannot describe both
                source units, so it is only accepted with a target id.
            record_dead_letters: Whether a source failure enqueues a DLQ entry.
            raise_on_persist_error: Raise raw snapshot persistence failures so a
                replay cannot claim success when its evidence write failed.

        Returns:
            Parsed documents from the selected source unit(s).

        Raises:
            ValueError: If ``target_id`` is not a supported source, or a single
                run spec is supplied for a two-source run.

        """
        if target_id is None:
            if run_spec is not None:
                message = "run_spec requires a single realtime issue target"
                raise ValueError(message)
            documents: list[dict[str, Any]] = []
            for source_id in (NAVER_NEWS_TARGET_ID, MLBPARK_BULLPEN_TARGET_ID):
                documents.extend(
                    await self._run_source(
                        source_id,
                        save=save,
                        run_spec=None,
                        record_dead_letters=record_dead_letters,
                        raise_on_persist_error=raise_on_persist_error,
                    ),
                )
            return documents

        if target_id == NAVER_NEWS_TARGET_ID:
            source_url = self._naver_news_api_url()
            fetch = self._fetch_naver_news
        elif target_id == MLBPARK_BULLPEN_TARGET_ID:
            source_url = MLBPARK_BULLPEN_URL
            fetch = self._fetch_mlbpark_posts
        else:
            message = f"unsupported realtime issue target: {target_id}"
            raise ValueError(message)

        return await self._run_source(
            target_id,
            source_url=source_url,
            fetch=fetch,
            save=save,
            run_spec=run_spec,
            record_dead_letters=record_dead_letters,
            raise_on_persist_error=raise_on_persist_error,
        )

    async def _run_source(  # noqa: PLR0913 - source-run dimensions match the crawler reliability contract
        self,
        target_id: str,
        *,
        source_url: str | None = None,
        fetch: SourceFetch | None = None,
        save: bool,
        run_spec: CrawlRunSpec | None,
        record_dead_letters: bool,
        raise_on_persist_error: bool,
    ) -> list[dict[str, Any]]:
        """Track one source fetch, parse, and optional evidence write."""
        if source_url is None:
            source_url = self._naver_news_api_url() if target_id == NAVER_NEWS_TARGET_ID else MLBPARK_BULLPEN_URL
        if fetch is None:
            fetch = self._fetch_naver_news if target_id == NAVER_NEWS_TARGET_ID else self._fetch_mlbpark_posts
        spec = run_spec or CrawlRunSpec(
            crawler=REALTIME_ISSUE_CRAWLER_NAME,
            target_type=REALTIME_ISSUE_TARGET_TYPE,
            target_id=target_id,
            source_url=source_url,
            parser_version=REALTIME_ISSUE_PARSER_VERSION,
        )
        raw_start = len(self._raw_pages)

        with track_crawl_run(spec) as run:
            try:
                result = await fetch()
            except REALTIME_FETCH_EXCEPTIONS as exc:
                result = self._failure_from_exception(exc, source_url)

            documents = result.data if result.ok and isinstance(result.data, list) else []
            run.records_read = len(documents)
            run.records_failed = 0
            run.checkpoint = {"outcome": str(result.outcome), "source_id": target_id, "url": result.url or source_url}

            if save and len(self._raw_pages) > raw_start:
                snapshots = self._raw_pages[raw_start:]
                written, persist_failure, persist_exception = self._persist_snapshots(snapshots, source_url)
                if persist_failure is None:
                    run.records_written = written
                elif result.outcome in {CrawlOutcome.SUCCESS, CrawlOutcome.EMPTY}:
                    self._fail_source_run(
                        run,
                        target_id,
                        source_url,
                        persist_failure,
                        record_dead_letters=record_dead_letters,
                    )
                    run.checkpoint["outcome"] = "persist_failed"
                    if raise_on_persist_error:
                        code = FailureCode(persist_failure.error_code or FailureCode.PERSIST_CONNECTION.value)
                        error = persist_failure.error or "snapshot persistence failed"
                        raise CrawlPersistError(error, error_code=code) from persist_exception
                    return []
                else:
                    logger.error(
                        "Raw snapshot persistence also failed for realtime issue source %s: %s",
                        target_id,
                        persist_failure.error,
                    )

            if result.outcome not in {CrawlOutcome.SUCCESS, CrawlOutcome.EMPTY}:
                self._fail_source_run(
                    run,
                    target_id,
                    source_url,
                    result,
                    record_dead_letters=record_dead_letters,
                )
                return []

            return documents

    @staticmethod
    def _persist_snapshots(
        snapshots: list[dict[str, Any]],
        source_url: str,
    ) -> tuple[int | None, CrawlResult[Any] | None, Exception | None]:
        """Persist captured pages, returning any typed write failure."""
        try:
            with get_db_session() as session:
                written = save_raw_snapshots(session, snapshots)
        except (SQLAlchemyError, OSError, RuntimeError, TimeoutError) as exc:
            _, code = classify_persist_failure(exc)
            failure = CrawlResult.failure(
                CrawlOutcome.PERMANENT_ERROR,
                error=str(exc),
                error_code=code.value,
                url=source_url,
            )
            return None, failure, exc
        return written, None, None

    def _fail_source_run(
        self,
        run: CrawlExecutionRun,
        target_id: str,
        source_url: str,
        result: CrawlResult[Any],
        *,
        record_dead_letters: bool,
    ) -> None:
        """Mark one source run failed and optionally enqueue its replay unit."""
        self._mark_failed(run, result)
        if record_dead_letters:
            self._enqueue_dead_letter(run.run_id, target_id, source_url, result)

    @staticmethod
    def _mark_failed(run: CrawlExecutionRun, result: CrawlResult[Any]) -> None:
        """Copy a classified source failure onto its tracked run."""
        run.status = RUN_STATUS_FAILED
        run.error_code = result.error_code or FailureCode.UNKNOWN.value
        run.error_message = result.error
        run.records_failed = 1

    def _enqueue_dead_letter(
        self,
        original_run_id: str,
        target_id: str,
        source_url: str,
        result: CrawlResult[Any],
    ) -> None:
        """Queue one failed source unit for operator or scheduled replay."""
        error_code = result.error_code or FailureCode.UNKNOWN.value
        try:
            enqueue_failure(
                DeadLetterSpec(
                    original_run_id=original_run_id,
                    crawler=REALTIME_ISSUE_CRAWLER_NAME,
                    target_type=REALTIME_ISSUE_TARGET_TYPE,
                    target_id=target_id,
                    source_url=source_url,
                    failure_stage=stage_for_code(error_code).value,
                    error_code=error_code,
                    error_message=result.error,
                ),
            )
        except Exception:
            logger.exception("Failed to enqueue realtime issue dead letter for %s", target_id)

    @staticmethod
    def _failure_from_exception(exc: BaseException, url: str) -> CrawlResult[Any]:
        """Classify an exception escaping the shared request or parser path."""
        stage, code = classify_failure(exc)
        outcome = CrawlOutcome.RETRYABLE_ERROR if stage is FailureStage.FETCH else CrawlOutcome.SCHEMA_CHANGED
        return CrawlResult.failure(outcome, error=str(exc), error_code=code.value, url=url)

    async def _fetch_naver_news(self) -> CrawlResult[list[dict[str, Any]]]:
        """Fetch Naver JSON, then fall back to the HTML listing if needed."""
        api_url = self._naver_news_api_url()
        api_headers = {
            **self.headers,
            "Referer": NAVER_NEWS_HTML_URL,
            "Origin": "https://sports.news.naver.com",
        }
        api_result = await self._http_for(api_url).fetch_text(api_url, headers=api_headers)
        self._capture_page("naver_sports_news", api_result, api_url)
        if api_result.ok:
            parsed = self._parse_naver_api_result(api_result)
            if parsed.outcome is CrawlOutcome.SUCCESS or parsed.outcome is CrawlOutcome.EMPTY:
                return parsed
            api_result = parsed
        elif api_result.outcome is CrawlOutcome.EMPTY:
            api_result = self._schema_failure(api_result, "Naver API returned an empty response body")

        logger.info("Naver news API failed; trying the HTML listing (%s)", api_result.error)
        fallback_result = await self._http_for(NAVER_NEWS_HTML_URL).fetch_text(
            NAVER_NEWS_HTML_URL,
            headers=self.headers,
        )
        self._capture_page("naver_sports_news", fallback_result, NAVER_NEWS_HTML_URL)
        if fallback_result.ok:
            body = fallback_result.data
            if not isinstance(body, str):
                fallback_result = self._schema_failure(fallback_result, "Naver HTML fallback returned non-text data")
            else:
                try:
                    articles = self._parse_naver_news_html(BeautifulSoup(body, "html.parser"))
                except (TypeError, ValueError) as exc:
                    fallback_result = self._schema_failure(fallback_result, f"Naver HTML parse failed: {exc}")
                else:
                    if articles:
                        return CrawlResult.success(
                            articles,
                            http_status=fallback_result.http_status,
                            url=fallback_result.url or NAVER_NEWS_HTML_URL,
                            content_type=fallback_result.content_type,
                        )
                    return CrawlResult.empty(
                        http_status=fallback_result.http_status,
                        url=fallback_result.url or NAVER_NEWS_HTML_URL,
                        content_type=fallback_result.content_type,
                    )
        elif fallback_result.outcome is CrawlOutcome.EMPTY:
            fallback_result = self._schema_failure(fallback_result, "Naver HTML fallback returned an empty body")

        return self._combine_failures(api_result, fallback_result)

    async def _fetch_mlbpark_posts(self) -> CrawlResult[list[dict[str, Any]]]:
        """Fetch and parse the MLBPark Bullpen listing."""
        result = await self._http_for(MLBPARK_BULLPEN_URL).fetch_text(
            MLBPARK_BULLPEN_URL,
            headers=self.headers,
        )
        self._capture_page("mlbpark_bullpen", result, MLBPARK_BULLPEN_URL)
        if not result.ok:
            if result.outcome is CrawlOutcome.EMPTY:
                return self._schema_failure(result, "MLBPark returned an empty response body")
            return result
        if not isinstance(result.data, str):
            return self._schema_failure(result, "MLBPark returned non-text data")
        try:
            posts = self._parse_mlbpark_posts(BeautifulSoup(result.data, "html.parser"))
        except (TypeError, ValueError) as exc:
            return self._schema_failure(result, f"MLBPark HTML parse failed: {exc}")
        if not posts:
            return CrawlResult.empty(
                http_status=result.http_status,
                url=result.url or MLBPARK_BULLPEN_URL,
                content_type=result.content_type,
            )
        return CrawlResult.success(
            posts,
            http_status=result.http_status,
            url=result.url or MLBPARK_BULLPEN_URL,
            content_type=result.content_type,
        )

    @staticmethod
    def _parse_naver_api_result(result: CrawlResult[str]) -> CrawlResult[list[dict[str, Any]]]:
        """Decode the Naver API shape and distinguish schema drift from empty data."""
        if not isinstance(result.data, str):
            return RealtimeIssueCrawler._schema_failure(result, "Naver API returned non-text data")
        try:
            payload = json.loads(result.data)
        except (json.JSONDecodeError, TypeError) as exc:
            return RealtimeIssueCrawler._schema_failure(result, f"Naver API JSON decode failed: {exc}")
        if not isinstance(payload, dict) or not isinstance(payload.get("result"), dict):
            return RealtimeIssueCrawler._schema_failure(result, "Naver API response is missing result.newsList")
        news_list = payload["result"].get("newsList")
        if not isinstance(news_list, list) or any(not isinstance(item, dict) for item in news_list):
            return RealtimeIssueCrawler._schema_failure(result, "Naver API result.newsList is not a list of objects")
        articles = RealtimeIssueCrawler._parse_naver_news_api_response(payload)
        if not articles:
            return CrawlResult.empty(
                http_status=result.http_status,
                url=result.url,
                content_type=result.content_type,
            )
        return CrawlResult.success(
            articles,
            http_status=result.http_status,
            url=result.url,
            content_type=result.content_type,
        )

    @staticmethod
    def _schema_failure(result: CrawlResult[Any], message: str) -> CrawlResult[Any]:
        """Create a non-retryable parsing failure while preserving safe metadata."""
        return CrawlResult.failure(
            CrawlOutcome.SCHEMA_CHANGED,
            error=message,
            error_code=FailureCode.PARSE_INVALID_FORMAT.value,
            http_status=result.http_status,
            url=result.url,
            content_type=result.content_type,
        )

    @staticmethod
    def _combine_failures(
        primary: CrawlResult[Any],
        fallback: CrawlResult[Any],
    ) -> CrawlResult[list[dict[str, Any]]]:
        """Keep the primary failure classification and report the fallback detail."""
        error = f"{primary.error or primary.outcome}; HTML fallback failed: {fallback.error or fallback.outcome}"
        return CrawlResult.failure(
            primary.outcome,
            error=error,
            error_code=primary.error_code or FailureCode.UNKNOWN.value,
            http_status=primary.http_status,
            url=primary.url,
            content_type=primary.content_type,
        )

    def _capture_page(self, source_key: str, result: CrawlResult[Any], url: str) -> None:
        """Keep the response body and safe response metadata for optional archival."""
        self._raw_pages.append(
            {
                "source_key": source_key,
                "url": result.url or url,
                "html": result.data if isinstance(result.data, str) else "",
                "status_code": result.http_status,
                "content_type": result.content_type,
            },
        )

    @staticmethod
    def _naver_news_api_url() -> str:
        """Build the current-day Naver Sports news API URL."""
        date_str = datetime.now(KST).strftime("%Y%m%d")
        return (
            "https://api-gw.sports.naver.com/news/articles/kbaseball?"
            f"sort=latest&date={date_str}&page=1&pageSize=20&isPhoto=N"
        )

    @staticmethod
    def _parse_naver_news_api_response(data: dict[str, Any]) -> list[dict[str, Any]]:
        """Normalize the Naver API news list into retrieval documents."""
        result_data = data.get("result", {})
        news_list = result_data.get("newsList", []) if isinstance(result_data, dict) else []
        return [RealtimeIssueCrawler._build_naver_api_article(item) for item in news_list]

    @staticmethod
    def _build_naver_api_article(item: dict[str, Any]) -> dict[str, Any]:
        """Build the common news document for one Naver API item."""
        title = item.get("title", "")
        sub_content = item.get("subContent", "")
        oid = item.get("oid", "")
        offset_id = item.get("aid", "")
        url = f"https://sports.news.naver.com/kbaseball/news/read?oid={oid}&aid={offset_id}"
        return {
            "title": title,
            "content": sub_content or title,
            "meta": {
                "source": url,
                "office_name": item.get("officeName", ""),
                "published_at": item.get("datetime", ""),
                "crawled_at": datetime.now(KST).isoformat(),
                "category": "naver_news",
            },
        }

    @staticmethod
    def _parse_naver_news_html(soup: BeautifulSoup) -> list[dict[str, Any]]:
        """Extract distinct article links from the Naver HTML fallback."""
        links = []
        for anchor in soup.find_all("a"):
            href = str(anchor.get("href", ""))
            title = anchor.get("title") or anchor.text.strip()
            if href and ("read" in href or "read.nhn" in href) and title:
                if href.startswith("/"):
                    href = "https://sports.news.naver.com" + href
                links.append((title, href))

        articles = []
        seen = set()
        for title, href in links:
            if href in seen:
                continue
            seen.add(href)
            articles.append(
                {
                    "title": title,
                    "content": title,
                    "meta": {
                        "source": href,
                        "crawled_at": datetime.now(KST).isoformat(),
                        "category": "naver_news",
                    },
                },
            )
        return articles

    @staticmethod
    def _parse_mlbpark_posts(soup: BeautifulSoup) -> list[dict[str, Any]]:
        """Extract and deduplicate topic links from the MLBPark Bullpen page."""
        posts = []
        seen_urls = set()
        for anchor in soup.find_all("a"):
            href = str(anchor.get("href", ""))
            title = str(anchor.text.strip())
            if "id=" not in href or "b=bullpen" not in href or "m=view" not in href:
                continue
            if "pos=reply" in href or not title or (title.startswith("[") and title.endswith("]")):
                continue
            title = re.sub(r"\s*\[\d+\]$", "", title)
            if href.startswith("/"):
                href = "https://mlbpark.donga.com" + href
            if href in seen_urls:
                continue
            seen_urls.add(href)
            posts.append(
                {
                    "title": title,
                    "content": f"MLBPark Bullpen popular discussion thread: {title}",
                    "meta": {
                        "source": href,
                        "crawled_at": datetime.now(KST).isoformat(),
                        "category": "mlbpark",
                    },
                },
            )
        return posts
