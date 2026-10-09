"""Crawler for KBO official event/promotion links."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from sqlalchemy.exc import SQLAlchemyError

from src.crawlers import kbo_event_outcome
from src.crawlers.failure_taxonomy import classify_failure, classify_persist_failure, stage_for_code
from src.crawlers.kbo_event_outcome import (
    KboEventPageRead,
    KboEventStatus,
    classify_page_failure,
    fetch_failed_read,
)
from src.db.engine import SessionLocal
from src.models.crawl_execution import RUN_STATUS_FAILED, RUN_STATUS_PARTIAL
from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
from src.repositories.crawl_execution_repository import CrawlRunSpec
from src.repositories.source_registry_repository import DataSourceRepository, save_raw_snapshots
from src.repositories.team_event_repository import TeamEventRepository
from src.services.crawl_dead_letter_service import enqueue_failure
from src.services.crawl_run_service import track_crawl_run
from src.utils.compliance import compliance, log_source_limited
from src.utils.playwright_pool import AsyncPlaywrightPool

if TYPE_CHECKING:
    from src.models.crawl_execution import CrawlExecutionRun

logger = logging.getLogger(__name__)

KBO_EVENT_SOURCE_KEY = "kbo_official_events"
KBO_EVENT_CRAWLER_NAME = "kbo_event"
KBO_EVENT_TARGET_TYPE = "kbo_event"
KBO_EVENT_BASE_URL = "https://www.koreabaseball.com"
KBO_EVENT_DEFAULT_URLS = (
    "https://www.koreabaseball.com/Kbo/BusinessAndEvent/Mvp.aspx",
    "https://www.koreabaseball.com/Kbo/BusinessAndEvent/Draft.aspx",
    "https://www.koreabaseball.com/Kbo/BusinessAndEvent/MediaDay.aspx",
    "https://www.koreabaseball.com/Kbo/BusinessAndEvent/RecordClass/LessonInfo.aspx",
    "https://www.koreabaseball.com/Kbo/BusinessAndEvent/SafeGuide.aspx",
    "https://www.koreabaseball.com/Kbo/BusinessAndEvent/KboArchive/PurchaseGuide.aspx",
    "https://www.koreabaseball.com/Kbo/BusinessAndEvent/OnSiteViewingSupport.aspx",
)
KBO_EVENT_KEYWORDS = ("이벤트", "event", "프로모션", "행사")
#: The site chrome every page in the BusinessAndEvent section carries. Verified
#: present on the standing pages (`SafeGuide`, `PurchaseGuide`) as well as the
#: announcement ones, so its absence means the response is no longer the document
#: the sweep asked for -- not that the site had nothing to announce.
KBO_EVENT_FRAME_SELECTORS = ("header", "nav", "footer")
KBO_EVENT_CRAWL_EXCEPTIONS = (PlaywrightError, PlaywrightTimeoutError, RuntimeError, ValueError, TypeError, OSError)
KBO_EVENT_SAVE_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)
GENERIC_PAGE_TITLES = {"메인", "신청하기", "신청확인"}
#: The reason keys understood by ``classify_page_failure``. Spelled here so a
#: typo in either module fails at import rather than as an UNKNOWN code in
#: the ledger.
# Re-exported from the outcome vocabulary so there is one definition of each key.
FRAME_MISSING_REASON = kbo_event_outcome.FRAME_MISSING_REASON
FETCH_FAILED_REASON = kbo_event_outcome.FETCH_FAILED_REASON
GENERIC_LINK_TITLES = {"신청하기", "신청 확인", "신청확인", "행사 개요"}


def extract_kbo_event_links(html: str, base_url: str = KBO_EVENT_BASE_URL) -> list[dict[str, object]]:
    """Extract likely KBO official event/promotion links from a page.

    Args:
        html: Html.
        base_url: Base URL.

    """
    soup = BeautifulSoup(html, "html.parser")

    events: list[dict[str, object]] = []
    seen_urls: set[str] = set()
    for link in soup.select("a[href]"):
        title = str(link.get_text(" ", strip=True))
        href = str(link.get("href") or "")
        normalized_href = href.strip().lower()
        if normalized_href.startswith(("#", "javascript:")) or title in GENERIC_LINK_TITLES:
            continue
        haystack = f"{title} {href}".lower()
        if not title or not any(keyword.lower() in haystack for keyword in KBO_EVENT_KEYWORDS):
            continue
        source_url = urljoin(base_url, href)
        if source_url in seen_urls:
            continue
        seen_urls.add(source_url)
        events.append(
            {
                "event_scope": "kbo",
                "team_id": None,
                "title": title[:300],
                "description": None,
                "event_type": "promotion",
                "source_url": source_url,
                "published_at": None,
                "last_seen_at": datetime.now(UTC).replace(tzinfo=None),
                "status": "unknown",
            },
        )
    return events


def has_kbo_event_frame(html: str) -> bool:
    """Return whether the document still carries the site's own frame.

    Every page in the BusinessAndEvent section shares one header, one
    navigation and one footer. When they are gone the response is a maintenance
    page, an error page, or a redesign -- whatever else it contains, it is no
    longer the document the sweep asked for. That has to be decided from the
    frame rather than from the candidate count: most of these seven pages are
    standing guides with nothing to announce, so "no candidates" is the correct
    reading of most of a healthy sweep and cannot carry the verdict alone.
    """
    soup = BeautifulSoup(html, "html.parser")
    return any(soup.select(selector) for selector in KBO_EVENT_FRAME_SELECTORS)


def read_kbo_event_page(html: str, base_url: str = KBO_EVENT_BASE_URL) -> KboEventPageRead:
    """Read one official-events page and say what it meant.

    Three outcomes the sweep used to collapse into an empty list:

    * the frame is there and candidates were found -> ``SUCCESS``
    * the frame is there and none were -> ``EMPTY`` (a standing guide page)
    * the frame is gone -> ``SCHEMA_CHANGED`` (not the page we asked for)

    The first two are "the source answered". The third is a document we can no
    longer read, and reporting it as empty would tell an operator the site has
    no events when it was never actually asked.
    """
    if not has_kbo_event_frame(html):
        return KboEventPageRead(status=KboEventStatus.SCHEMA_CHANGED, reason=FRAME_MISSING_REASON, url=base_url)

    events = list(extract_kbo_event_links(html, base_url))
    page_event = extract_kbo_event_page(html, base_url)
    if page_event is not None:
        events.insert(0, page_event)
    return KboEventPageRead(
        status=KboEventStatus.SUCCESS if events else KboEventStatus.EMPTY,
        events=events,
        url=base_url,
    )


def extract_kbo_event_page(html: str, source_url: str) -> dict[str, object] | None:
    """Build one event payload for an official KBO business/event page.

    Args:
        html: Html.
        source_url: Source URL.

    """
    soup = BeautifulSoup(html, "html.parser")

    title = _extract_page_title(soup)
    if not title:
        return None
    return _build_event_payload(title, source_url)


def _extract_page_title(soup: BeautifulSoup) -> str | None:
    title_node = soup.select_one("title")
    if not title_node:
        return None
    parts = [part.strip() for part in title_node.get_text(" ", strip=True).split("|") if part.strip()]
    for part in parts:
        if part not in GENERIC_PAGE_TITLES and part not in {"KBO", "주요 사업/행사"}:
            return part[:300]
    return None


def _build_event_payload(title: str, source_url: str) -> dict[str, object]:
    return {
        "event_scope": "kbo",
        "team_id": None,
        "title": title[:300],
        "description": None,
        "event_type": "promotion",
        "source_url": source_url,
        "published_at": None,
        "last_seen_at": datetime.now(UTC).replace(tzinfo=None),
        "status": "unknown",
    }


def _page_key(url: str) -> str:
    """Return a short, stable replay key for one official page.

    ``CrawlDeadLetter.target_id`` is a ``String(128)`` column, so a full URL is not
    a safe key; the last path segment identifies the page just as well.
    """
    path = url.split("?", 1)[0].rstrip("/")
    slug = path.rsplit("/", 1)[-1]
    return (slug or path or url)[:128]


class KboEventCrawler:
    """Fetch KBO official page and extract event/promotion link candidates."""

    def __init__(self, base_url: str | None = None) -> None:
        """Initialize a new instance.

        Args:
            base_url: Base URL.

        """
        self.urls = (base_url,) if base_url else KBO_EVENT_DEFAULT_URLS

        self._raw_pages: list[dict[str, object]] = []
        self._last_failure_reason: str | None = None
        #: Pages that failed inside :meth:`run`, kept so the ledger and the dead
        #: letter queue can see them instead of one bad page ending the sweep.
        self._page_failures: list[tuple[str, str, str]] = []
        """Pages that could not be read, as (url, error_code, message).

        A code rather than an exception because a page can fail without raising:
        a document that lost the site frame is exactly as unreadable as a
        connection error, and the run has to be able to say so.
        """

        #: One read per page visited, so the sweep's outcome vocabulary has a
        #: producer for every state it declares. Kept alongside `_page_failures`
        #: rather than instead of it: the failures drive the ledger and the dead
        #: letter queue, while the reads say what the sweep saw (BUG-006).
        self._page_reads: list[KboEventPageRead] = []

    async def run(
        self,
        *,
        save: bool = False,
        run_spec: CrawlRunSpec | None = None,
        record_dead_letters: bool = True,
    ) -> list[dict[str, object]]:
        """Fetch the official event pages and extract promotion candidates.

        The pages are independent, so one failing page no longer aborts the whole
        sweep: it is recorded in the run ledger, reflected as a ``partial`` run,
        and enqueued as its own dead letter (a page is the replay unit). A
        persistence failure still propagates -- that is our own write failing and
        the CLI must not exit successfully -- after being classified and enqueued.

        Args:
            save: Whether to persist the results.
            run_spec: Optional pre-built ledger spec (replay supplies one).
            record_dead_letters: Whether failures enqueue DLQ entries.

        Returns:
            The extracted event candidates.

        """
        spec = run_spec or CrawlRunSpec(
            crawler=KBO_EVENT_CRAWLER_NAME,
            target_type=KBO_EVENT_TARGET_TYPE,
            target_id=KBO_EVENT_SOURCE_KEY,
            source_url=KBO_EVENT_BASE_URL,
        )

        with track_crawl_run(spec) as run:
            if await self._blocked_by_compliance(run):
                return []

            # Reset per run, not per instance: both lists are sweep state, and a
            # crawler object outlives one sweep. Leaving them set would make a
            # second `run()` report the first sweep's failures as its own.
            self._page_failures = []
            self._page_reads = []
            events: list[dict[str, object]] = []
            seen_urls: set[str] = set()
            for url in self.urls:
                await self._collect_page(url, events, seen_urls)
            logger.info("[KBO_EVENT] Found %s official event link candidates.", len(events))
            run.records_read = len(events)

            if save and events:
                await self._persist(run, events, record_dead_letters=record_dead_letters)

            self._record_page_failures(run, record_dead_letters=record_dead_letters)
            return events

    async def _blocked_by_compliance(self, run: CrawlExecutionRun) -> bool:
        """Record a compliance skip on the run and report whether it is blocked."""
        for url in self.urls:
            if await compliance.is_allowed(url):
                continue
            self._last_failure_reason = log_source_limited("kbo_event", url)
            run.checkpoint = {
                "outcome": "source_limited",
                "reason": self._last_failure_reason,
                "source_url": url,
            }
            logger.info("[KBO_EVENT] skipped: blocked by compliance policy (%s)", url)
            return True
        return False

    async def _collect_page(self, url: str, events: list[dict[str, object]], seen_urls: set[str]) -> None:
        """Fetch one page and extend ``events`` with its candidates.

        A page that fails -- by raising, or by answering with something that is
        no longer a KBO page -- is captured for the ledger instead of ending the
        sweep. The second case used to arrive as an empty candidate list and be
        reported as a successful read of a page that had nothing on it.
        """
        try:
            html, final_url = await self._fetch_html(url)
        except KBO_EVENT_CRAWL_EXCEPTIONS as exc:
            logger.exception("[KBO_EVENT] Failed to fetch %s", url)
            _stage, code = classify_failure(exc)
            self._page_failures.append((url, code.value, str(exc)))
            # Recorded so the fetch failure reaches the outcome vocabulary rather
            # than being handled entirely by this except branch. The dead letter
            # above is the recovery path and was always correct; what was missing
            # is that `KboEventStatus.FETCH_FAILED` had no producer at all, so the
            # enum claimed a distinction the sweep could not express (BUG-006).
            self._page_reads.append(fetch_failed_read(url))
            return

        read = read_kbo_event_page(html, final_url)
        self._page_reads.append(read)
        if read.status is KboEventStatus.SCHEMA_CHANGED:
            # Keep the raw document: it is the evidence for what the page turned
            # into, and `kbo snapshot replay` re-parses from it.
            self._raw_pages.append(
                {
                    "source_key": KBO_EVENT_SOURCE_KEY,
                    "url": final_url,
                    "html": html,
                    "status_code": 200,
                },
            )
            code, _terminal = classify_page_failure(read.reason or "")
            logger.warning("[KBO_EVENT] %s is no longer a KBO page: %s", final_url, read.reason)
            self._page_failures.append((final_url, code, "site frame missing"))
            return

        self._raw_pages.append(
            {
                "source_key": KBO_EVENT_SOURCE_KEY,
                "url": final_url,
                "html": html,
                "status_code": 200,
            },
        )
        for event in read.events:
            source_url = str(event["source_url"])
            if source_url in seen_urls:
                continue
            events.append(event)
            seen_urls.add(source_url)
        if read.status is KboEventStatus.EMPTY:
            # Worth one line, because this is the case that must not page anyone
            # and the one that looks identical to a broken page from outside.
            logger.info("[KBO_EVENT] %s carried no event candidates", final_url)

    async def _persist(
        self,
        run: CrawlExecutionRun,
        events: list[dict[str, object]],
        *,
        record_dead_letters: bool,
    ) -> None:
        """Persist events, classifying and enqueueing a write failure before re-raising."""
        try:
            written = await asyncio.to_thread(self._save_to_db, events)
        except KBO_EVENT_SAVE_EXCEPTIONS as exc:
            _, code = classify_persist_failure(exc)
            logger.exception("[KBO_EVENT] persist failed (%s)", code.value)
            run.status = RUN_STATUS_FAILED
            run.error_code = code.value
            run.error_message = str(exc)
            if record_dead_letters:
                self._enqueue_dead_letter(run.run_id, KBO_EVENT_SOURCE_KEY, code.value, str(exc))
            # ``track_crawl_run`` reads ``error_code`` off the propagating
            # exception, so attach the taxonomy before re-raising.
            exc.error_code = code.value
            raise
        run.records_written = written

    def _record_page_failures(self, run: CrawlExecutionRun, *, record_dead_letters: bool) -> None:
        """Reflect unreadable pages in the run status and the dead letter queue.

        A page that lost the site frame is as unreadable as one that timed out,
        and both end up here with a code already attached. The queue is the only
        durable trace either way, so a drift must not be allowed to slip past it
        as a page that happened to carry nothing.
        """
        if not self._page_failures:
            return

        failed_keys = [_page_key(url) for url, _, _ in self._page_failures]
        run.error_message = f"pages failed: {failed_keys}"
        if run.records_read:
            run.status = RUN_STATUS_PARTIAL
        else:
            run.status = RUN_STATUS_FAILED
            run.error_code = self._page_failures[0][1]
        logger.warning("[KBO_EVENT] pages failed: %s", failed_keys)

        if not record_dead_letters:
            return
        for url, code, message in self._page_failures:
            self._enqueue_dead_letter(run.run_id, _page_key(url), code, message, source_url=url)

    def _enqueue_dead_letter(
        self,
        original_run_id: str,
        target_id: str,
        error_code: str,
        error_message: str | None,
        *,
        source_url: str | None = None,
    ) -> None:
        """Enqueue one dead letter for a page or the source that could not be read."""
        try:
            enqueue_failure(
                DeadLetterSpec(
                    original_run_id=original_run_id,
                    crawler=KBO_EVENT_CRAWLER_NAME,
                    target_type=KBO_EVENT_TARGET_TYPE,
                    target_id=target_id,
                    source_url=source_url or KBO_EVENT_BASE_URL,
                    # Derived from the code, never supplied beside it.
                    failure_stage=stage_for_code(error_code).value,
                    error_code=error_code,
                    error_message=error_message,
                ),
            )
        except Exception:
            logger.exception("Failed to enqueue dead letter for kbo_event %s", target_id)

    async def _fetch_html(self, url: str) -> tuple[str, str]:
        pool = AsyncPlaywrightPool(
            max_pages=1,
            context_kwargs={
                "locale": "ko-KR",
                "timezone_id": "Asia/Seoul",
                "viewport": {"width": 1920, "height": 1080},
            },
        )
        await pool.start()
        page = await pool.acquire()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(1500)
            return await page.content(), page.url
        except KBO_EVENT_CRAWL_EXCEPTIONS:
            logger.exception("[KBO_EVENT] Failed to fetch %s", url)
            raise
        finally:
            await pool.release(page)
            await pool.close()

    def _save_to_db(self, events: list[dict[str, object]]) -> int:
        with SessionLocal() as session:
            try:
                saved_snaps = save_raw_snapshots(session, self._raw_pages)
                source = DataSourceRepository(session).get_by_key(KBO_EVENT_SOURCE_KEY)
                repo = TeamEventRepository(session)
                saved_events = 0
                for event in events:
                    payload = dict(event)
                    if source:
                        payload["source_id"] = source.id
                    repo.save(payload)
                    saved_events += 1
                session.commit()
                logger.info("[KBO_EVENT] Saved %s events, %s snapshots.", saved_events, saved_snaps)
            except KBO_EVENT_SAVE_EXCEPTIONS:
                session.rollback()
                logger.exception("[KBO_EVENT] Save failed")
                raise
            else:
                return saved_events
