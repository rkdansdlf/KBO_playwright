"""Crawler for stadium seat section information from team websites."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import TYPE_CHECKING, Any

import httpx
from bs4 import BeautifulSoup
from sqlalchemy.exc import SQLAlchemyError

from src.crawlers.base import BaseHttpCrawler
from src.crawlers.failure_taxonomy import FailureCode, classify_failure, stage_for_code
from src.crawlers.http_client import CrawlerHttpClient, HttpPolicy
from src.crawlers.sweep_outcome import CrawlSweepOutcome
from src.db.engine import SessionLocal
from src.models.crawl_execution import RUN_STATUS_FAILED, RUN_STATUS_PARTIAL
from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
from src.repositories.crawl_execution_repository import CrawlRunSpec
from src.repositories.source_registry_repository import save_raw_snapshots
from src.repositories.stadium_seat_section_repository import StadiumSeatSectionRepository
from src.services.crawl_dead_letter_service import enqueue_failure
from src.services.crawl_run_service import track_crawl_run
from src.utils.http_client import DEFAULT_HEADERS as HEADERS

if TYPE_CHECKING:
    from src.crawlers.result import CrawlResult
    from src.models.crawl_execution import CrawlExecutionRun
    from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)

SEAT_CRAWLER_NAME = "seat"
SEAT_TARGET_TYPE = "seat_section"
SEAT_CRAWL_EXCEPTIONS = (httpx.HTTPError, RuntimeError, ValueError, TypeError, OSError)
SEAT_SAVE_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)
MIN_SEAT_SECTION_NAME_LENGTH = 2

TEAM_SEAT_SOURCES: dict[str, dict[str, Any]] = {
    "LG": {
        "source_key": "lg_twins_seat",
        "stadium_id": "JAMSIL",
        "url": "https://www.lgtwins.com/ticket/general",
    },
    "OB": {
        "source_key": "seoul_stadium_seat",
        "stadium_id": "JAMSIL",
        "url": "https://www.lgtwins.com/ticket/general",
    },
}

SECTION_PATTERNS = [
    re.compile(r"([가-힣]+(?:석|존|Zone|zone))\b"),
    re.compile(r"\b(블루|오렌지|레드|네이비|그린|화이트|골드|블랙|핑크|옐로우|퍼플|스카이|마젠타|브라운|그레이|실버|골드|VIP|테이블|가족|휠체어|응원|내야|외야|중앙|프리미엄|익사이팅|치어|응원석)\s*(석|존)\b"),
]


class SeatCrawler(BaseHttpCrawler):
    """SeatCrawler class."""

    def __init__(
        self,
        request_delay: float = 0.5,
        policy: RequestPolicy | None = None,
        http_client: CrawlerHttpClient | None = None,
    ) -> None:
        """Initialize SeatCrawler.

        Args:
            request_delay: Request delay in seconds.
            policy: Optional request policy.
            http_client: Transport to use. Defaults to a client named for this
                crawler. Tests inject a client backed by a mock transport.

        """
        super().__init__(request_delay=request_delay, policy=policy, default_headers=HEADERS)
        # The shared client already waits on an adaptive rate limiter, retries
        # with backoff, and validates every redirect target. The hand-rolled
        # `httpx.AsyncClient` this replaced carried none of that, so a failing
        # team page lost the request after one attempt.
        self._http = http_client or CrawlerHttpClient(
            name=SEAT_CRAWLER_NAME,
            policy=HttpPolicy(base_delay_seconds=request_delay, timeout_seconds=15.0),
            headers=HEADERS,
        )
        self._raw_pages: list[dict] = []
        # Initialised here as well as in `run()` because a test or a replay may
        # call `_crawl_team_seats` directly, and a failure that raises
        # AttributeError there is a failure that cannot be reported at all.
        self._team_failures: list[tuple[str, str, str]] = []
        #: What the last completed ``run()`` did, for an in-process caller that
        #: has no other way to tell a full sweep from a failed one.
        self._last_outcome: CrawlSweepOutcome | None = None

    @property
    def last_run_outcome(self) -> CrawlSweepOutcome | None:
        """Return what the last completed ``run()`` did, or ``None`` before one."""
        return self._last_outcome

    async def run(
        self,
        *,
        save: bool = False,
        team_filter: str | None = None,
        run_spec: CrawlRunSpec | None = None,
        record_dead_letters: bool = True,
    ) -> list[dict[str, Any]]:
        """Crawl every team's seat sections.

        Args:
            save: Whether to persist the results.
            team_filter: Crawl only this team code.
            run_spec: Ledger entry to write under. A default is built when omitted.
            record_dead_letters: Whether a team failure becomes a dead letter.

        Returns:
            The sections found.

        """
        selected = {code: info for code, info in TEAM_SEAT_SOURCES.items() if not team_filter or code == team_filter}
        spec = run_spec or CrawlRunSpec(
            crawler=SEAT_CRAWLER_NAME,
            target_type=SEAT_TARGET_TYPE,
            target_id=team_filter or "all",
            source_url=next(iter(selected.values()))["url"] if len(selected) == 1 else None,
        )

        # Every team failing used to leave no trace at all: the exception was
        # logged and `[]` came back, which is the same result as a page with no
        # seats on it. The scheduler calls this daily and nothing in the ledger
        # recorded it, so the table simply aged and the reason was unknowable.
        with track_crawl_run(spec) as run:
            self._team_failures: list[tuple[str, str, str]] = []
            all_sections: list[dict[str, Any]] = []
            for team_code, info in selected.items():
                try:
                    sections = await self._crawl_team_seats(team_code, info)
                except SEAT_CRAWL_EXCEPTIONS as exc:
                    logger.exception("Failed to crawl seats for %s", team_code)
                    code = getattr(exc, "error_code", None) or FailureCode.UNKNOWN.value
                    self._team_failures.append((team_code, code, str(exc)))
                    continue
                for entry in sections:
                    entry["team_code"] = team_code
                all_sections.extend(sections)
                logger.info("[SEAT] %s: %s sections found", team_code, len(sections))

            logger.info("[SEAT] Total: %s sections", len(all_sections))
            run.records_read = len(all_sections)
            if save:
                written, failed = await asyncio.to_thread(self._save_to_db, all_sections)
                run.records_written = written
                run.records_failed = failed
            else:
                for section in all_sections[:5]:
                    logger.info(section)

            self._record_team_failures(run, selected, record_dead_letters=record_dead_letters)
            self._last_outcome = CrawlSweepOutcome.from_run(
                run,
                crawler=SEAT_CRAWLER_NAME,
                failed_units=[team for team, _, _ in self._team_failures],
            )
            return all_sections

    def _record_team_failures(
        self,
        run: CrawlExecutionRun,
        selected: dict[str, dict[str, Any]],
        *,
        record_dead_letters: bool,
    ) -> None:
        """Attribute each team failure to the run and to a dead letter.

        Args:
            run: The ledger entry for this run.
            selected: The teams this run covered, for the source URL.
            record_dead_letters: Whether to enqueue.

        """
        if not self._team_failures:
            return
        failed_teams = [team_code for team_code, _, _ in self._team_failures]
        wrote_nothing = run.records_failed and not run.records_written
        run.records_failed += len(self._team_failures)
        run.error_message = (
            "teams failed: "
            + ", ".join(sorted(set(failed_teams)))
            + (" (nothing was written)" if wrote_nothing else "")
        )
        # The status is set here rather than left to the tracker. A run whose teams
        # all failed is not a success, and deriving that from the record counts
        # alone would call every failed sweep a success -- which is what the seat
        # table did for weeks.
        if run.records_read and not wrote_nothing:
            run.status = RUN_STATUS_PARTIAL
        else:
            run.status = RUN_STATUS_FAILED
            run.error_code = self._team_failures[0][1]
        logger.warning("[SEAT] teams failed: %s", failed_teams)
        for team_code, code, message in self._team_failures:
            logger.warning("[SEAT] team %s failed: %s (%s)", team_code, code, message)
            if not record_dead_letters:
                continue
            enqueue_failure(
                DeadLetterSpec(
                    original_run_id=run.run_id,
                    crawler=SEAT_CRAWLER_NAME,
                    target_type=SEAT_TARGET_TYPE,
                    target_id=team_code,
                    # Derived from the code, never supplied beside it: a stage and a
                    # code that disagree are two claims about one failure, and the
                    # retry budget is spent on the stage.
                    failure_stage=stage_for_code(code).value,
                    error_code=code,
                    error_message=message,
                    source_url=selected.get(team_code, {}).get("url"),
                ),
            )

    async def _fetch_seat_page(self, url: str) -> CrawlResult[str]:
        """Fetch one seat page, classifying the outcome.

        Split from :meth:`_crawl_team_seats` so the transport reports a typed
        result rather than an empty list. The two are indistinguishable from the
        outside -- a 404, a timeout and a page with no seat names on it all
        produced `[]` -- and only one of them is a reason to look again.
        """
        return await self._http.fetch_text(url)

    async def _crawl_team_seats(self, team_code: str, info: dict) -> list[dict[str, Any]]:
        sections = []
        try:
            result = await self._fetch_seat_page(info["url"])
            if not result.ok:
                # The taxonomy code is already attached, so a failure carries one
                # name from here into the ledger and the metric rather than being
                # re-derived from a message later.
                # No code came back from the transport. The taxonomy has no
                # generic fetch failure because the transport is expected to name
                # one, so `UNKNOWN` is the honest record rather than an invented
                # code that would read as a specific diagnosis.
                code = result.error_code or FailureCode.UNKNOWN.value
                logger.warning(
                    "Seat page fetch failed for %s: %s (%s)",
                    team_code,
                    code,
                    result.url or info["url"],
                )
                # Recorded, not raised. An empty list was returning from every
                # failure -- a 404, a timeout and a page with no seat names were
                # the same result -- and only one of those is a reason to look
                # again. Raising here would abandon the other nine teams.
                self._team_failures.append((team_code, code, "seat page fetch failed"))
                return []
            html = result.data
            self._raw_pages.append(
                {
                    "team_code": team_code,
                    "source_key": info["source_key"],
                    "url": info["url"],
                    "html": html,
                    "status_code": result.http_status,
                },
            )
            sections = self._parse_seat_page(html, team_code, info)
            if not sections:
                # A stadium that publishes no seat names is not the same answer as
                # one we failed to read, and reporting them alike is how a table
                # ages with nothing in the ledger to explain it.
                self._team_failures.append((team_code, FailureCode.PARSE_EMPTY.value, "no seat section found"))
                logger.warning("[SEAT] %s returned no seat sections", team_code)
        except SEAT_CRAWL_EXCEPTIONS as exc:
            logger.exception("Failed to parse seat page for %s", team_code)
            _, code = classify_failure(exc)
            self._team_failures.append((team_code, code.value, str(exc)))
        return sections

    def _parse_seat_page(self, html: str, _team_code: str, info: dict) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        sections = []
        seen = set()

        for pattern in SECTION_PATTERNS:
            for match in pattern.finditer(text):
                name = match.group(0).strip()
                if name in seen or len(name) < MIN_SEAT_SECTION_NAME_LENGTH:
                    continue
                seen.add(name)
                sections.append(
                    {
                        "stadium_id": info["stadium_id"],
                        "section_name": name,
                        "section_code": None,
                        "seat_grade": name,
                        "source_id": None,
                    },
                )

        return sections

    def _save_to_db(self, data: list[dict]) -> tuple[int, int]:
        """Persist one team's sections in its own transaction.

        A single batch session meant the first constraint violation poisoned it and
        the commit rolled every team's rows back, after which the run still reported
        a count of what it had saved and never a failure. One session per team makes
        the team the unit of work, of attribution, and of the dead letter.

        Args:
            data: Sections to persist, each carrying ``team_code``.

        Returns:
            How many rows were saved, and how many failed.

        """
        by_team: dict[str, list[dict[str, Any]]] = {}
        for item in data:
            by_team.setdefault(str(item.get("team_code") or ""), []).append(item)

        saved = 0
        failed = 0
        # Index raw pages by team once, before the per-team transaction loop.
        # The old code called _raw_pages_by_team() inside the loop, which cleared
        # self._raw_pages on the first iteration and left nothing for subsequent teams.
        raw_by_team = self._raw_pages_by_team()
        for team_code, items in by_team.items():
            with SessionLocal() as session:
                try:
                    repo = StadiumSeatSectionRepository(session)
                    for item in items:
                        repo.save(item)
                    saved_snaps = save_raw_snapshots(session, raw_by_team.pop(team_code, []))
                    session.commit()
                    saved += len(items)
                    logger.info("[SEAT] %s saved %s sections, %s snapshots.", team_code or "-", len(items), saved_snaps)
                except SEAT_SAVE_EXCEPTIONS:
                    session.rollback()
                    failed += len(items)
                    logger.exception("Seat section save failed for %s", team_code or "-")
        self._raw_pages.clear()
        return saved, failed

    def _raw_pages_by_team(self) -> dict[str, list[dict]]:
        """Index the captured pages by team, consuming them as they are saved.

        Returns:
            Source pages keyed by team code.

        """
        pages: dict[str, list[dict]] = {}
        keep: list[dict] = []
        for page in self._raw_pages:
            key = str(page.get("team_code") or "")
            if key:
                pages.setdefault(key, []).append(page)
            else:
                keep.append(page)
        self._raw_pages = keep
        return pages
