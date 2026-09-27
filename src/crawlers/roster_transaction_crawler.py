"""Crawler for daily roster transactions (call-up / send-down).

Sources:
  - KBO mobile registration page: https://m.koreabaseball.com/Kbo/PlayerAdd.aspx
  - KBO player register page: https://www.koreabaseball.com/Player/Register.aspx.

"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.crawlers.base import BasePlaywrightCrawler
from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.http_client import CrawlerHttpClient, HttpPolicy
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.db.engine import SessionLocal
from src.repositories.roster_transaction_repository import RosterTransactionRepository
from src.repositories.source_registry_repository import save_raw_snapshots
from src.urls import REGISTER
from src.utils.compliance import compliance, log_source_limited
from src.utils.http_client import DEFAULT_HEADERS as HEADERS
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.playwright_retry import NAV_TIMEOUT, SHORT_TIMEOUT
from src.utils.request_policy import RequestPolicy

if TYPE_CHECKING:
    from src.utils.playwright_pool import AsyncPlaywrightPool
    from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)

ROSTER_CRAWLER_NAME = "roster_transactions"
ROSTER_TARGET_TYPE = "roster_date"

ROSTER_CRAWL_EXCEPTIONS = (
    PlaywrightError,
    PlaywrightTimeoutError,
    TimeoutError,
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
)
ROSTER_SAVE_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)

TEAM_CODES = [
    ("LG", "LG"),
    ("HH", "HH"),
    ("SS", "SS"),
    ("KT", "KT"),
    ("OB", "OB"),
    ("LT", "LT"),
    ("HT", "HT"),
    ("NC", "NC"),
    ("SK", "SK"),
    ("WO", "WO"),
]


@dataclass(frozen=True)
class _MobileParse:
    """What the mobile page proved, beyond the rows it yielded.

    The rows alone cannot distinguish "no transactions today" from "the page
    changed": both come back as an empty list. `structure_seen` records that a
    section this parser expects was actually present, and the team counts show
    whether a block was found but could not be read. `EMPTY` is only claimed
    when the structure was positively confirmed.
    """

    transactions: list[dict[str, Any]] = field(default_factory=list)
    structure_seen: bool = False
    team_blocks: int = 0
    mapped_teams: int = 0
    unknown_teams: list[str] = field(default_factory=list)


class RosterTransactionCrawler(BasePlaywrightCrawler):
    """RosterTransactionCrawler class."""

    def __init__(
        self,
        request_delay: float = 1.0,
        pool: AsyncPlaywrightPool | None = None,
        policy: RequestPolicy | None = None,
        http_client: CrawlerHttpClient | None = None,
    ) -> None:
        """Initialize RosterTransactionCrawler.

        Args:
            request_delay: Request Delay.
            pool: Connection pool for async operations.
            policy: Optional request policy.
            http_client: HTTP transport. Defaults to a client that owns
                throttling, retry, and the circuit breaker. Tests inject a client
                backed by a mock transport.

        """
        super().__init__(request_delay=request_delay, pool=pool, policy=policy)
        self.mobile_url = "https://m.koreabaseball.com/Kbo/PlayerAdd.aspx"
        self.register_url = REGISTER
        self._raw_pages: list[dict] = []
        self._http = http_client or CrawlerHttpClient(
            name=ROSTER_CRAWLER_NAME,
            headers=dict(HEADERS),
            policy=HttpPolicy(timeout_seconds=15.0),
        )

    async def run(self, *, save: bool = False, target_date: str | None = None) -> list[dict[str, Any]]:
        """Run run.

        Args:
            save: Whether to persist the results.
            target_date: Target date for the operation.
            save: Whether to persist the results.
            target_date: Target date for the operation.

        Returns:
            List of results.

        """
        crawl_date = (
            datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=KST).date()
            if target_date
            else datetime.now(KST).date()
        )

        if not await compliance.is_allowed(self.mobile_url):
            log_source_limited("roster_transaction", self.mobile_url)
            return []

        result = await self._resolve_crawl(crawl_date)
        data = result.data or []
        if not result.ok and result.outcome is not CrawlOutcome.EMPTY:
            logger.error("[ROSTER] %s unresolved: %s", crawl_date, result.error)

        logger.info("[ROSTER] %s: %s transactions found", crawl_date, len(data))
        if save:
            await asyncio.to_thread(self._save_to_db, data)
        else:
            for d in data[:10]:
                logger.info(d)

        return data

    async def _resolve_crawl(self, crawl_date: date) -> CrawlResult[list[dict[str, Any]]]:
        """Run the mobile source, falling back to desktop only when it cannot answer.

        The fallback is for a source that could not be read, never for a source
        that legitimately has nothing. A confirmed quiet day resolves to `EMPTY`
        without launching a browser.

        The mobile page is the primary attempt, so its code is the canonical
        cause when both fail: a replay starts from the primary again. The
        fallback's own code is only promoted when the primary had nothing
        meaningful to say, and its detail is always preserved in the message.

        Args:
            crawl_date: Date to crawl.

        Returns:
            The resolved result for the date.

        """
        primary = await self._crawl_mobile_page(crawl_date)
        if primary.ok or primary.outcome is CrawlOutcome.EMPTY:
            return primary

        logger.warning(
            "[ROSTER] %s mobile unusable (%s: %s), trying desktop fallback",
            crawl_date,
            primary.error_code or primary.outcome,
            primary.error,
        )
        fallback = await self._crawl_desktop_page(crawl_date)
        if fallback.ok or fallback.outcome is CrawlOutcome.EMPTY:
            # The desktop page produced the data, so the date is answered. The
            # primary's failure is not the run's outcome and earns no dead letter.
            return fallback

        return CrawlResult.failure(
            CrawlOutcome.PERMANENT_ERROR,
            error=(
                f"mobile[{primary.error_code or primary.outcome}]: {primary.error}; "
                f"desktop[{fallback.error_code or fallback.outcome}]: {fallback.error}"
            ),
            error_code=self._canonical_failure_code(primary, fallback),
        )

    @staticmethod
    def _canonical_failure_code(
        primary: CrawlResult[Any],
        fallback: CrawlResult[Any],
    ) -> str:
        """Pick the code that describes why both sources failed.

        A meaningful primary classification wins, since a replay retries the
        primary first. `UNKNOWN` and a missing code are placeholders rather than
        findings, so a specific fallback classification is preferred over them.
        """
        meaningful = {
            None,
            FailureCode.UNKNOWN.value,
        }
        if primary.error_code not in meaningful:
            return primary.error_code or FailureCode.UNKNOWN.value
        return fallback.error_code or primary.error_code or FailureCode.UNKNOWN.value

    async def _crawl_mobile_page(self, target_date: date) -> CrawlResult[list[dict[str, Any]]]:
        """Fetch and parse the mobile roster page for one date.

        Distinguishes four outcomes that used to collapse into an empty list:

        * rows found -> ``SUCCESS``
        * expected structure confirmed but no rows -> ``EMPTY`` (a real quiet day)
        * a block was found but no team could be resolved -> ``PARSE_SELECTOR_MISSING``
        * no expected structure at all -> ``PARSE_SELECTOR_MISSING``

        Only the first two are "the source answered". The rest mean the page can no
        longer be read and the caller should try the desktop fallback instead of
        storing zero rows.

        Args:
            target_date: Date to crawl.

        Returns:
            A classified result carrying the transactions.

        """
        url = f"{self.mobile_url}?searchDate={target_date.strftime('%Y-%m-%d')}"
        result = await self._http.fetch_text(url)
        if not result.ok:
            return CrawlResult[Any](
                outcome=result.outcome,
                data=None,
                http_status=result.http_status,
                attempts=result.attempts,
                elapsed_seconds=result.elapsed_seconds,
                retry_after=result.retry_after,
                error=result.error,
                error_code=result.error_code,
                url=url,
            )

        html = result.data
        self._raw_pages.append(
            {
                "source_key": "kbo_today_roster",
                "url": url,
                "html": html,
                "status_code": result.http_status,
            },
        )
        analysis = self._analyze_mobile_html(html, target_date)
        if analysis.transactions:
            return CrawlResult.success(analysis.transactions, http_status=result.http_status)

        if analysis.mapped_teams == 0 and (analysis.team_blocks > 0 or not analysis.structure_seen):
            reason = (
                f"no expected section and no resolvable team block "
                f"(teams seen={analysis.team_blocks}, unknown={analysis.unknown_teams or 'n/a'})"
            )
            logger.error("[ROSTER] %s mobile page unreadable for %s: %s", self.mobile_url, target_date, reason)
            return CrawlResult.failure(
                CrawlOutcome.SCHEMA_CHANGED,
                error=reason,
                error_code=FailureCode.PARSE_SELECTOR_MISSING.value,
                http_status=result.http_status,
                url=url,
            )

        if analysis.unknown_teams:
            logger.warning(
                "[ROSTER] %s unmapped team names for %s: %s",
                self.mobile_url,
                target_date,
                ", ".join(analysis.unknown_teams),
            )
        logger.info("[ROSTER] %s: valid page, 0 transactions for %s", self.mobile_url, target_date)
        return CrawlResult.empty(http_status=result.http_status)

    def _parse_mobile_html(self, html: str, target_date: date) -> list[dict[str, Any]]:
        """Parse the mobile KBO registration page.

        Args:
            html: Html.
            target_date: Target date for the operation.

        Returns:
            The transactions found, which may be empty for a valid quiet page.

        """
        return self._analyze_mobile_html(html, target_date).transactions

    def _analyze_mobile_html(self, html: str, target_date: date) -> _MobileParse:
        """Parse the mobile page and report what the page proved.

        Args:
            html: Mobile page HTML.
            target_date: Target date for the operation.

        Returns:
            The rows plus the structural evidence used to classify an empty page.

        """
        # Split into registered and deregistered sections
        registered_section = ""
        deregistered_section = ""

        # Find sections: "오늘자 선수 등록현황" and "오늘자 선수 말소현황"
        reg_match = re.search(r"오늘자\s*선수\s*등록현황.*?(?=오늘자\s*선수\s*말소현황|\Z)", html, re.DOTALL)
        if reg_match:
            registered_section = reg_match.group(0)

        dereg_match = re.search(r"오늘자\s*선수\s*말소현황.*?(?=<div\s+(?:class|id)=|$)", html, re.DOTALL)
        if dereg_match:
            deregistered_section = dereg_match.group(0)

        if not registered_section and not deregistered_section:
            # No primary layout. The alternate parser is legacy compatibility, and
            # without a marker of its own it cannot confirm a quiet day.
            return self._analyze_alternate_mobile(html, target_date)

        return self._analyze_sections(
            (registered_section, deregistered_section),
            target_date,
            structure_seen=True,
        )

    def _analyze_sections(
        self,
        sections: tuple[str, str],
        target_date: date,
        *,
        structure_seen: bool,
    ) -> _MobileParse:
        """Extract transactions from the registered/deregistered sections."""
        transactions: list[dict[str, Any]] = []
        registered_section, deregistered_section = sections
        team_blocks = 0
        mapped_teams = 0
        unknown_teams: list[str] = []

        for section_text, action in [(registered_section, "registered"), (deregistered_section, "deregistered")]:
            if not section_text:
                continue

            # Find team blocks within section
            found_blocks = re.findall(
                r'<strong[^>]*class="team"[^>]*>([^<]+)</strong>\s*<ul[^>]*>(.*?)</ul>',
                section_text,
                re.DOTALL,
            )
            team_blocks += len(found_blocks)
            for team_name_raw, list_html in found_blocks:
                raw_team = team_name_raw.strip()
                team_code = self._map_team_name(raw_team)
                if not team_code:
                    unknown_teams.append(raw_team)
                    continue
                mapped_teams += 1

                # Extract player names and IDs from list items
                player_items = re.findall(
                    r'<li[^>]*>(?:\s*<a[^>]*href="[^"]*playerId=(\d+)[^"]*"[^>]*>)?\s*([^<]+?)\s*(?:</a>)?\s*</li>',
                    list_html,
                )
                for player_id_str, raw_name in player_items:
                    player_name = raw_name.strip()
                    if not player_name or player_name == "":
                        continue
                    transactions.append(
                        {
                            "transaction_date": target_date,
                            "team_id": team_code,
                            "player_id": int(player_id_str) if player_id_str and player_id_str.isdigit() else None,
                            "player_name": player_name,
                            "action": action,
                            "roster_level": "first_team",
                            "inferred_to_level": "second_team" if action == "deregistered" else None,
                            "source_type": "kbo_today_page",
                            "confidence": "high",
                            "dedupe_key": f"{target_date}_{team_code}_{player_name}_{action}",
                        },
                    )

        return _MobileParse(
            transactions=transactions,
            structure_seen=structure_seen,
            team_blocks=team_blocks,
            mapped_teams=mapped_teams,
            unknown_teams=unknown_teams,
        )

    def _parse_alternate_mobile(self, html: str, target_date: date) -> list[dict[str, Any]]:
        """Fallback parser for alternate mobile page layout.

        Args:
            html: Html.
            target_date: Target date for the operation.

        Returns:
            The transactions found, which may be empty for a valid quiet page.

        """
        return self._analyze_alternate_mobile(html, target_date).transactions

    def _analyze_alternate_mobile(self, html: str, target_date: date) -> _MobileParse:
        """Parse the table-based alternate layout and report what it proved."""
        transactions: list[dict[str, Any]] = []

        # Look for table-based layout
        current_team = None
        current_action = None
        team_blocks = 0
        mapped_teams = 0
        unknown_teams: list[str] = []

        for raw_line in html.split("\n"):
            line = raw_line.strip()
            team_match = re.search(r'class="team"[^>]*>\s*([^<]+)', line)
            if team_match:
                team_blocks += 1
                raw_team = team_match.group(1).strip()
                current_team = self._map_team_name(raw_team)
                if not current_team:
                    unknown_teams.append(raw_team)
                else:
                    mapped_teams += 1
                continue

            if "등록" in line and ("선수" in line or "현황" in line):
                current_action = "registered"
                continue
            if "말소" in line and ("선수" in line or "현황" in line):
                current_action = "deregistered"
                continue

            if current_team and current_action:
                player_match = re.search(r"playerId=(\d+)[^>]*>\s*([^<]+)", line)
                if player_match:
                    pid, pname = int(player_match.group(1)), player_match.group(2).strip()
                    transactions.append(
                        {
                            "transaction_date": target_date,
                            "team_id": current_team,
                            "player_id": pid,
                            "player_name": pname,
                            "action": current_action,
                            "roster_level": "first_team",
                            "inferred_to_level": "second_team" if current_action == "deregistered" else None,
                            "source_type": "kbo_today_page",
                            "confidence": "high",
                            "dedupe_key": f"{target_date}_{current_team}_{pname}_{current_action}",
                        },
                    )

        # The alternate layout has no section header to confirm a quiet day, so
        # `structure_seen` stays false: a zero-row result here is unconfirmed and
        # must not be reported as a legitimate empty.
        return _MobileParse(
            transactions=transactions,
            structure_seen=False,
            team_blocks=team_blocks,
            mapped_teams=mapped_teams,
            unknown_teams=unknown_teams,
        )

    async def _crawl_desktop_page(self, target_date: date) -> CrawlResult[list[dict[str, Any]]]:
        """Fallback: crawl the desktop ASP.NET page.

        Per-team failures are tolerated, because one team's widget failing does
        not invalidate the other nine. Every team failing does, so the count is
        tracked rather than inferred from an empty result.

        Args:
            target_date: Target date for the operation.

        Returns:
            A classified result carrying the transactions.

        """
        transactions: list[dict[str, Any]] = []

        if not await compliance.is_allowed(self.register_url):
            log_source_limited("roster_transaction", self.register_url)
            # Unlike the mobile-only case, the run has already lost its primary
            # source, so a blocked fallback leaves the date with no data at all.
            return CrawlResult.failure(
                CrawlOutcome.PERMANENT_ERROR,
                error=f"desktop fallback blocked by compliance policy: {self.register_url}",
                error_code=FailureCode.FETCH_BLOCKED.value,
            )

        failed_teams = 0
        try:
            async with self.page_context() as page:
                await self.goto_with_retry(page, self.register_url, timeout=NAV_TIMEOUT)

                date_str = target_date.strftime("%Y%m%d")
                date_input = "cphContents_cphContents_cphContents_hfSearchDate"
                await page.evaluate(
                    f"document.getElementById('{date_input}').value = '{date_str}';",
                )
                calendar_button = "ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$btnCalendarSelect"
                try:
                    async with page.expect_response(lambda r: "Register.aspx" in r.url, timeout=SHORT_TIMEOUT):
                        await page.evaluate(f"__doPostBack('{calendar_button}', '')")
                except TimeoutError:
                    logger.warning("Calendar select postback timeout, continuing")
                await page.wait_for_timeout(2000)

                desktop_html = await page.content()
                self._raw_pages.append(
                    {
                        "source_key": "kbo_player_register",
                        "url": self.register_url,
                        "html": desktop_html,
                        "status_code": 200,
                    },
                )

                for site_code, db_code in TEAM_CODES:
                    try:
                        await page.evaluate(f"fnSearchChange('{site_code}')")
                        await page.wait_for_timeout(500)
                        daily = await self._extract_desktop_roster(page, db_code, target_date)
                        transactions.extend(daily)
                    except ROSTER_CRAWL_EXCEPTIONS:
                        failed_teams += 1
                        logger.exception("Desktop roster team %s failed", site_code)
        except ROSTER_CRAWL_EXCEPTIONS as exc:
            return CrawlResult.failure(
                CrawlOutcome.PERMANENT_ERROR,
                error=f"desktop fallback failed: {type(exc).__name__}: {exc}",
                error_code=FailureCode.FETCH_HTTP_ERROR.value,
            )

        if failed_teams == len(TEAM_CODES):
            return CrawlResult.failure(
                CrawlOutcome.PERMANENT_ERROR,
                error=f"desktop fallback failed for all {failed_teams} teams",
                error_code=FailureCode.PARSE_SELECTOR_MISSING.value,
            )
        if transactions:
            return CrawlResult.success(transactions)
        return CrawlResult.empty()

    async def _extract_desktop_roster(self, page: Page, team_code: str, roster_date: date) -> list[dict[str, Any]]:
        script = """
        () => {
            const results = [];
            const tables = document.querySelectorAll('#cphContents_cphContents_cphContents_udpRecord table.tNData');
            if (tables.length === 0) return [];
            tables.forEach(table => {
                const rows = table.querySelectorAll('tbody tr');
                rows.forEach(tr => {
                    const cells = tr.querySelectorAll('td');
                    if (cells.length < 4) return;
                    const nameLink = cells[1].querySelector('a');
                    const name = cells[1].innerText.trim();
                    if (!nameLink || !name) return;
                    const href = nameLink.getAttribute('href');
                    const url = new URL(href, window.location.origin);
                    const playerId = url.searchParams.get('playerId');
                    if (playerId) {
                        results.push({ player_id: playerId, player_name: name });
                    }
                });
            });
            return results;
        }
        """
        data = await page.evaluate(script)

        return [
            {
                "transaction_date": roster_date,
                "team_id": team_code,
                "player_id": int(item["player_id"]),
                "player_name": item["player_name"],
                "action": "registered",
                "roster_level": "first_team",
                "source_type": "kbo_today_page",
                "confidence": "high",
                "dedupe_key": f"{roster_date}_{team_code}_{item['player_name']}_registered",
            }
            for item in data
        ]

    def _map_team_name(self, name: str) -> str | None:
        mapping = {
            "LG": "LG",
            "lg": "LG",
            "엘지": "LG",
            "HH": "HH",
            "한화": "HH",
            "SS": "SS",
            "삼성": "SS",
            "KT": "KT",
            "kt": "KT",
            "OB": "OB",
            "두산": "OB",
            "LT": "LT",
            "롯데": "LT",
            "HT": "HT",
            "KIA": "HT",
            "기아": "HT",
            "NC": "NC",
            "SK": "SK",
            "SSG": "SK",
            "WO": "WO",
            "키움": "WO",
        }
        return mapping.get(name)

    def _save_to_db(self, data: list[dict]) -> None:
        with SessionLocal() as session:
            try:
                saved_snaps = save_raw_snapshots(session, self._raw_pages)
                repo = RosterTransactionRepository(session)
                count = 0
                for item in self._dedupe_transactions(data):
                    try:
                        repo.save(item)
                        count += 1
                    except ROSTER_SAVE_EXCEPTIONS:
                        logger.exception("Roster transaction save failed: %s", item.get("dedupe_key", ""))
                session.commit()
                logger.info("[ROSTER] Saved %s transaction records, %s snapshots.", count, saved_snaps)
            except ROSTER_SAVE_EXCEPTIONS:
                session.rollback()
                logger.exception("Roster batch save error")
            finally:
                self._raw_pages.clear()

    def _dedupe_transactions(self, data: list[dict]) -> list[dict]:
        seen = set()
        deduped = []
        for item in data:
            key = item.get("dedupe_key")
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            deduped.append(item)
        return deduped


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    parser.add_argument("--date", type=str, default=None, help="Target date (YYYY-MM-DD)")
    args = parser.parse_args()
    asyncio.run(RosterTransactionCrawler().run(save=args.save, target_date=args.date))
