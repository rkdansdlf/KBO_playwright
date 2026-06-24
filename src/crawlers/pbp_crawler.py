"""
PBP Crawler - Historical Play-by-play data collection (`LiveTextView2.aspx`).
Navigaes directly to the Live Text View page to collect events.
Computes WPA transitions based on the events.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page

from src.services.wpa_calculator import WPACalculator
from src.utils.compliance import compliance
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.playwright_retry import LONG_TIMEOUT, NAV_TIMEOUT, SEL_TIMEOUT
from src.utils.request_policy import RequestPolicy
from src.utils.text_parser import KBOTextParser

logger = logging.getLogger(__name__)

PBP_CRAWLER_EXCEPTIONS = (
    PlaywrightError,
    TimeoutError,
    asyncio.TimeoutError,
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
)


class PBPCrawler:
    def __init__(
        self,
        request_delay: float = 1.0,
        policy: RequestPolicy | None = None,
        pool: AsyncPlaywrightPool | None = None,
    ) -> None:
        # Using the older but more robust LiveText.aspx which behaves better with Referer checks
        self.base_url = "https://www.koreabaseball.com/Game/LiveText.aspx"
        self.policy = policy or RequestPolicy.with_delay(request_delay, request_delay + 0.5)
        self.pool = pool
        self._context_kwargs = self.policy.build_context_kwargs(locale="ko-KR")
        self.wpa_calc = WPACalculator()
        self.last_failure_reason: str | None = None

    @staticmethod
    def _is_auth_redirect(page: Page) -> bool:
        return "Error.html" in page.url or "Login.aspx" in page.url

    async def _prepare_live_text_page(self, page: Page, game_date: str, url: str) -> bool:
        logger.info("[FETCH] PBP Data: %s", url)
        if not await compliance.is_allowed(url):
            logger.info("[COMPLIANCE] Navigation to %s aborted.", url)
            return False

        await self.policy.delay_async(host="www.koreabaseball.com")
        parent_url = f"https://www.koreabaseball.com/Schedule/ScoreBoard.aspx?gameDate={game_date}"
        logger.info("[AUTH] Warming up session on Scoreboard: %s", parent_url)
        await page.goto(parent_url, wait_until="networkidle", timeout=NAV_TIMEOUT)
        await asyncio.sleep(2)

        logger.info("[FETCH] Navigating to Relay page with Referer: %s", url)
        await page.goto(url, wait_until="domcontentloaded", timeout=LONG_TIMEOUT, referer=parent_url)
        return True

    async def _wait_for_pbp_container(self, page: Page, game_id: str) -> bool:
        try:
            await page.wait_for_selector('div[id^="numCont"]', timeout=SEL_TIMEOUT)
        except (PlaywrightError, TimeoutError):
            logger.warning("No PBP containers found for %s", game_id)
            body = await page.content()
            if "데이터가 없습니다" in body or "취소" in body:
                self.last_failure_reason = "empty"
                return False
        return True

    @staticmethod
    def _initial_legacy_state() -> dict[str, Any]:
        return {
            "current_inning": 0,
            "current_half": "unknown",
            "home_score": 0,
            "away_score": 0,
            "current_outs": 0,
            "current_runners": 0,
        }

    @staticmethod
    def _apply_inning_header(state: dict[str, Any], text: str, cls: str) -> bool:
        if "blue" not in cls or "회" not in text:
            return False
        match = re.search(r"(\d+)회(초|말)", text)
        if match:
            state["current_inning"] = int(match.group(1))
            state["current_half"] = "top" if match.group(2) == "초" else "bottom"
            state["current_outs"] = 0
            state["current_runners"] = 0
        return True

    @staticmethod
    def _is_legacy_event_text(text: str, cls: str) -> bool:
        if "normaiflTxt" not in cls and "red" not in cls:
            return False
        return "경기 준비중" not in text and "경기 시작" not in text

    @staticmethod
    def _update_out_base_state(state: dict[str, Any], text: str) -> tuple[int, int]:
        outs_before = state["current_outs"]
        runners_before = state["current_runners"]
        parsed_outs = KBOTextParser.parse_outs(text)
        parsed_runners = KBOTextParser.parse_runners(text)
        if "사" in text and ("루" in text or "무사" in text):
            if parsed_outs >= 0:
                outs_before = parsed_outs
                state["current_outs"] = outs_before
            if parsed_runners >= 0:
                runners_before = parsed_runners
                state["current_runners"] = runners_before
        if any(keyword in text for keyword in ["삼진", "아웃", "플라이", "땅볼", "범타"]):
            state["current_outs"] += 2 if "병살" in text else 3 if "삼중살" in text else 1
        state["current_outs"] = min(state["current_outs"], 3)
        return outs_before, runners_before

    def _build_legacy_event(
        self, state: dict[str, Any], text: str, sequence: int, outs_before: int, runners_before: int
    ) -> dict[str, Any]:
        is_bottom = state["current_half"] == "bottom"
        score_diff_before = state["home_score"] - state["away_score"]
        runs_scored = KBOTextParser.parse_score_change(text)
        state["home_score" if is_bottom else "away_score"] += runs_scored
        runners_after = 0

        wp_before = self.wpa_calc.get_win_probability(
            state["current_inning"],
            is_bottom=is_bottom,
            outs=outs_before,
            runners=runners_before,
            score_diff=score_diff_before,
        )
        wp_after = self.wpa_calc.get_win_probability(
            state["current_inning"],
            is_bottom=is_bottom,
            outs=state["current_outs"],
            runners=runners_after,
            score_diff=state["home_score"] - state["away_score"],
        )
        wpa = round(wp_after - wp_before if is_bottom else wp_before - wp_after, 4)
        return {
            "event_seq": sequence,
            "inning": state["current_inning"],
            "inning_half": state["current_half"],
            "description": text,
            "event_type": "batting" if "타자" in text or "전환" in text else "unknown",
            "batter": text.split(":")[0].replace("타자", "").strip() if ":" in text else None,
            "result": text.split(":")[1].strip() if ":" in text else None,
            "wpa": wpa,
            "win_expectancy_before": wp_before,
            "win_expectancy_after": wp_after,
            "home_score": state["home_score"],
            "away_score": state["away_score"],
            "score_diff": score_diff_before,
            "base_state": runners_before,
            "outs": outs_before,
            "bases_before": self._format_base_string(runners_before),
            "bases_after": self._format_base_string(runners_after),
        }

    async def crawl_game_events(self, game_id: str) -> dict[str, Any] | None:
        """
        Loads the LiveText page for a specific game and extracts PBP data.
        """
        self.last_failure_reason = None
        game_date = game_id[:8]
        # Common ids: leagueId=1 (KBO), seriesId=0 (Regular)
        url = f"{self.base_url}?leagueId=1&seriesId=0&gameId={game_id}&gyear={game_date[:4]}"

        pool = self.pool or AsyncPlaywrightPool(max_pages=1, context_kwargs=self._context_kwargs, requires_auth=True)
        owns_pool = self.pool is None

        if owns_pool:
            await pool.start()

        try:
            return await self._crawl_game_events_with_pool(pool, game_id, game_date, url)
        finally:
            if owns_pool:
                await pool.close()

    async def _crawl_game_events_with_pool(
        self,
        pool: AsyncPlaywrightPool,
        game_id: str,
        game_date: str,
        url: str,
        retry_count: int = 0,
    ) -> dict[str, Any] | None:
        try:
            page = await pool.acquire()
            try:
                return await self._crawl_game_events_page(pool, page, game_id, game_date, url, retry_count)
            finally:
                await pool.release(page)
        except PBP_CRAWLER_EXCEPTIONS:
            logger.exception("Pool error for %s", game_id)
            self.last_failure_reason = "error"
            return None

    async def _crawl_game_events_page(  # noqa: PLR0913
        self,
        pool: AsyncPlaywrightPool,
        page: Page,
        game_id: str,
        game_date: str,
        url: str,
        retry_count: int,
    ) -> dict[str, Any] | None:
        try:
            if not await self._prepare_live_text_page(page, game_date, url):
                return None
            if self._is_auth_redirect(page):
                return await self._retry_after_auth_redirect(pool, page, game_id, game_date, url, retry_count)
            if not await self._wait_for_pbp_container(page, game_id):
                return None
            logger.info("[INFO] Extracting Relay Data...")
            events = await self._extract_flat_events_legacy(page)
        except PBP_CRAWLER_EXCEPTIONS:
            logger.exception("PBP crawl failed for %s", game_id)
            self.last_failure_reason = "error"
            return None
        if not events:
            self.last_failure_reason = "empty"
            return None
        return {"game_id": game_id, "game_date": game_date, "events": events}

    async def _retry_after_auth_redirect(  # noqa: PLR0913
        self,
        pool: AsyncPlaywrightPool,
        page: Page,
        game_id: str,
        game_date: str,
        url: str,
        retry_count: int,
    ) -> dict[str, Any] | None:
        logger.info("[ERROR] Redirected to %s.", page.url)
        self.last_failure_reason = "auth_required"
        if retry_count > 0:
            return None
        await pool.close()
        await pool.start()
        return await self._crawl_game_events_with_pool(pool, game_id, game_date, url, retry_count=1)

    async def _extract_flat_events_legacy(self, page: Page) -> list[dict[str, Any]]:
        """Extract events from LiveText.aspx which are in reverse chronological order."""
        extraction_script = """
        () => {
            const getSpans = (container) => {
                if (!container) return [];
                return Array.from(container.querySelectorAll('span')).map(span => ({
                    text: span.innerText.trim(),
                    class: span.className
                })).filter(item => item.text !== "");
            };

            const mainContainer = document.querySelector('#numCont11');
            let results = getSpans(mainContainer);

            if (results.length === 0) {
                // If #numCont11 is empty, try individual innings 1-12
                for (let i = 1; i <= 12; i++) {
                    if (i === 11) continue;
                    const container = document.querySelector('#numCont' + i);
                    const inningSpans = getSpans(container);
                    results = results.concat(inningSpans);
                }
            }
            return results;
        }
        """

        try:
            raw_spans = await page.evaluate(extraction_script)
            if not raw_spans:
                return []

            # Since the page is reverse chronological, we REVERSE the list to process it forward.
            raw_spans.reverse()

            state = self._initial_legacy_state()
            sequence = 1
            events = []

            for item in raw_spans:
                text = item["text"]
                cls = item["class"]

                if self._apply_inning_header(state, text, cls):
                    continue

                if "---" in text and len(text) > 10:
                    continue

                if not self._is_legacy_event_text(text, cls):
                    continue

                outs_before, runners_before = self._update_out_base_state(state, text)
                event = self._build_legacy_event(state, text, sequence, outs_before, runners_before)
                events.append(event)
                sequence += 1
        except PBP_CRAWLER_EXCEPTIONS:
            logger.exception("Error extracting PBP legacy (JS)")
            return []
        else:
            return events

    def _format_base_string(self, runners: int) -> str:
        s = ""
        s += "1" if (runners & 1) else "-"
        s += "2" if (runners & 2) else "-"
        s += "3" if (runners & 4) else "-"
        return s

    def _parse_inning_header(self, text: str, idx: int) -> dict[str, Any]:
        match = re.search(r"(\d+)회(초|말)", text)
        if match:
            return {"inning": int(match.group(1)), "half": "top" if match.group(2) == "초" else "bottom"}
        return {"inning": idx + 1, "half": "unknown"}

    def _format_base_string(self, runners: int) -> str:
        s = ""
        s += "1" if (runners & 1) else "-"
        s += "2" if (runners & 2) else "-"
        s += "3" if (runners & 4) else "-"
        return s
