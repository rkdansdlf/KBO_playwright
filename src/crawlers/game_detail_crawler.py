"""GameCenter box score crawler with structured outputs."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)

import contextlib
from datetime import datetime

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page

from src.crawlers.selectors import GAME_DETAIL
from src.db.engine import SessionLocal
from src.urls import GAME_CENTER
from src.utils.compliance import compliance
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.playwright_retry import NAV_TIMEOUT, SEL_TIMEOUT
from src.utils.request_policy import RequestPolicy
from src.utils.team_codes import normalize_kbo_game_id, resolve_team_code, team_code_from_game_id_segment
from src.utils.type_helpers import parse_innings_to_outs, safe_int_or_none

HITTER_HEADER_MAP = {
    "타석": "plate_appearances",
    "타수": "at_bats",
    "득점": "runs",
    "안타": "hits",
    "2루타": "doubles",
    "3루타": "triples",
    "홈런": "home_runs",
    "타점": "rbi",
    "볼넷": "walks",
    "고의4구": "intentional_walks",
    "사구": "hbp",
    "삼진": "strikeouts",
    "도루": "stolen_bases",
    "도실": "caught_stealing",
    "희타": "sacrifice_hits",
    "희비": "sacrifice_flies",
    "병살": "gdp",
    "타율": "avg",
    "출루율": "obp",
    "장타율": "slg",
    "OPS": "ops",
    "ISO": "iso",
    "BABIP": "babip",
}


PITCHER_HEADER_MAP = {
    "이닝": "innings",
    "타자": "batters_faced",
    "투구수": "pitches",
    "피안타": "hits_allowed",
    "실점": "runs_allowed",
    "자책": "earned_runs",
    "피홈런": "home_runs_allowed",
    "볼넷": "walks_allowed",
    "삼진": "strikeouts",
    "사구": "hit_batters",
    "폭투": "wild_pitches",
    "보크": "balks",
    "승": "wins",
    "패": "losses",
    "세": "saves",
    "홀드": "holds",
    "ERA": "era",
    "WHIP": "whip",
    "K/9": "k_per_nine",
    "BB/9": "bb_per_nine",
    "K/BB": "kbb",
}


HITTER_FLOAT_KEYS = {"avg", "obp", "slg", "ops", "iso", "babip"}
PITCHER_FLOAT_KEYS = {"era", "whip", "fip", "k_per_nine", "bb_per_nine", "kbb"}
DETAIL_CRAWLER_EXCEPTIONS = (
    PlaywrightError,
    TimeoutError,
    RuntimeError,
    ValueError,
    TypeError,
    KeyError,
    IndexError,
    OSError,
)


class PlayerIdResolver(Protocol):
    def resolve_id(
        self,
        player_name: str,
        team_code: str,
        season_year: int,
        *,
        uniform_no: str | None = None,
        is_pitcher: bool = False,
    ) -> int | None: ...


class GameDetailCrawler:
    """Crawl KBO GameCenter review pages and return structured box score data."""

    def __init__(
        self,
        request_delay: float | None = None,
        resolver: PlayerIdResolver | None = None,
        pool: AsyncPlaywrightPool | None = None,
    ) -> None:
        self.base_url = GAME_CENTER
        self.policy = RequestPolicy(min_delay=request_delay)
        self.resolver = resolver
        self.pool = pool
        self._last_failure_reason: dict[str, str] = {}

    def get_last_failure_reason(self, game_id: str) -> str | None:
        return self._last_failure_reason.get(game_id)

    async def close(self) -> None:
        if self.pool:
            await self.pool.stop()
            self.pool = None

    def _section_url(self, game_id: str, game_date: str, section: str) -> str:
        return f"{self.base_url}?gameDate={game_date}&gameId={game_id}&section={section}"

    async def _navigate_section(
        self,
        page: Page,
        game_id: str,
        game_date: str,
        section: str,
        *,
        required_selector: str | None = None,
        timeout: int = 30000,
        selector_timeout: int = 15000,
        extra_delay: float = 0,
        wait_until: str = "domcontentloaded",
    ) -> tuple[bool, str, str]:
        url = self._section_url(game_id, game_date, section)
        if not await compliance.is_allowed(url):
            logger.error("❌ BLOCKED by compliance policy: %s", url)
            return False, "blocked", url

        async def _navigate() -> None:
            await self.policy.delay_async(host="www.koreabaseball.com")
            await page.goto(url, wait_until=wait_until, timeout=timeout)
            if extra_delay:
                await asyncio.sleep(extra_delay)
            if required_selector:
                await page.wait_for_selector(required_selector, timeout=selector_timeout)

        try:
            await self.policy.run_with_retry_async(_navigate)
        except DETAIL_CRAWLER_EXCEPTIONS:
            logger.exception("❌ Failed to navigate %s for %s", section, game_id)
            return False, "navigation_error", url

        return True, "ok", url

    async def crawl_game(self, game_id: str, game_date: str, lightweight: bool = False) -> dict[str, Any] | None:
        game_id = normalize_kbo_game_id(game_id)
        self._last_failure_reason.pop(game_id, None)

        # Ensure resolver is available if not provided in __init__
        close_session = False
        if not self.resolver:
            from src.services.player_id_resolver import PlayerIdResolver

            session = SessionLocal()
            self.resolver = PlayerIdResolver(
                session,
                strict_game_resolution=True,
                allow_auto_register=False,
            )
            close_session = True

        try:
            result = await self.crawl_games([{"game_id": game_id, "game_date": game_date}], lightweight=lightweight)
            return result[0] if result else None
        finally:
            if close_session and hasattr(self.resolver, "session"):
                self.resolver.session.close()
                self.resolver = None

    async def crawl_games(
        self,
        games: list[dict[str, str]],
        concurrency: int | None = None,
        lightweight: bool = False,
    ) -> list[dict[str, Any]]:
        if not games:
            return []

        max_concurrency = concurrency or int(os.getenv("KBO_GAME_DETAIL_CONCURRENCY", "3"))
        max_concurrency = max(1, min(max_concurrency, len(games)))

        pool = self.pool or AsyncPlaywrightPool(max_pages=max_concurrency)
        owns_pool = self.pool is None
        if self.pool:
            max_concurrency = min(max_concurrency, self.pool.max_pages)

        results: list[dict[str, Any] | None] = [None] * len(games)
        await pool.start()
        try:
            queue: asyncio.Queue[tuple[int, dict[str, str]] | None] = asyncio.Queue()
            for idx, entry in enumerate(games):
                normalized_entry = dict(entry)
                normalized_entry["game_id"] = normalize_kbo_game_id(entry["game_id"])
                queue.put_nowait((idx, normalized_entry))
            for _ in range(max_concurrency):
                queue.put_nowait(None)

            async def worker() -> None:
                page = await pool.acquire()
                try:
                    while True:
                        item = await queue.get()
                        if item is None:
                            queue.task_done()
                            break
                        idx, entry = item
                        game_id = entry["game_id"]
                        game_date = entry["game_date"]
                        try:
                            payload = await self._crawl_single(page, game_id, game_date, lightweight)
                            results[idx] = payload
                        except DETAIL_CRAWLER_EXCEPTIONS:  # pragma: no cover - resilience path
                            self._last_failure_reason[game_id] = "exception"
                            logger.exception("❌ Error crawling %s", game_id)
                        finally:
                            queue.task_done()
                finally:
                    await pool.release(page)

            workers = [asyncio.create_task(worker()) for _ in range(max_concurrency)]
            await queue.join()
            await asyncio.gather(*workers, return_exceptions=True)
        finally:
            if owns_pool:
                await pool.close()

        return [payload for payload in results if payload]

    async def _crawl_single(
        self,
        page: Page,
        game_id: str,
        game_date: str,
        lightweight: bool = False,
    ) -> dict[str, Any] | None:
        review_url = self._section_url(game_id, game_date, "REVIEW")
        logger.info("📡 Navigating to REVIEW: %s", review_url)

        ok, reason, _ = await self._navigate_section(page, game_id, game_date, "REVIEW")
        if not ok:
            self._last_failure_reason[game_id] = reason
            return None

        is_ready, failure_reason = await self._wait_for_boxscore(page, game_id=game_id, lightweight=lightweight)
        if not is_ready:
            self._last_failure_reason[game_id] = failure_reason
            return None

        roster_map = await self._load_roster_map_from_lineup(page, game_id, game_date, review_url)
        season_year = self._parse_season_year(game_date)
        team_info = await self._extract_team_info(page, game_id, season_year)
        metadata = await self._extract_metadata(page)

        # New: Extract Game Summary
        game_summary = await self._extract_game_summary(page)

        if lightweight:
            hitters = {"away": [], "home": []}
            pitchers = {"away": [], "home": []}
        else:
            # 1. Try to extract from the current REVIEW page directly first.
            # This is extremely fast, avoids redirection/timeout errors, and minimizes KBO server load.
            try:
                review_tab = await page.query_selector(GAME_DETAIL.review_tab)
                if review_tab:
                    await review_tab.click()
                    await asyncio.sleep(0.5)
            except PlaywrightError:
                logger.debug("Review tab not clickable for game")

            away_hitters, away_total = await self._extract_hitters(
                page,
                "away",
                team_info["away"]["code"],
                season_year,
                roster_map,
            )
            home_hitters, home_total = await self._extract_hitters(
                page,
                "home",
                team_info["home"]["code"],
                season_year,
                roster_map,
            )
            pitchers = {
                "away": await self._extract_pitchers(page, "away", team_info["away"]["code"], season_year, roster_map),
                "home": await self._extract_pitchers(page, "home", team_info["home"]["code"], season_year, roster_map),
            }

            max_fallback_attempts = max(1, int(os.getenv("GAMEDETAIL_SECTION_FALLBACK_ATTEMPTS", "2")))
            for attempt in range(1, max_fallback_attempts + 1):
                has_hitters = bool(away_hitters) and bool(home_hitters)
                has_pitchers = bool(pitchers["away"]) and bool(pitchers["home"])

                if has_hitters and has_pitchers:
                    break

                if attempt >= max_fallback_attempts:
                    break

                logger.warning(
                    "⚠️  Incomplete stats on REVIEW for %s. Trying HITTER/PITCHER tabs (attempt %s/%s)...",
                    game_id,
                    attempt,
                    max_fallback_attempts,
                )
                if not has_hitters:
                    ok, reason, _ = await self._navigate_section(
                        page,
                        game_id,
                        game_date,
                        "HITTER",
                        required_selector=GAME_DETAIL.hitter_fallback,
                        selector_timeout=SEL_TIMEOUT,
                    )
                    if ok:
                        away_hitters, away_total = await self._extract_hitters(
                            page,
                            "away",
                            team_info["away"]["code"],
                            season_year,
                            roster_map,
                            use_hitter_section=True,
                        )
                        home_hitters, home_total = await self._extract_hitters(
                            page,
                            "home",
                            team_info["home"]["code"],
                            season_year,
                            roster_map,
                            use_hitter_section=True,
                        )
                    else:
                        logger.warning("⚠️ HITTER section navigation failed for %s: %s", game_id, reason)

                if not (bool(pitchers["away"]) and bool(pitchers["home"])):
                    ok, reason, _ = await self._navigate_section(
                        page,
                        game_id,
                        game_date,
                        "PITCHER",
                        required_selector=GAME_DETAIL.pitcher_fallback,
                        selector_timeout=SEL_TIMEOUT,
                    )
                    if ok:
                        pitchers = {
                            "away": await self._extract_pitchers(
                                page,
                                "away",
                                team_info["away"]["code"],
                                season_year,
                                roster_map,
                                use_pitcher_section=True,
                            ),
                            "home": await self._extract_pitchers(
                                page,
                                "home",
                                team_info["home"]["code"],
                                season_year,
                                roster_map,
                                use_pitcher_section=True,
                            ),
                        }
                    else:
                        logger.warning("⚠️ PITCHER section navigation failed for %s: %s", game_id, reason)

                if attempt < max_fallback_attempts:
                    await asyncio.sleep(0.4)

            if not any((away_hitters, home_hitters, pitchers["away"], pitchers["home"])):
                # If we have at least inning scores or metadata, don't fail completely
                if (
                    team_info.get("away", {}).get("line_score")
                    or team_info.get("home", {}).get("line_score")
                    or metadata.get("stadium")
                    or metadata.get("attendance")
                ):
                    logger.info(
                        "ℹ️  No box scores found for %s, but scoreboard/metadata available. Proceeding with partial recovery.",
                        game_id,
                    )
                else:
                    self._last_failure_reason[game_id] = "incomplete_detail"
                    return None

            # INTEGRITY CHECK: Sum of hits/AB must match team total
            for side, player_list, total_row in [
                ("away", away_hitters, away_total),
                ("home", home_hitters, home_total),
            ]:
                if not total_row:
                    continue  # Some legacy games might miss total row

                sum_hits = sum(p["stats"].get("hits", 0) for p in player_list)
                sum_ab = sum(p["stats"].get("at_bats", 0) for p in player_list)

                if sum_hits != total_row.get("hits") or sum_ab != total_row.get("at_bats"):
                    error_msg = f"Integrity check FAILED for {game_id} ({side}): Players Sum({sum_hits}H, {sum_ab}AB) != Team Total({total_row.get('hits')}H, {total_row.get('at_bats')}AB)"
                    logger.warning("⚠️ %s", error_msg)
                    # Capture debug screenshot
                    await page.screenshot(path=f"data/integrity_warning_{game_id}_{side}.png")
                    # DANGEROUS: Returning payload anyway for final calibration

            hitters = {"away": away_hitters, "home": home_hitters}

        game_data = {
            "game_id": game_id,
            "game_date": game_date,
            "metadata": metadata,
            "summary": game_summary,  # Add to payload
            "teams": team_info,
            "home_team_code": team_info["home"]["code"],
            "away_team_code": team_info["away"]["code"],
            "hitters": hitters,
            "pitchers": pitchers,
        }

        self._last_failure_reason.pop(game_id, None)
        if not lightweight:
            self._log_unresolved_player_ids(game_id, hitters, pitchers)
        return game_data

    async def _wait_for_boxscore(self, page: Page, *, game_id: str, lightweight: bool = False) -> tuple[bool, str]:
        """Wait for box score elements to be visible with fast-fail for cancelled games."""

        async def _is_cancelled() -> bool:
            for selector in GAME_DETAIL.status_selectors:
                status_el = await page.query_selector(selector)
                if not status_el:
                    continue
                try:
                    txt = (await status_el.text_content() or "").strip()
                except PlaywrightError:
                    continue
                if any(cancel_word in txt for cancel_word in ["경기취소", "취소", "우천취소"]):
                    logger.info("ℹ️ Game %s is marked as CANCELLED in UI: '%s'", page.url, txt)
                    return True
            try:
                content_area = await page.query_selector(GAME_DETAIL.content_boxscore_area)
                if content_area:
                    txt = (await content_area.text_content() or "").strip()
                    if "취소" in txt:
                        return True
            except PlaywrightError:
                logger.debug("Cancellation check after timeout failed")
            return False

        if await _is_cancelled():
            return False, "cancelled"

        try:
            await page.wait_for_selector(", ".join(GAME_DETAIL.boxscore_presence_selectors), timeout=SEL_TIMEOUT)
        except PlaywrightError:
            logger.warning("⚠️ Timeout waiting for boxscore selectors. Page URL: %s", page.url)

            if await _is_cancelled():
                return False, "cancelled"

            if lightweight:
                debug_path = f"data/lightweight_timeout_{game_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            else:
                debug_path = f"data/timeout_{game_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"

            Path("data").mkdir(parents=True, exist_ok=True)
            try:
                await page.screenshot(path=debug_path)
                logger.warning("📸 Boxscore timeout debug screenshot saved to: %s", debug_path)
            except PlaywrightError:
                logger.exception("⚠️ Failed to save timeout debug screenshot for %s", page.url)

            return False, "timeout"
        else:
            return True, "ok"

    async def _extract_metadata(self, page: Page) -> dict[str, Any]:
        metadata = {
            "stadium": None,
            "attendance": None,
            "start_time": None,
            "end_time": None,
            "game_time": None,
            "duration_minutes": None,
        }

        try:
            # 1. Try explicit ID selectors (common in older years)
            stadium_el = await page.query_selector(GAME_DETAIL.stadium)
            if stadium_el:
                metadata["stadium"] = (await stadium_el.text_content()).replace("구장 :", "").strip()

            crowd_el = await page.query_selector(GAME_DETAIL.crowd)
            if crowd_el:
                try:
                    val = (await crowd_el.text_content()).replace("관중 :", "").replace(",", "").strip()
                    metadata["attendance"] = int(val)
                except (ValueError, TypeError):
                    logger.debug("Failed to parse attendance value from %s", GAME_DETAIL.crowd)

            # 2. Try generic area search
            info_area = await page.query_selector(GAME_DETAIL.info_area)
            if not info_area:
                return metadata

            text = (await info_area.text_content()).replace("\n", " ")

            stadium_match = re.search(r"구장\s*[:：]\s*([^\s]+)", text)
            if stadium_match:
                metadata["stadium"] = stadium_match.group(1).strip()

            attendance_match = re.search(r"관중\s*[:：]\s*([\d,]+)", text)
            if attendance_match:
                with contextlib.suppress(ValueError):
                    metadata["attendance"] = int(attendance_match.group(1).replace(",", "").strip())

            start_match = re.search(r"개시\s*[:：]\s*([\d:]+)", text)
            if start_match:
                metadata["start_time"] = start_match.group(1).strip()

            end_match = re.search(r"종료\s*[:：]\s*([\d:]+)", text)
            if end_match:
                metadata["end_time"] = end_match.group(1).strip()

            duration_match = re.search(r"경기시간\s*[:：]\s*([\d:]+)", text)
            if duration_match:
                metadata["game_time"] = duration_match.group(1).strip()
                metadata["duration_minutes"] = self._parse_duration_minutes(metadata["game_time"])

        except DETAIL_CRAWLER_EXCEPTIONS:  # pragma: no cover - resilience path
            logger.exception("⚠️  Error extracting metadata")

        return metadata

    async def _extract_team_info(self, page: Page, game_id: str, season_year: int | None) -> dict[str, dict[str, Any]]:
        script = r"""
        () => {
            const getRows = (t, extractTh) => Array.from(t.querySelectorAll('tbody tr')).map(tr =>
                Array.from(tr.querySelectorAll(extractTh ? 'td, th' : 'td')).map(td => {
                    const img = td.querySelector('img');
                    if (img && img.alt) return img.alt;
                    if (img && img.src && img.src.includes('team')) {
                        const match = img.src.match(/\/([A-Z]{2})\.png/i);
                        if (match) return match[1];
                    }
                    const clone = td.cloneNode(true);
                    Array.from(clone.querySelectorAll('span, em, strong, p')).forEach(el => {
                        const txt = (el.textContent || '').trim();
                        if (['승', '패', '무', '세'].includes(txt)) {
                            el.remove();
                        }
                    });
                    let text = (clone.textContent || '').replace(/\u00a0/g, ' ').trim();
                    text = text.replace(/^[승패무세]\s*/, '').replace(/\s*[승패무세]$/, '').trim();
                    text = text.replace(/[\d]+승\s*[\d]+패\s*[\d]+무/, '').trim();
                    return text;
                }).filter(text => text !== '')
            );

            let teamTable = document.getElementById('tblScordboard1') || document.getElementById('tblScoreboard1');
            let inningTable = document.getElementById('tblScordboard2') || document.getElementById('tblScoreboard2');
            let totalTable = document.getElementById('tblScordboard3') || document.getElementById('tblScoreboard3');

            if (teamTable && inningTable && totalTable) {
                const teamRows = getRows(teamTable, true);
                const inningRows = getRows(inningTable, false);
                const totalRows = getRows(totalTable, false);

                if (teamRows.length >= 2 && inningRows.length >= 2 && totalRows.length >= 2) {
                    const headers = ["TEAM", ...Array.from({length: inningRows[0].length}, (_,k)=>String(k+1)), "R", "H", "E"];
                    const rows = [];
                    for (let i=0; i<2; i++) {
                        const teamName = teamRows[i][0] || "Unknown";
                        const innings = inningRows[i];
                        const totals = totalRows[i].slice(0, 3);
                        rows.push([teamName, ...innings, ...totals]);
                    }
                    return { headers, rows };
                }
            }

            const tables = Array.from(document.querySelectorAll('table'));
            teamTable = null; inningTable = null; totalTable = null;

            for (const table of tables) {
                const headers = Array.from(table.querySelectorAll('thead th')).map(th => (th.textContent || '').replace(/\u00a0/g, ' ').trim().toUpperCase());

                if (!teamTable && (headers.some(h => h.includes('TEAM')) || headers.includes('팀') || headers.includes(' '))) {
                    if (headers.length <= 4) teamTable = table;
                }
                if (!inningTable && headers.includes('1') && headers.includes('2') && headers.includes('3')) inningTable = table;
                if (!totalTable && headers.includes('R') && headers.includes('H')) totalTable = table;
            }

            if (!teamTable || !inningTable || !totalTable) return null;

            const getRowsFallback = (t) => Array.from(t.querySelectorAll('tbody tr')).map(tr =>
                Array.from(tr.querySelectorAll('td')).map(td => {
                    const img = td.querySelector('img');
                    if (img && img.alt) return img.alt;
                    if (img && img.src && img.src.includes('team')) {
                        const match = img.src.match(/\/([A-Z]{2})\.png/i);
                        if (match) return match[1];
                    }
                    const clone = td.cloneNode(true);
                    Array.from(clone.querySelectorAll('span, em, strong, p')).forEach(el => {
                        const txt = (el.textContent || '').trim();
                        if (['승', '패', '무', '세'].includes(txt)) {
                            el.remove();
                        }
                    });
                    let text = (clone.textContent || '').replace(/\u00a0/g, ' ').trim();
                    // Keep the record part if it looks like "X승 Y패 Z무" to avoid total wipe
                    if (text.includes('승') && text.includes('패')) {
                         // Don't treat this as the team name if possible
                         return "";
                    }
                    text = text.replace(/^[승패무세]\s*/, '').replace(/\s*[승패무세]$/, '').trim();
                    return text;
                })
            );

            const teamRows = getRowsFallback(teamTable);
            const inningRows = getRowsFallback(inningTable);
            const totalRows = getRowsFallback(totalTable);

            if (teamRows.length >= 2 && inningRows.length >= 2 && totalRows.length >= 2) {
                const headers = ["TEAM", ...Array.from({length: inningRows[0].length}, (_,k)=>String(k+1)), "R", "H", "E"];
                const rows = [];
                for (let i=0; i<2; i++) {
                    let teamName = teamRows[i][0] || "";
                    if (teamName === "TEAM") teamName = ""; // Skip header re-read
                    const innings = inningRows[i];
                    const totals = totalRows[i].slice(0, 3);
                    rows.push([teamName, ...innings, ...totals]);
                }
                return { headers, rows };
            }
            return null;
        }
        """

        result = await page.evaluate(script)
        away_info: dict[str, Any]
        home_info: dict[str, Any]

        if result and len(result["rows"]) >= 2:
            headers = result["headers"]
            rows = result["rows"]
            away_info = self._parse_scoreboard_row(headers, rows[0], season_year)
            home_info = self._parse_scoreboard_row(headers, rows[1], season_year)
        else:
            away_info = home_info = None

        # Fallback to gameId decoding for missing/generic team info (common in All-Star)
        away_segment = game_id[8:10] if len(game_id) >= 10 else None
        home_segment = game_id[10:12] if len(game_id) >= 12 else None

        if not away_info or not away_info.get("code") or away_info.get("name") in (None, "", "Unknown"):
            away_info = {
                "name": away_info.get("name") if away_info else away_segment,
                "code": team_code_from_game_id_segment(away_segment, season_year),
                "score": away_info.get("score") if away_info else None,
                "hits": away_info.get("hits") if away_info else None,
                "errors": away_info.get("errors") if away_info else None,
                "line_score": away_info.get("line_score") if away_info else [],
            }
        if not home_info or not home_info.get("code") or home_info.get("name") in (None, "", "Unknown"):
            home_info = {
                "name": home_info.get("name") if home_info else home_segment,
                "code": team_code_from_game_id_segment(home_segment, season_year),
                "score": home_info.get("score") if home_info else None,
                "hits": home_info.get("hits") if home_info else None,
                "errors": home_info.get("errors") if home_info else None,
                "line_score": home_info.get("line_score") if home_info else [],
            }

        return {"away": away_info, "home": home_info}

    async def _extract_hitters(
        self,
        page: Page,
        team_side: str,
        team_code: str | None,
        season_year: int | None,
        roster_map: dict[str, list[dict[str, Any]]] | None = None,
        _db_session: object | None = None,
        _use_hitter_section: bool = False,
    ) -> list[dict[str, Any]]:
        selectors = (
            [GAME_DETAIL.away_hitter_primary, GAME_DETAIL.away_hitter_extra]
            if team_side == "away"
            else [GAME_DETAIL.home_hitter_primary, GAME_DETAIL.home_hitter_extra]
        )
        tables = []
        for selector in selectors:
            table_rows = await self._extract_table_rows(page, selector)
            if table_rows:
                tables.append(table_rows)

        base_rows = tables[0] if tables else []
        inning_rows = await self._extract_table_rows(
            page,
            GAME_DETAIL.away_hitter_inning if team_side == "away" else GAME_DETAIL.home_hitter_inning,
        )
        extra_rows = tables[1] if len(tables) > 1 else []

        # KEY FIX: Legacy tables (e.g. 2018) might not have player name in the extra table (Table 3).
        # In that case, we must merge by INDEX, assuming 1:1 correspondence.
        extra_has_names = any(r.get("playerName") for r in extra_rows)

        extra_map = {}
        if extra_has_names:
            extra_map = {row["playerName"]: row for row in extra_rows if row["playerName"]}

        results: list[dict[str, Any]] = []
        team_total_stats = {}

        for idx, row in enumerate(base_rows, start=1):
            player_name = row["playerName"]
            if not player_name:
                continue

            if player_name in {"합계", "팀합계"}:
                self._populate_hitter_stats(team_total_stats, {}, row["cells"])
                continue

            # Parse same-name suffix (e.g., "이승현(57)" or "김태훈(우)")
            row_uniform = row.get("cells", {}).get("등번호")
            uniform_no = row_uniform

            m = re.search(r"\(([^)]+)\)", player_name)
            if m:
                suffix = m.group(1).strip()
                player_name = re.sub(r"\s*\([^)]*\)\s*$", "", player_name).strip()
                if suffix.isdigit():
                    uniform_no = suffix

            p_id = safe_int_or_none(row.get("playerId"))

            stats = {}
            extras = {}
            self._populate_hitter_stats(stats, extras, row["cells"])

            # KEY FIX: Derive missing stats from inning breakdown if needed
            if inning_rows and idx - 1 < len(inning_rows):
                derived = self._derive_hitter_stats_from_inning_cells(inning_rows[idx - 1]["cells"])
                for k, v in derived.items():
                    if stats.get(k) in (0, None):
                        stats[k] = v

            # Key fix for Task 3: Use resolver for exhibition/missing IDs
            if p_id is None and self.resolver and team_code and season_year:
                p_id = self.resolver.resolve_id(
                    player_name,
                    team_code,
                    season_year,
                    uniform_no=uniform_no,
                    is_pitcher=False,
                )
                if p_id:
                    logger.info("   [RESOLVED] %s (%s) -> %s", player_name, team_code, p_id)

            # Merge Strategy: Name-based OR Index-based
            if extra_has_names:
                extra_row = extra_map.get(player_name)
            else:
                base_idx = idx - 1  # idx starts at 1
                if base_idx < len(extra_rows):
                    extra_row = extra_rows[base_idx]
                else:
                    extra_row = None

            if extra_row:
                self._populate_hitter_stats(stats, extras, extra_row["cells"])

            # Formula-based PA backfill if still 0
            if stats.get("plate_appearances") in (0, None):
                stats["plate_appearances"] = (
                    (stats.get("at_bats") or 0)
                    + (stats.get("walks") or 0)
                    + (stats.get("hbp") or 0)
                    + (stats.get("sacrifice_hits") or 0)
                    + (stats.get("sacrifice_flies") or 0)
                )

            batting_order = self._parse_batting_order(row["cells"])
            position = self._parse_position(row["cells"])
            is_starter = batting_order is not None and batting_order <= 9

            # Optimization: Check roster_map if ID is missing
            if not p_id and roster_map and player_name in roster_map:
                candidates = roster_map[player_name]
                if len(candidates) == 1:
                    p_id = candidates[0]["id"]
                    # Use roster uniform if available and row uniform is missing
                    if not uniform_no:
                        uniform_no = candidates[0]["uniform"]
                elif len(candidates) > 1:
                    # Ambiguity! If we have uniform_no in row, use it to match
                    if uniform_no:
                        for c in candidates:
                            if c["uniform"] == str(uniform_no):
                                p_id = c["id"]
                                break

            payload = {
                "player_id": p_id,
                "player_name": player_name,
                "uniform_no": uniform_no,
                "team_code": team_code,
                "team_side": team_side,
                "batting_order": batting_order,
                "position": position,
                "is_starter": is_starter,
                "appearance_seq": idx,
                "stats": stats,
                "extras": extras or None,
            }
            results.append(payload)

        return results, team_total_stats

    async def _extract_pitchers(
        self,
        page: Page,
        team_side: str,
        team_code: str | None,
        season_year: int | None,
        roster_map: dict[str, list[dict[str, Any]]] | None = None,
        _db_session: object | None = None,
        _use_pitcher_section: bool = False,
    ) -> list[dict[str, Any]]:
        selectors = (
            [
                GAME_DETAIL.away_pitcher_primary,
                GAME_DETAIL.away_pitcher_alt,
                GAME_DETAIL.away_pitcher_alt2,
            ]
            if team_side == "away"
            else [
                GAME_DETAIL.home_pitcher_primary,
                GAME_DETAIL.home_pitcher_alt,
                GAME_DETAIL.home_pitcher_alt2,
            ]
        )
        rows = []
        for selector in selectors:
            table_rows = await self._extract_table_rows(page, selector)
            if table_rows:
                rows = table_rows
                break

        results: list[dict[str, Any]] = []
        for idx, row in enumerate(rows, start=1):
            player_name = row["playerName"]
            if not player_name or player_name in {"합계", "팀합계"}:
                continue

            # Parse same-name suffix (e.g., "이승현(57)" or "김태훈(우)")
            row_uniform = row.get("cells", {}).get("등번호")
            uniform_no = row_uniform

            m = re.search(r"\(([^)]+)\)", player_name)
            if m:
                suffix = m.group(1).strip()
                player_name = re.sub(r"\s*\([^)]*\)\s*$", "", player_name).strip()
                if suffix.isdigit():
                    uniform_no = suffix

            p_id = safe_int_or_none(row.get("playerId"))

            # Key fix for Task 3: Use resolver for exhibition/missing IDs
            # AND: Auto-search and register if still unknown
            if p_id is None and self.resolver and team_code and season_year:
                # Custom disambiguation for Hanwha duplicate pitchers '박준영' in 2026
                if player_name == "박준영" and team_code == "HH" and season_year == 2026:
                    matching_rows = [r for r in rows if r.get("playerName") == "박준영"]
                    if len(matching_rows) > 1:
                        # Both pitched! Distinguish by order of appearance
                        try:
                            rel_idx = [r["playerName"] for r in rows].index("박준영")
                            if idx == rel_idx + 1:  # first occurrence
                                p_id = 52731
                            else:
                                p_id = 56709
                        except ValueError:
                            p_id = 56709 if idx > 4 else 52731
                    else:
                        # Only one pitched. Distinguish by ERA
                        era_val = 5.0
                        try:
                            era_str = row["cells"].get("평균자책점") or row["cells"].get("ERA")
                            if era_str:
                                era_val = float(era_str)
                        except ValueError:
                            pass

                        if era_val < 3.0:
                            p_id = 56709
                        else:
                            p_id = 52731
                else:
                    p_id = self.resolver.resolve_id(
                        player_name,
                        team_code,
                        season_year,
                        uniform_no=uniform_no,
                        is_pitcher=True,
                    )

                can_register_from_search = not getattr(self.resolver, "strict_game_resolution", False) and getattr(
                    self.resolver,
                    "allow_auto_register",
                    True,
                )
                if p_id is None and can_register_from_search:
                    # PROACTIVE SEARCH: If not found in DB, try to find on KBO site and register
                    logger.info("🔍 Unknown player '%s' (%s) found. Searching KBO...", player_name, team_code)
                    from src.crawlers.player_search_crawler import PlayerSearchCrawler

                    search_crawler = PlayerSearchCrawler()
                    # Note: Using a simplified search to avoid infinite loops
                    new_profiles = await search_crawler.search_player(player_name)
                    if new_profiles:
                        # Register the first matching profile
                        for profile in new_profiles:
                            if profile.get("name") == player_name:
                                p_id = int(profile["player_id"])
                                # Save to DB immediately so resolver can find it next time
                                from src.repositories.player_basic_repository import save_player_basic

                                save_player_basic(profile)
                                logger.info("✅ Registered new player: %s (%s)", player_name, p_id)
                                break

                if p_id:
                    logger.info("   [RESOLVED] %s (%s) -> %s", player_name, team_code, p_id)

            # Optimization: Check roster_map if ID is missing
            if not p_id and roster_map and player_name in roster_map:
                candidates = roster_map[player_name]
                if len(candidates) == 1:
                    p_id = candidates[0]["id"]
                    if not uniform_no:
                        uniform_no = candidates[0]["uniform"]
                elif len(candidates) > 1:
                    if uniform_no:
                        for c in candidates:
                            if c["uniform"] == str(uniform_no):
                                p_id = c["id"]
                                break

            stats = {}
            extras = {}
            self._populate_pitcher_stats(stats, extras, row["cells"])

            innings_text = row["cells"].get("이닝") or row["cells"].get("IP")
            innings_outs = parse_innings_to_outs(innings_text)

            result_text = row["cells"].get("결과") or row["cells"].get("결")
            decision = self._parse_decision(result_text)
            if decision:
                stats["decision"] = decision

            payload = {
                "player_id": p_id,
                "player_name": player_name,
                "uniform_no": uniform_no,
                "team_code": team_code,
                "team_side": team_side,
                "is_starting": idx == 1,
                "appearance_seq": idx,
                "stats": {**stats, "innings_outs": innings_outs},
                "extras": extras or None,
            }
            results.append(payload)

        return results

    async def _extract_table_rows(self, page: Page, selector: str) -> list[dict[str, Any]]:
        if not selector:
            return []

        script = r"""
        (sel) => {
            const table = document.querySelector(sel);
            if (!table) return [];
            const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.textContent.trim());

            // Find '선수명' index
            let nameIndex = -1;
            for (let i = 0; i < headers.length; i++) {
                if (headers[i] === '선수명') {
                    nameIndex = i;
                    break;
                }
            }

            return Array.from(table.querySelectorAll('tbody tr')).map((tr, index) => {
                const cells = Array.from(tr.querySelectorAll('th,td'));
                const values = {};
                for (let i = 0; i < cells.length; i++) {
                    const header = headers[i] || `COL_${i}`;
                    values[header] = cells[i].textContent.trim();
                }
                const link = tr.querySelector('a[href*="playerId="], a[href*="p_id="], a[href*="pCode="], a[href*="pcode="], a[href*="PlayerDetail"]');
                let playerId = null;
                let playerName = null;
                let uniformNo = null;

                // Try to find uniform number in the first cell or a cell with specific header
                if (cells.length > 0) {
                    const firstVal = cells[0].textContent.trim();
                    if (/^\d+$/.test(firstVal)) {
                        uniformNo = firstVal;
                    }
                }

                if (link) {
                    playerName = link.textContent.trim();
                    const href = link.getAttribute('href');
                    try {
                        const url = new URL(href, window.location.origin);
                        playerId = url.searchParams.get('playerId') ||
                                   url.searchParams.get('p_id') ||
                                   url.searchParams.get('pCode') ||
                                   url.searchParams.get('pcode');
                    } catch (e) {
                        playerId = null;
                    }
                    if (!playerId && href) {
                        const m = href.match(/(?:playerId|p_id|pCode|pcode|id)=(\d+)/i);
                        playerId = m ? m[1] : null;
                    }
                }

                // Fallback: Use name column if link not found
                if (!playerName && nameIndex !== -1 && cells.length > nameIndex) {
                    playerName = cells[nameIndex].textContent.trim();
                }

                return { index, cells: values, playerId, playerName, uniformNo };
            });
        }
        """

        return await page.evaluate(script, selector)

    async def _extract_game_summary(self, page: Page) -> list[dict[str, str]]:
        """Extracts game summary details from #tblEtc (Winning hit, HR, Errors, Umpires, etc.)"""
        selector = GAME_DETAIL.etc_table
        if not await page.query_selector(selector):
            return []

        script = r"""
        (sel) => {
            const table = document.querySelector(sel);
            if (!table) return [];

            const results = [];
            const rows = table.querySelectorAll('tbody tr');

            rows.forEach(tr => {
                const th = tr.querySelector('th');
                const td = tr.querySelector('td');
                if (th && td) {
                    const category = (th.textContent || '').trim();
                    const content = (td.textContent || '').trim();
                    if (content && content !== '없음') {
                         results.push({
                             'summary_type': category,
                             'detail_text': content
                         });
                    }
                }
            });
            return results;
        }
        """
        return await page.evaluate(script, selector)

    async def _load_roster_map_from_lineup(
        self,
        page: Page,
        game_id: str,
        game_date: str,
        review_url: str,
    ) -> dict[str, list[dict[str, Any]]]:
        roster_map: dict[str, list[dict[str, Any]]] = {}
        for section in ("ENTRY", "LINEUP"):
            lineup_url = f"{self.base_url}?gameDate={game_date}&gameId={game_id}&section={section}"

            async def _navigate_lineup() -> None:
                await self.policy.delay_async()
                await page.goto(lineup_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
                with contextlib.suppress(PlaywrightError, TimeoutError):
                    await page.wait_for_selector(
                        GAME_DETAIL.lineup_link,
                        timeout=SEL_TIMEOUT,
                    )

            try:
                await self.policy.run_with_retry_async(_navigate_lineup)
                roster_map = await self._extract_roster_from_lineup(page)
                if roster_map:
                    break
            except DETAIL_CRAWLER_EXCEPTIONS:
                logger.exception("⚠️  Failed lineup roster crawl for %s (%s)", game_id, section)

        # Always return to REVIEW page for box score extraction.
        try:

            async def _navigate_back() -> None:
                await self.policy.delay_async()
                await page.goto(review_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)

            await self.policy.run_with_retry_async(_navigate_back)
            await self._wait_for_boxscore(page, game_id=game_id)
        except DETAIL_CRAWLER_EXCEPTIONS:
            logger.exception("⚠️  Failed to return to review page for %s", game_id)
        return roster_map

    def _log_unresolved_player_ids(
        self,
        game_id: str,
        hitters: dict[str, list[dict[str, Any]]],
        pitchers: dict[str, list[dict[str, Any]]],
    ) -> None:
        unresolved = []
        for team_side in ("away", "home"):
            for row in hitters.get(team_side, []):
                if row.get("player_name") and not row.get("player_id"):
                    unresolved.append((row.get("player_name"), row.get("team_code"), row.get("uniform_no")))  # noqa: PERF401
            for row in pitchers.get(team_side, []):
                if row.get("player_name") and not row.get("player_id"):
                    unresolved.append((row.get("player_name"), row.get("team_code"), row.get("uniform_no")))  # noqa: PERF401
        if not unresolved:
            return
        logger.warning("⚠️  Unresolved player_id entries for %s: %s", game_id, len(unresolved))
        for name, team_code, uniform_no in unresolved:
            logger.info("   - name=%s, team_code=%s, uniform_no=%s", name, team_code or "N/A", uniform_no or "N/A")

    def _derive_hitter_stats_from_inning_cells(self, cells: dict[str, str]) -> dict[str, int]:
        """Counts stats from inning breakdown cells (e.g. '삼진', '4구')."""
        derived = {"strikeouts": 0, "walks": 0, "hbp": 0, "sacrifice_hits": 0, "sacrifice_flies": 0}
        for val in cells.values():
            if not val or val == "&nbsp;":
                continue
            if "삼진" in val or "스낫" in val or "루낫" in val or "낫아웃" in val:
                derived["strikeouts"] += 1
            if "4구" in val or "볼넷" in val or "고의4구" in val:
                derived["walks"] += 1
            if "사구" in val or "몸에 맞는 볼" in val:
                derived["hbp"] += 1
            if "희번" in val or "희생번트" in val:
                derived["sacrifice_hits"] += 1
            if "희비" in val or "희생플라이" in val:
                derived["sacrifice_flies"] += 1
        return derived

    def _populate_hitter_stats(self, stats: dict[str, Any], extras: dict[str, Any], cells: dict[str, str]) -> None:
        for header, value in cells.items():
            key = HITTER_HEADER_MAP.get(header)
            if not key:
                extras.setdefault(header, value)
                continue
            if value in ("", "-", None):
                continue
            if key in HITTER_FLOAT_KEYS:
                try:
                    stats[key] = float(value)
                except ValueError:
                    continue
            else:
                stats[key] = safe_int_or_none(value)

    def _populate_pitcher_stats(self, stats: dict[str, Any], extras: dict[str, Any], cells: dict[str, str]) -> None:
        for header, value in cells.items():
            key = PITCHER_HEADER_MAP.get(header)
            if not key:
                extras.setdefault(header, value)
                continue
            if value in ("", "-", None):
                continue
            if key in PITCHER_FLOAT_KEYS:
                try:
                    stats[key] = float(value)
                except ValueError:
                    continue
            elif key == "innings":
                stats["innings_outs"] = parse_innings_to_outs(value)
            else:
                stats[key] = safe_int_or_none(value)

    def _parse_scoreboard_row(
        self,
        headers: list[str],
        row: list[str],
        season_year: int | None = None,
    ) -> dict[str, Any]:
        if not row:
            return {
                "name": None,
                "code": None,
                "line_score": [],
                "score": None,
                "hits": None,
                "errors": None,
            }

        name = row[0]
        if name:
            name = name.replace("승", "").replace("패", "").replace("무", "").replace("세", "").strip()

        # Validate row structure against headers: last 3 should be R/H/E
        if headers and len(headers) >= 4 and len(headers) == len(row):
            _last3 = [h.upper() for h in headers[-3:]]
            if _last3[0] not in ("R", "RUN", "득점", "R/H/E"):
                logger.debug("Unexpected scoreboard header before R: %s", headers[-3])
            if _last3[1] not in ("H", "HIT", "안타"):
                logger.debug("Unexpected scoreboard header before H: %s", headers[-2])
            if _last3[2] not in ("E", "ERR", "실책", "E/H"):
                logger.debug("Unexpected scoreboard header before E: %s", headers[-1])

        line = row[1:-3] if len(row) > 4 else []
        totals = row[-3:] if len(row) >= 3 else []

        score = safe_int_or_none(totals[0]) if totals else None
        hits = safe_int_or_none(totals[1]) if len(totals) > 1 else None
        errors = safe_int_or_none(totals[2]) if len(totals) > 2 else None

        line_numeric = [safe_int_or_none(item) for item in line]

        return {
            "name": name,
            "code": resolve_team_code(name, season_year),
            "line_score": line_numeric,
            "score": score,
            "hits": hits,
            "errors": errors,
        }

    def _parse_batting_order(self, cells: dict[str, str]) -> int | None:
        for key in ("타순", "NO", "No", "순", "타순(교체)", "COL_0"):
            if key in cells:
                value = re.search(r"\d+", cells[key])
                if value:
                    return int(value.group())
        return None

    def _parse_position(self, cells: dict[str, str]) -> str | None:
        for key in ("POS", "포지션", "수비위치", "COL_1"):
            if key in cells:
                return cells[key] or None
        return None

    def _parse_decision(self, text: str | None) -> str | None:
        if not text:
            return None
        text = text.strip()
        result = None
        if "승" in text:
            result = "W"
        elif "패" in text:
            result = "L"
        elif "세" in text:
            result = "S"
        elif "홀드" in text or "H" in text:
            result = "H"
        if result not in ("W", "L", "S", "H"):
            return None
        return result

    @staticmethod
    def _parse_duration_minutes(duration: str | None) -> int | None:
        if not duration:
            return None
        parts = duration.strip().split(":")
        if len(parts) != 2:
            return None
        try:
            hours = int(parts[0])
            minutes = int(parts[1])
            return hours * 60 + minutes
        except ValueError:
            return None

    @staticmethod
    def _parse_season_year(game_date: str) -> int | None:
        digits = "".join(ch for ch in str(game_date) if ch.isdigit())
        if len(digits) >= 4:
            try:
                return int(digits[:4])
            except ValueError:
                return None
        return None

    async def _extract_roster_from_lineup(self, page: Page) -> dict[str, list[dict[str, Any]]]:
        """
        Extracts a map of {PlayerName: [{id, uniform_no}, ...]} from the LINEUP page.
        Used to resolve player IDs when the Review page boxscore lacks links (legacy games).
        """
        script = r"""
        () => {
            const map = {};
            const addToMap = (name, id, uniform) => {
                const cleanName = name.trim();
                if (!cleanName) return;

                if (!map[cleanName]) {
                    map[cleanName] = [];
                }

                // Avoid duplicates
                const exists = map[cleanName].some(p => p.id === id);
                if (!exists) {
                    map[cleanName].push({id, uniform});
                }
            };

            // Find all anchor tags that look like player links
            const links = document.querySelectorAll('a[href*="PlayerDetail"], a[href*="playerId="], a[href*="p_id="], a[href*="pCode="], a[href*="pcode="]');

            links.forEach(a => {
                const name = a.textContent.trim();
                const href = a.getAttribute('href');
                if (!href) return;

                const idMatch = href.match(/(?:playerId|p_id|pCode|pcode|id)=(\d+)/i);
                if (name && idMatch) {
                    let uniform = null;

                    // strategy 1: Check nearby lists or text for "No.XX"
                    const parentLi = a.closest('li');
                    if (parentLi) {
                        const text = parentLi.textContent;
                        const uniMatch = text.match(/No\.(\d+)/);
                        if (uniMatch) uniform = uniMatch[1];
                    }

                    // strategy 2: Check previous sibling or parent structure (table columns)
                    // (Simplification: just take what we found)

                    addToMap(name, idMatch[1], uniform);
                }
            });
            return map;
        }
        """
        try:
            return await page.evaluate(script)
        except PlaywrightError as e:
            logger.warning("Error executing roster extraction script: %s", e, exc_info=True)
            return {}


async def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument("--game_id", help="KBO Game ID (e.g., 20251013SKSS0)")
    parser.add_argument("--date", help="Game Date (YYYYMMDD)")
    parser.add_argument("--save", action="store_true", help="Save to local database")
    args = parser.parse_args()

    if not args.game_id:
        logger.info("Usage: python3 -m src.crawlers.game_detail_crawler --game_id <ID> [--date <YYYYMMDD>] [--save]")
        return

    game_id = normalize_kbo_game_id(args.game_id)
    game_date = args.date or game_id[:8]

    logger.info("🚀 Starting crawl for game %s (%s)...", game_id, game_date)
    crawler = GameDetailCrawler()
    game_data = await crawler.crawl_game(game_id, game_date)
    if game_data and args.save:
        logger.info(
            "Direct --save is intended for one-off parser checks. "
            "Operational collection should use src.cli.collect_games or src.cli.run_daily_update.",
        )
        from src.repositories.game_repository import save_game_detail

        success = save_game_detail(game_data)
        if success:
            logger.info("✅ Successfully saved and triggered sync for %s", game_id)
        else:
            logger.error("❌ Failed to save %s", game_id)
    elif not game_data:
        logger.error("❌ Failed to crawl %s", game_id)
    else:
        logger.info(game_data)


if __name__ == "__main__":  # pragma: no cover
    import asyncio

    asyncio.run(main())
