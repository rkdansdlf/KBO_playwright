"""
KBO Preview Crawler
Fetches Pre-game information (Starting Pitchers, Lineups) for LLM context generation.
Uses KBO's internal XHR APIs (GetKboGameList, GetLineUpAnalysis) for stability.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import httpx
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.playwright_retry import NAV_TIMEOUT
from src.utils.request_policy import RequestPolicy
from src.utils.team_codes import normalize_kbo_game_id

logger = logging.getLogger(__name__)
HTTP_API_EXCEPTIONS = (httpx.HTTPError, RuntimeError, ValueError, TypeError)
PLAYWRIGHT_API_EXCEPTIONS = (PlaywrightError, PlaywrightTimeoutError, RuntimeError, ValueError, TypeError, OSError)
LINEUP_PARSE_EXCEPTIONS = (json.JSONDecodeError, TypeError, ValueError, KeyError, IndexError)
PREVIEW_CRAWL_EXCEPTIONS = (*HTTP_API_EXCEPTIONS, *PLAYWRIGHT_API_EXCEPTIONS)


class PreviewCrawler:
    GAME_LIST_URL = "https://www.koreabaseball.com/ws/Main.asmx/GetKboGameList"
    LINEUP_URL = "https://www.koreabaseball.com/ws/Schedule.asmx/GetLineUpAnalysis"
    BASE_REFERER = "https://www.koreabaseball.com/"
    BASE_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }

    def __init__(self, request_delay: float = 1.0, pool: AsyncPlaywrightPool | None = None) -> None:
        self.request_delay = request_delay
        self.pool = pool
        self.policy = RequestPolicy()

    def _coerce_api_payload(self, payload: Any) -> Any | None:
        """Normalize API payloads from ASP.NET/JSON wrappers to a Python object."""
        if payload is None:
            return None
        if isinstance(payload, str):
            payload = payload.strip()
            if not payload:
                return None
            try:
                return self._coerce_api_payload(json.loads(payload))
            except (json.JSONDecodeError, TypeError) as e:
                logger.debug("Failed to parse JSON payload: %s", e)
                return None
        if isinstance(payload, dict) and "d" in payload:
            return self._coerce_api_payload(payload.get("d"))
        return payload

    def _extract_list_payload(self, payload: Any) -> list[Any]:
        """Get list-like payload from various KBO API shapes."""
        payload = self._coerce_api_payload(payload)
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ("game", "games", "result", "data"):
                value = self._coerce_api_payload(payload.get(key))
                if isinstance(value, list):
                    return value
                nested = self._extract_list_payload(value)
                if nested:
                    return nested
        return []

    def _clean_text(self, value: Any) -> str:
        """Return a stripped string for nullable KBO API fields."""
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _to_flag(value: Any) -> bool:
        """Interpret API flags (0/1/numeric/string) as bool."""
        try:
            return bool(int(value))
        except (TypeError, ValueError):
            if isinstance(value, str):
                return value.strip() not in ("", "0", "false", "False", "FALSE")
            return bool(value)

    def _first_non_empty_text(self, payload: dict[str, Any], keys: tuple[str, ...]) -> str:
        for key in keys:
            value = self._clean_text(payload.get(key))
            if value:
                return value
        return ""

    def _first_non_empty_value(self, payload: dict[str, Any], keys: tuple[str, ...]) -> Any | None:
        for key in keys:
            value = payload.get(key)
            if value not in (None, ""):
                return value
        return None

    def _extract_starter_name(self, game: dict[str, Any], side: str) -> str:
        """Extract announced starter names from KBO game-list variants."""
        if side == "away":
            return self._first_non_empty_text(
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
        return self._first_non_empty_text(
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

    def _extract_starter_id(self, game: dict[str, Any], side: str) -> Any | None:
        """Extract announced starter ids from KBO game-list variants."""
        if side == "away":
            return self._first_non_empty_value(
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
        return self._first_non_empty_value(
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

    def _extract_lineup_announced(self, lineup_rows: list[Any], fallback: bool) -> bool:
        """Read LINEUP_CK from GetLineUpAnalysis when present."""
        if not lineup_rows:
            return fallback
        first = lineup_rows[0]
        if isinstance(first, list) and first and isinstance(first[0], dict) and "LINEUP_CK" in first[0]:
            return self._to_flag(first[0].get("LINEUP_CK"))
        if isinstance(first, dict) and "LINEUP_CK" in first:
            return self._to_flag(first.get("LINEUP_CK"))
        return fallback

    def _extract_embedded_game_ids(self, payload: Any) -> set[str]:
        """Collect normalized G_ID values embedded in lineup analysis payloads."""
        game_ids: set[str] = set()
        if isinstance(payload, dict):
            raw_game_id = payload.get("G_ID")
            game_id = normalize_kbo_game_id(raw_game_id) if raw_game_id else None
            if game_id:
                game_ids.add(game_id)
            for value in payload.values():
                game_ids.update(self._extract_embedded_game_ids(value))
        elif isinstance(payload, list):
            for item in payload:
                game_ids.update(self._extract_embedded_game_ids(item))
        return game_ids

    def _lineup_rows_match_game(self, lineup_rows: list[Any], game_id: str) -> bool:
        """Return False when KBO returns stale lineup rows for a different game."""
        expected_game_id = normalize_kbo_game_id(game_id)
        if not expected_game_id:
            return False
        embedded_game_ids = self._extract_embedded_game_ids(lineup_rows)
        return not embedded_game_ids or embedded_game_ids == {expected_game_id}

    async def _fetch_api_json(
        self,
        url: str,
        form: dict[str, Any],
        referer: str,
        page: Any | None = None,
    ) -> Any | None:
        """
        Try direct HTTP first (fast/fewer dependencies), then fallback to Playwright
        request when a page is available.
        """
        headers = dict(self.BASE_HEADERS)
        headers["Referer"] = referer

        # 1) Direct API call.
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await self.policy.run_with_retry_async(
                    client.post,
                    url,
                    data=form,
                    headers=headers,
                    follow_redirects=True,
                )
                response.raise_for_status()
                payload = self._coerce_api_payload(response.json())
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
            response = await self.policy.run_with_retry_async(page.request.post, url, form=form, headers=headers)
            if response.ok:
                payload = self._coerce_api_payload(await response.json())
                if isinstance(payload, (dict, list)):
                    return payload
                logger.warning("⚠️ Unexpected Playwright response type from %s: %s", url, type(payload).__name__)
        except PLAYWRIGHT_API_EXCEPTIONS:
            logger.exception("⚠️ Playwright API call failed for %s", url)
        return None

    async def crawl_preview_for_date(self, game_date: str) -> list[dict[str, Any]]:
        """
        주어진 날짜(game_date: 'YYYYMMDD')의 모든 경기에 대해
        선발투수와 선발 라인업(발표되었을 경우) 정보를 수집합니다.
        """
        logger.info("🔍 Fetching Pre-game preview data for %s...", game_date)

        pool = self.pool
        owns_pool = False
        page = None
        results: list[dict[str, Any]] = []

        try:
            # 1. Fetch Game List and Starting Pitchers
            list_payload = {"leId": "1", "srId": "0,1,3,4,5,7,9", "date": game_date}
            list_data = await self._fetch_api_json(self.GAME_LIST_URL, list_payload, self.BASE_REFERER)

            # If direct API fails, use Playwright fallback only when a pool is explicitly injected.
            if list_data is None:
                if self.pool is not None:
                    pool = self.pool
                else:
                    pool = AsyncPlaywrightPool(max_pages=1)
                    owns_pool = True
                try:
                    await pool.start()
                except PLAYWRIGHT_API_EXCEPTIONS as e:
                    logger.exception("⚠️ Playwright fallback failed")
                    raise RuntimeError("Failed to start Playwright fallback pool") from e
                page = await pool.acquire()
                await page.goto(self.BASE_REFERER, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
                await asyncio.sleep(self.request_delay)
                list_data = await self._fetch_api_json(
                    self.GAME_LIST_URL,
                    list_payload,
                    self.BASE_REFERER,
                    page=page,
                )
            if list_data is None:
                raise RuntimeError(f"HTTP API and Playwright fallback both failed to fetch game list for {game_date}")

            games = self._extract_list_payload(list_data)
            if not games:
                logger.info("ℹ️ No games found or no starting pitchers announced for %s.", game_date)
                return []

            for g in games:
                game_id = normalize_kbo_game_id(g.get("G_ID"))
                if not game_id:
                    continue

                season_year = str(g.get("SEASON_ID", game_date[:4]))
                le_id = str(g.get("LE_ID", 1))
                sr_id = str(g.get("SR_ID", 0))

                away_team_name = self._clean_text(g.get("AWAY_NM"))
                home_team_name = self._clean_text(g.get("HOME_NM"))
                away_starter = self._extract_starter_name(g, "away")
                home_starter = self._extract_starter_name(g, "home")
                away_starter_id = self._extract_starter_id(g, "away")
                home_starter_id = self._extract_starter_id(g, "home")
                stadium = self._clean_text(g.get("S_NM")) or None
                start_time = self._clean_text(g.get("G_TM")) or None
                start_pitcher_announced = self._to_flag(g.get("START_PIT_CK")) or bool(away_starter and home_starter)
                lineup_announced = self._to_flag(g.get("LINEUP_CK"))

                preview_data = {
                    "game_id": game_id,
                    "game_date": game_date,
                    "stadium": stadium,
                    "start_time": start_time,
                    "away_team_name": away_team_name,
                    "home_team_name": home_team_name,
                    "away_starter": away_starter,
                    "away_starter_id": away_starter_id,
                    "home_starter": home_starter,
                    "home_starter_id": home_starter_id,
                    "start_pitcher_announced": start_pitcher_announced,
                    "lineup_announced": lineup_announced,
                    "away_lineup": [],
                    "home_lineup": [],
                }

                # 2. Fetch Lineups for this specific game
                # Lineups might not be announced until ~1 hour before the game.
                await asyncio.sleep(self.request_delay)
                lineup_payload = {
                    "leId": le_id,
                    "srId": sr_id,
                    "seasonId": season_year,
                    "gameId": game_id,
                }
                lineup_data = await self._fetch_api_json(
                    self.LINEUP_URL,
                    lineup_payload,
                    "https://www.koreabaseball.com/Schedule/GameCenter/Preview/LineUp.aspx",
                    page=page,
                )

                if lineup_data:
                    try:
                        lineup_rows = self._extract_list_payload(lineup_data)
                        preview_data["lineup_announced"] = self._extract_lineup_announced(
                            lineup_rows,
                            bool(preview_data["lineup_announced"]),
                        )
                        lineup_rows_match = self._lineup_rows_match_game(lineup_rows, game_id)
                        if preview_data["lineup_announced"] and lineup_rows_match:
                            # Parse Home Lineup (Index 3 is Home)
                            if len(lineup_rows) > 3:
                                preview_data["home_lineup"] = self._parse_lineup_grid(lineup_rows[3])
                            # Parse Away Lineup (Index 4 is Away)
                            if len(lineup_rows) > 4:
                                preview_data["away_lineup"] = self._parse_lineup_grid(lineup_rows[4])
                        elif not lineup_rows_match:
                            logger.warning("⚠️ Ignoring stale lineup payload for %s", game_id)
                    except LINEUP_PARSE_EXCEPTIONS:
                        logger.exception("⚠️ Error parsing lineup for %s", game_id)

                results.append(preview_data)
                logger.info(
                    "✅ Preview Extracted: %s (Starter: %s vs %s) [Lineups: %d vs %d]",
                    game_id,
                    away_starter,
                    home_starter,
                    len(preview_data["away_lineup"]),
                    len(preview_data["home_lineup"]),
                )

            return results

        except PREVIEW_CRAWL_EXCEPTIONS:
            logger.exception("❌ PreviewCrawler error")
            return []
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

    def _parse_lineup_grid(self, grid_str_list: list[str]) -> list[dict[str, str]]:
        """Parses the nested KBO Lineup grid JSON string into a structured list."""
        lineup = []
        if not grid_str_list or not isinstance(grid_str_list, list):
            return lineup

        try:
            grid_data = json.loads(grid_str_list[0])
            rows = grid_data.get("rows", [])
            for row in rows:
                cells = row.get("row", [])
                if len(cells) >= 3:
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
