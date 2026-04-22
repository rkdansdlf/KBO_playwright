"""
KBO Preview Crawler
Fetches Pre-game information (Starting Pitchers, Lineups) for LLM context generation.
Uses KBO's internal XHR APIs (GetKboGameList, GetLineUpAnalysis) for stability.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional

import httpx

from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.safe_print import safe_print as print

from src.utils.team_codes import normalize_kbo_game_id


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

    def __init__(self, request_delay: float = 1.0, pool: Optional[AsyncPlaywrightPool] = None):
        self.request_delay = request_delay
        self.pool = pool

    def _coerce_api_payload(self, payload: Any) -> Optional[Any]:
        """Normalize API payloads from ASP.NET/JSON wrappers to a Python object."""
        if payload is None:
            return None
        if isinstance(payload, str):
            payload = payload.strip()
            if not payload:
                return None
            try:
                return self._coerce_api_payload(json.loads(payload))
            except Exception:
                return None
        if isinstance(payload, dict) and "d" in payload:
            return self._coerce_api_payload(payload.get("d"))
        return payload

    def _extract_list_payload(self, payload: Any) -> List[Any]:
        """Get list-like payload from various KBO API shapes."""
        payload = self._coerce_api_payload(payload)
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ("game", "games", "result", "data"):
                value = self._coerce_api_payload(payload.get(key))
                if isinstance(value, list):
                    return value
        return []

    def _clean_text(self, value: Any) -> str:
        """Return a stripped string for nullable KBO API fields."""
        if value is None:
            return ""
        return str(value).strip()

    async def _fetch_api_json(
        self,
        url: str,
        form: Dict[str, Any],
        referer: str,
        page: Optional[Any] = None,
    ) -> Optional[Any]:
        """
        Try direct HTTP first (fast/fewer dependencies), then fallback to Playwright
        request when a page is available.
        """
        headers = dict(self.BASE_HEADERS)
        headers["Referer"] = referer

        # 1) Direct API call.
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    url,
                    data=form,
                    headers=headers,
                    follow_redirects=True,
                )
                response.raise_for_status()
                payload = self._coerce_api_payload(response.json())
                if isinstance(payload, (dict, list)):
                    return payload
                print(f"⚠️ Unexpected response type from {url}: {type(payload).__name__}")
        except Exception as exc:
            # Keep logs concise; caller may still recover via Playwright.
            print(f"⚠️ HTTP API call failed for {url}: {exc}")

        # 2) Fallback via Playwright request, when a page is available.
        if page is None:
            return None

        try:
            response = await page.request.post(url, form=form, headers=headers)
            if response.ok:
                payload = self._coerce_api_payload(await response.json())
                if isinstance(payload, (dict, list)):
                    return payload
                print(f"⚠️ Unexpected Playwright response type from {url}: {type(payload).__name__}")
        except Exception as exc:
            print(f"⚠️ Playwright API call failed for {url}: {exc}")
        return None

    async def crawl_preview_for_date(self, game_date: str) -> List[Dict[str, Any]]:
        """
        주어진 날짜(game_date: 'YYYYMMDD')의 모든 경기에 대해
        선발투수와 선발 라인업(발표되었을 경우) 정보를 수집합니다.
        """
        print(f"🔍 Fetching Pre-game preview data for {game_date}...")

        pool = self.pool
        owns_pool = False
        page = None
        results: List[Dict[str, Any]] = []

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
                except Exception as exc:
                    print(f"⚠️ Playwright fallback failed: {exc}")
                    return []
                page = await pool.acquire()
                await page.goto(self.BASE_REFERER, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(self.request_delay)
                list_data = await self._fetch_api_json(
                    self.GAME_LIST_URL,
                    list_payload,
                    self.BASE_REFERER,
                    page=page,
                )
            if list_data is None:
                print(f"⚠️ HTTP preview API call returned no data for {game_date}.")
                return []

            games = self._extract_list_payload(list_data)
            if not games:
                print(f"ℹ️ No games found or no starting pitchers announced for {game_date}.")
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
                away_starter = self._clean_text(g.get("T_PIT_P_NM"))
                home_starter = self._clean_text(g.get("B_PIT_P_NM"))
                away_starter_id = g.get("T_PIT_P_ID")
                home_starter_id = g.get("B_PIT_P_ID")
                stadium = self._clean_text(g.get("S_NM")) or None
                start_time = self._clean_text(g.get("G_TM")) or None

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
                        # Parse Home Lineup (Index 3 is Home)
                        if len(lineup_rows) > 3:
                            preview_data["home_lineup"] = self._parse_lineup_grid(lineup_rows[3])
                        # Parse Away Lineup (Index 4 is Away)
                        if len(lineup_rows) > 4:
                            preview_data["away_lineup"] = self._parse_lineup_grid(lineup_rows[4])
                    except Exception as e:
                        print(f"⚠️ Error parsing lineup for {game_id}: {e}")

                results.append(preview_data)
                print(
                    f"✅ Preview Extracted: {game_id} "
                    f"(Starter: {away_starter} vs {home_starter}) "
                    f"[Lineups: {len(preview_data['away_lineup'])} vs {len(preview_data['home_lineup'])}]"
                )

            return results

        except Exception as e:
            print(f"❌ PreviewCrawler error: {e}")
            return []
        finally:
            if page is not None and pool is not None:
                try:
                    await pool.release(page)
                except Exception:
                    pass
            if owns_pool and pool is not None:
                try:
                    await pool.close()
                except Exception:
                    pass

    def _parse_lineup_grid(self, grid_str_list: List[str]) -> List[Dict[str, str]]:
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
                        lineup.append({
                            "batting_order": int(order),
                            "position": pos,
                            "player_name": name,
                        })
        except Exception:
            pass
        return lineup
