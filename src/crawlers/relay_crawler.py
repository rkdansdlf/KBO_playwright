"""
KBO PBP (Relay) Crawler - Powered by Naver Sports API
Fetches play-by-play data from Naver Sports API instead of KBO website due to access restrictions.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any

import httpx
from sqlalchemy.exc import SQLAlchemyError

from src.services.wpa_calculator import WPACalculator
from src.services.wpa_transitions import apply_wpa_transitions, format_base_string
from src.utils.compliance import compliance
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.relay_text import (
    advance_pitch_count,
    detect_relay_event_type,
    is_relay_result_event_text,
)
from src.utils.request_policy import RequestPolicy
from src.utils.team_codes import normalize_kbo_game_id
from src.utils.type_helpers import to_int

logger = logging.getLogger(__name__)

PARSER_VERSION = "2026-05-31-v1"
SOURCE_SCHEMA_VERSION = "naver-relay-v1"


class _PermanentStatusError(Exception):
    """Raised when the HTTP response indicates a permanent (non-retryable) error."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"permanent_http_{status_code}")


RELAY_CRAWL_EXCEPTIONS = (
    httpx.HTTPError,
    _PermanentStatusError,
    json.JSONDecodeError,
    SQLAlchemyError,
    RuntimeError,
    ValueError,
    TypeError,
    KeyError,
    IndexError,
    OSError,
)


KBO_TO_NAVER_TEAM_CODE = {
    "PA": "PN",  # Panama -> PN (Naver)
    "DB": "DO",  # Doosan (OB) -> DO (Naver)
    "KH": "WO",  # Kiwoom -> WO (Naver uses WO for Heroes franchise)
    "NX": "WO",  # Nexen -> WO
    "HT": "KIA",  # KIA (HT) -> KIA (Naver)
    "SK": "SSG",  # SSG (SK) -> SSG (Naver)
    "DR": "DREAM",  # Dream All-Star
    "NA": "NANUM",  # Nanum All-Star
    "KR": "KOREA",  # National Team
}


class RelayCrawler:
    schedule_fallback_window_days = 7

    def __init__(self, request_delay: float = 1.0, policy=None, pool: AsyncPlaywrightPool | None = None) -> None:
        """
        pool is retained for backward compatibility with GameDetailCrawler but is unused.
        """
        self.api_base_url = "https://api-gw.sports.naver.com/schedule/games/{game_id}/relay"
        self.wpa_calc = WPACalculator()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://m.sports.naver.com",
        }
        self.schedule_api_base_url = "https://api-gw.sports.naver.com/schedule/today-games"
        self.last_resolved_naver_game_id: str | None = None
        self.policy = policy or RequestPolicy(min_delay=request_delay)
        self._last_failure_reason: dict[str, str] = {}
        self.last_failure_reason: str | None = None
        self._last_fetch_failure_reason: str | None = None

    def get_last_failure_reason(self, game_id: str) -> str | None:
        return self._last_failure_reason.get(normalize_kbo_game_id(game_id))

    def _set_failure_reason(self, game_id: str, reason: str) -> None:
        normalized_id = normalize_kbo_game_id(game_id)
        self._last_failure_reason[normalized_id] = reason
        self.last_failure_reason = reason

    async def close(self) -> None:
        """API-based crawler doesn't need explicit resource release for now."""
        pass

    async def crawl_game_events(self, game_id: str) -> dict[str, Any] | None:
        """Backward-compatible alias used by older CLI entrypoints."""
        return await self.crawl_game_relay(game_id)

    def _map_to_naver_id(self, kbo_game_id: str) -> str:
        """
        Convert KBO game ID (e.g., 20260412SKLG0) to Naver ID (e.g., 20260412SKLG02026).
        """
        year = kbo_game_id[:4]
        return f"{kbo_game_id}{year}"

    def _schedule_query_context(
        self,
        kbo_game_id: str | None = None,
        *,
        query_date: str | None = None,
    ) -> dict[str, str]:
        if kbo_game_id and len(kbo_game_id) >= 8:
            date_part = kbo_game_id[:8]
            year_part = date_part[:4]
        else:
            date_part = (query_date or "").replace("-", "")
            year_part = date_part[:4] if len(date_part) >= 4 else str(datetime.now().year)

        query_date_str = query_date or f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
        if "20241110" <= date_part <= "20241124":
            return {
                "sectionId": "worldbaseball",
                "categoryId": "premier12",
                "seasonYear": year_part,
                "date": query_date_str,
            }
        return {
            "sectionId": "kbaseball",
            "categoryId": "kbo",
            "seasonYear": year_part,
            "date": query_date_str,
        }

    def _naver_team_code(self, code: str) -> str:
        return KBO_TO_NAVER_TEAM_CODE.get(str(code or "").strip(), str(code or "").strip())

    def _expected_match_values(self, kbo_game_id: str) -> tuple[str, str, str, str, str]:
        kbo_game_id = normalize_kbo_game_id(kbo_game_id)
        game_date = kbo_game_id[:8]
        away_code = self._naver_team_code(kbo_game_id[8:10])
        home_code = self._naver_team_code(kbo_game_id[10:12])
        doubleheader_no = kbo_game_id[-1] if kbo_game_id[-1:].isdigit() else "0"
        season_year = kbo_game_id[:4]
        return game_date, away_code, home_code, doubleheader_no, season_year

    @staticmethod
    def _schedule_game_has_team_match(game: dict[str, Any], away_code: str, home_code: str) -> bool:
        away_field = str(game.get("awayTeamCode") or "").strip()
        home_field = str(game.get("homeTeamCode") or "").strip()
        if not away_field and not home_field:
            return True
        return away_field == away_code and home_field == home_code

    def _schedule_query_dates(self, kbo_game_id: str) -> list[str]:
        base_date = datetime.strptime(kbo_game_id[:8], "%Y%m%d").date()
        query_dates = [base_date.isoformat()]
        for offset in range(1, self.schedule_fallback_window_days + 1):
            query_dates.append((base_date + timedelta(days=offset)).isoformat())
            query_dates.append((base_date - timedelta(days=offset)).isoformat())
        return query_dates

    PERMANENT_HTTP_ERRORS = frozenset({400, 401, 403, 404, 405, 410, 422})

    async def _request_json(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 10.0,
    ) -> tuple[dict[str, Any] | None, str | None]:
        full_url = str(httpx.URL(url, params=params or {}))
        if not await compliance.is_allowed(full_url):
            logger.info("[COMPLIANCE] Relay request blocked: %s", full_url)
            return None, "blocked"

        async def _fetch() -> dict[str, Any]:
            await self.policy.delay_async(host="api-gw.sports.naver.com")
            response = await client.get(
                url,
                params=params,
                headers=headers or self.headers,
                timeout=timeout,
            )
            if response.status_code != 200:
                status = response.status_code
                if status in self.PERMANENT_HTTP_ERRORS:
                    raise _PermanentStatusError(status)
                raise RuntimeError(f"status_{status}")
            payload = response.json()
            if not isinstance(payload, dict):
                raise TypeError("non_object_json")
            return payload

        try:
            return await self.policy.run_with_retry_async(_fetch), None
        except _PermanentStatusError as exc:
            logger.exception("[INFO] Relay API permanent error: %s status=%s", full_url, exc.status_code)
            return None, f"http_{exc.status_code}"
        except (httpx.HTTPError, RuntimeError, ValueError) as exc:
            logger.warning("Relay API request failed: %s reason=%s", full_url, exc)
            return None, "relay_api_error"

    def _score_suffix_match(self, game_id: str, suffixes: dict) -> int:
        if game_id.endswith(suffixes["exact"]):
            return 100
        if game_id.endswith(suffixes["legacy"]):
            return 90
        if game_id.endswith(suffixes["team"]):
            return 80
        if suffixes["id_has_teams"] and suffixes["dh_no"] in game_id:
            return 20
        return 0

    def _score_team_match(self, g_away: str, g_home: str, away_code: str, home_code: str) -> tuple[int, bool]:
        teams_match = g_away == away_code and g_home == home_code
        if teams_match:
            return 50, True
        score = 0
        if g_away == away_code or g_home == home_code:
            score += 10
        if (g_away and g_away != away_code) or (g_home and g_home != home_code):
            score -= 150
        return score, False

    def _resolve_dh_no(
        self,
        game: dict,
        games: list,
        dh_no_val,
        away_code: str,
        home_code: str,
        game_date_str: str,
    ) -> str:
        dh_no_str = str(dh_no_val or "").strip()
        if dh_no_str in {"1", "2"}:
            return dh_no_str
        same_team_games = []
        for g in games:
            g_away_t = self._naver_team_code(str(g.get("awayTeamCode") or "").strip())
            g_home_t = self._naver_team_code(str(g.get("homeTeamCode") or "").strip())
            if (g_away_t == away_code and g_home_t == home_code) or (g_away_t == home_code and g_home_t == away_code):
                g_date = str(g.get("gameDate") or "").replace("-", "").strip()
                if not g_date:
                    g_id_temp = str(g.get("gameId") or "").strip()
                    if len(g_id_temp) >= 8:
                        date_match = re.search(r"(\d{8})", g_id_temp)
                        if date_match:
                            g_date = date_match.group(1)
                if g_date == game_date_str:
                    same_team_games.append(g)
        if len(same_team_games) > 1:
            same_team_games.sort(key=lambda g: self._game_time_mins(g))
            try:
                return str(same_team_games.index(game) + 1)
            except ValueError:
                return "1"
        return "1"

    def _score_doubleheader(
        self,
        game: dict,
        dh_no: str,
        game_date_str: str,
        away_code: str,
        home_code: str,
        games: list[dict],
    ) -> int:
        def is_dh_truthy(v) -> bool:
            if isinstance(v, bool):
                return v
            if v is None:
                return False
            return str(v).strip().lower() in {"true", "y", "yes", "1", "2"}

        dh_val = game.get("doubleHeader")
        dh_no_val = game.get("doubleHeaderNo")
        is_dh = is_dh_truthy(dh_val) or is_dh_truthy(dh_no_val)

        g_dh = "0"
        if is_dh:
            g_dh = self._resolve_dh_no(game, games, dh_no_val, away_code, home_code, game_date_str)

        if g_dh == dh_no:
            return 30
        return -50

    def _score_date_match(self, game: dict, game_id: str, game_date_str: str) -> int:
        g_date = str(game.get("gameDate") or "").replace("-", "").strip()
        if not g_date and len(game_id) >= 8:
            date_match = re.search(r"(\d{8})", game_id)
            if date_match:
                g_date = date_match.group(1)
        if g_date == game_date_str:
            return 30
        if g_date and g_date[4:8] == game_date_str[4:8]:
            return 10
        return 0

    def _score_time_match(self, game: dict, game_time: str | None) -> int:
        if not game_time:
            return 0
        g_start_time = str(game.get("gameStartTime") or game.get("startTime") or "").strip()
        if not g_start_time:
            return 0
        try:
            k_time_clean = re.sub(r"[^\d:]", "", game_time)
            g_time_clean = re.sub(r"[^\d:]", "", g_start_time)
            if ":" not in k_time_clean or ":" not in g_time_clean:
                return 0
            k_hours, k_mins = map(int, k_time_clean.split(":"))
            g_hours, g_mins = map(int, g_time_clean.split(":"))
        except (ValueError, TypeError):
            logger.warning("Failed to compute time diff score")
            return 0
        else:
            diff_mins = abs((k_hours * 60 + k_mins) - (g_hours * 60 + g_mins))
            if diff_mins == 0:
                return 25
            if diff_mins <= 30:
                return 15
            if diff_mins > 120:
                return -30
            return -10

    def _score_stadium_match(self, game: dict, stadium: str | None) -> int:
        if not stadium:
            return 0
        g_stadium = str(game.get("stadium") or game.get("place") or "").strip().lower()
        if g_stadium == stadium.strip().lower():
            return 30
        return 0

    def _game_time_mins(self, game: dict) -> int:
        t = str(game.get("gameStartTime") or game.get("startTime") or "").strip()
        t_clean = re.sub(r"[^\d:]", "", t)
        if ":" in t_clean:
            try:
                h, m = map(int, t_clean.split(":"))
                return h * 60 + m
            except (ValueError, TypeError):
                pass
        return 0

    def _match_schedule_game(
        self,
        kbo_game_id: str,
        games: list[dict[str, Any]],
        *,
        allow_team_fallback: bool = True,
        stadium: str | None = None,
        game_time: str | None = None,
    ) -> dict[str, Any] | None:
        """
        Match a KBO game ID to a Naver schedule game object using a scoring system.
        """
        game_date_str, away_code, home_code, dh_no, season_year = self._expected_match_values(kbo_game_id)

        exact_suffix = f"{game_date_str[4:8]}{away_code}{home_code}{dh_no}{season_year}"
        team_suffix = f"{away_code}{home_code}{dh_no}{season_year}"
        legacy_suffix = f"{game_date_str[4:8]}{away_code}{home_code}{dh_no}"

        candidates = []
        for game in games:
            game_id = str(game.get("gameId") or "").strip()
            g_away_raw = self._naver_team_code(str(game.get("awayTeamCode") or "").strip())
            g_home_raw = self._naver_team_code(str(game.get("homeTeamCode") or "").strip())
            is_reversed = bool(game.get("reversedHomeAway"))
            has_response_teams = bool(g_away_raw and g_home_raw)
            any_full_team_match = False

            best_iter_score = -1000
            for swapped in [False, True]:
                score = 0
                g_away = g_home_raw if swapped else g_away_raw
                g_home = g_away_raw if swapped else g_home_raw

                cur_exact = (
                    f"{game_date_str[4:8]}{home_code}{away_code}{dh_no}{season_year}" if swapped else exact_suffix
                )
                cur_legacy = f"{game_date_str[4:8]}{home_code}{away_code}{dh_no}" if swapped else legacy_suffix
                suffixes = {
                    "exact": cur_exact,
                    "legacy": cur_legacy,
                    "team": team_suffix,
                    "id_has_teams": self._is_team_in_id(away_code, game_id) and self._is_team_in_id(home_code, game_id),
                    "dh_no": dh_no,
                }
                score += self._score_suffix_match(game_id, suffixes)

                team_score, teams_match = self._score_team_match(g_away, g_home, away_code, home_code)
                score += team_score
                any_full_team_match = any_full_team_match or teams_match

                if swapped == is_reversed and teams_match:
                    score += 10

                best_iter_score = max(best_iter_score, score)

            score = best_iter_score
            if has_response_teams and not any_full_team_match:
                score -= 200

            if len(game_id) >= 8:
                g_mmdd = game_id[4:8]
                k_mmdd = game_date_str[4:8]
                if g_mmdd.isdigit() and g_mmdd != k_mmdd:
                    score -= 100

            score += self._score_doubleheader(game, dh_no, game_date_str, away_code, home_code, games)
            score += self._score_date_match(game, game_id, game_date_str)
            score += self._score_time_match(game, game_time)
            score += self._score_stadium_match(game, stadium)

            candidates.append((game, score))

        if not candidates:
            return None

        best_game, best_score = max(candidates, key=lambda x: x[1])
        threshold = 80 if allow_team_fallback else 120
        if best_score < threshold:
            logger.info(
                "Scores for %s: best=%s, score=%d, below threshold=%d",
                kbo_game_id,
                best_game.get("gameId"),
                best_score,
                threshold,
            )
            return None

        logger.info("Matched %s to Naver gameId=%s score=%d", kbo_game_id, best_game.get("gameId"), best_score)
        return best_game

    def _is_team_in_id(self, team_code: str, game_id: str) -> bool:
        """Check if a team code (modern or legacy) is present in the game ID string."""
        if team_code in game_id:
            return True
        # Check for legacy equivalents commonly found in Naver IDs
        legacy_map = {"KIA": "HT", "SSG": "SK", "WO": "KH", "DO": "OB", "KH": "WO"}
        legacy = legacy_map.get(team_code)
        return bool(legacy and legacy in game_id)

    async def _resolve_naver_game_id(
        self,
        client: httpx.AsyncClient,
        kbo_game_id: str,
        *,
        stadium: str | None = None,
        game_time: str | None = None,
    ) -> str | None:
        query_dates = self._schedule_query_dates(kbo_game_id)
        saw_schedule_games = False
        for index, query_date in enumerate(query_dates):
            query = self._schedule_query_context(kbo_game_id, query_date=query_date)
            payload, failure_reason = await self._request_json(
                client,
                self.schedule_api_base_url,
                params=query,
                headers=self.headers,
                timeout=10.0,
            )
            if payload is None:
                if failure_reason:
                    self._set_failure_reason(kbo_game_id, failure_reason)
                continue
            games = list((payload.get("result") or {}).get("games") or [])
            if games:
                saw_schedule_games = True
            matched = self._match_schedule_game(
                kbo_game_id,
                games,
                allow_team_fallback=(index == 0),
                stadium=stadium,
                game_time=game_time,
            )
            if matched:
                return str(matched.get("gameId") or "").strip() or None
        self._set_failure_reason(kbo_game_id, "invalid_relay_match" if saw_schedule_games else "relay_not_found")
        return None

    async def _fetch_text_relays(
        self,
        client: httpx.AsyncClient,
        naver_id: str,
    ) -> list[dict[str, Any]]:
        all_text_relays: list[dict[str, Any]] = []
        self._last_fetch_failure_reason = None
        for inn in range(1, 16):
            url = f"{self.api_base_url.format(game_id=naver_id)}?inning={inn}"
            data, failure_reason = await self._request_json(
                client,
                url,
                headers={**self.headers, "Referer": f"https://m.sports.naver.com/game/{naver_id}/relay"},
                timeout=10.0,
            )
            if data is None:
                self._last_fetch_failure_reason = failure_reason
                break
            result = data.get("result") or {}
            if not isinstance(result, dict):
                result = {}
            relay_data = result.get("textRelayData") or {}
            if not isinstance(relay_data, dict):
                relay_data = {}
            text_relays = relay_data.get("textRelays") or []
            if not isinstance(text_relays, list):
                text_relays = []
            if not text_relays:
                break
            has_logs = any(len(tr.get("textOptions", [])) > 0 for tr in text_relays)
            if not has_logs and all_text_relays:
                break
            all_text_relays.extend(text_relays)
        return all_text_relays

    async def crawl_game_relay(
        self,
        kbo_game_id: str,
        stadium: str | None = None,
        game_time: str | None = None,
    ) -> dict[str, Any] | None:
        """
        Fetch and parse ALL PBP events for a given KBO game ID by iterating innings.
        Supports both LIVE and COMPLETED games natively through the API.
        """
        kbo_game_id = normalize_kbo_game_id(kbo_game_id)
        self._last_failure_reason.pop(kbo_game_id, None)
        self.last_failure_reason = None
        self.last_resolved_naver_game_id = None
        direct_naver_id = self._map_to_naver_id(kbo_game_id)

        # Resolve stadium and game_time from DB if not explicitly provided
        if not stadium or not game_time:
            try:
                from src.db.engine import SessionLocal
                from src.models.game import Game, GameMetadata

                with SessionLocal() as session:
                    g_row = session.query(Game).filter(Game.game_id == kbo_game_id).first()
                    if g_row:
                        if not game_time:
                            game_time = getattr(g_row, "game_time", None)
                        meta_row = session.query(GameMetadata).filter(GameMetadata.game_id == kbo_game_id).first()
                        if meta_row:
                            if not stadium:
                                stadium = getattr(meta_row, "stadium_name", None)
                            if not game_time:
                                game_time = getattr(meta_row, "start_time", None)
                                if hasattr(game_time, "strftime"):
                                    game_time = game_time.strftime("%H:%M")
            except SQLAlchemyError:
                logger.warning("Failed to extract game metadata for relay relay")

        if game_time and not isinstance(game_time, str):
            try:
                game_time = game_time.strftime("%H:%M")
            except AttributeError:
                game_time = str(game_time)

        try:
            async with httpx.AsyncClient() as client:
                naver_id = direct_naver_id
                all_text_relays = await self._fetch_text_relays(client, naver_id)
                if not all_text_relays:
                    resolved_naver_id = await self._resolve_naver_game_id(
                        client,
                        kbo_game_id,
                        stadium=stadium,
                        game_time=game_time,
                    )
                    if resolved_naver_id and resolved_naver_id != direct_naver_id:
                        self.last_resolved_naver_game_id = resolved_naver_id
                        all_text_relays = await self._fetch_text_relays(client, resolved_naver_id)
                        naver_id = resolved_naver_id

            if not all_text_relays:
                reason = (
                    self.get_last_failure_reason(kbo_game_id) or self._last_fetch_failure_reason or "relay_not_found"
                )
                self._set_failure_reason(kbo_game_id, reason)
                return None

            parsed_payload = self._parse_naver_payload(all_text_relays)
            events = parsed_payload["events"]
            raw_pbp_rows = parsed_payload["raw_pbp_rows"]
            if not events and not raw_pbp_rows:
                self._set_failure_reason(kbo_game_id, "relay_empty")
                return None
            # Determine status by heuristic: if 9+ innings and 3 outs recorded, it's completed, but we can just say completed if events exist
            # since game status is handled by GameDetailCrawler anyway.
            return {
                "game_id": kbo_game_id,
                "naver_game_id": naver_id,
                "game_date": kbo_game_id[:8],
                "status": "completed",
                "events": events,
                "raw_pbp_rows": raw_pbp_rows,
                "parser_version": parsed_payload.get("parser_version", PARSER_VERSION),
                "source_schema_version": parsed_payload.get("source_schema_version", SOURCE_SCHEMA_VERSION),
                "payload_hash": parsed_payload.get("payload_hash"),
            }
        except RELAY_CRAWL_EXCEPTIONS:
            logger.exception("Relay API crawl failed for %s", kbo_game_id)
            self._set_failure_reason(kbo_game_id, "relay_api_error")
            return None

    def _parse_naver_data(self, text_relays: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return self._parse_naver_payload(text_relays)["events"]

    @staticmethod
    def _compute_payload_hash(text_relays: list[dict[str, Any]]) -> str:
        raw = json.dumps(text_relays, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(raw.encode()).hexdigest()[:12]

    @staticmethod
    def _provider_log_id(
        *,
        payload_hash: str,
        inning: int | None,
        half: str | None,
        segment_index: int,
        log_index: int,
        text: str,
    ) -> str:
        text_hash = hashlib.sha1(str(text or "").encode("utf-8")).hexdigest()[:10]
        half_token = (half or "x")[:1]
        inning_token = inning if inning is not None else "x"
        return f"naver:{payload_hash}:{inning_token}{half_token}:{segment_index}:{log_index}:{text_hash}"

    def _parse_naver_payload(self, text_relays: list[dict[str, Any]]) -> dict[str, Any]:
        parsed_events = []
        raw_pbp_rows = []
        sequence = 1
        processed_segments = []
        payload_hash = self._compute_payload_hash(text_relays)

        for index, segment in enumerate(text_relays):
            inn, half = self._parse_segment_inning_half(segment)
            if not inn or not half:
                # Graceful degradation: if we can't parse inning/half but segment has
                # a title-like field, still preserve it in raw_pbp_rows
                title = str(segment.get("title", "") or "")
                if title:
                    raw_pbp_rows.append(
                        {
                            "inning": None,
                            "inning_half": None,
                            "pitcher_name": None,
                            "batter_name": None,
                            "play_description": title,
                            "event_type": "unclassified",
                            "result": None,
                            "provider_log_id": self._provider_log_id(
                                payload_hash=payload_hash,
                                inning=None,
                                half=None,
                                segment_index=index,
                                log_index=-1,
                                text=title,
                            ),
                            "source_row_index": len(raw_pbp_rows),
                            "source_name": "naver",
                        },
                    )
                else:
                    logger.debug("Skipping segment with unparseable inning/half and no title at index=%d", index)
                continue
            segment["_parsed_index"] = index
            segment["_parsed_inn"] = inn
            segment["_parsed_half"] = half
            processed_segments.append(segment)

        sorted_segments = sorted(
            processed_segments,
            key=lambda x: (
                x["_parsed_inn"],
                0 if x["_parsed_half"] == "top" else 1,
                -int(x["_parsed_index"]),
            ),
        )

        pbp_raw_index = 0
        for segment in sorted_segments:
            inning, half = segment["_parsed_inn"], segment["_parsed_half"]
            segment_index = int(segment["_parsed_index"])
            segment_title = str(segment.get("title", "") or "")
            header_index = pbp_raw_index
            pbp_raw_index += 1
            # Insert an explicit inning header marker into the raw PBP stream
            raw_pbp_rows.append(
                {
                    "inning": inning,
                    "inning_half": half,
                    "pitcher_name": None,
                    "batter_name": None,
                    "play_description": segment_title or f"{inning}회{'초' if half == 'top' else '말'}",
                    "event_type": "inning_header",
                    "result": None,
                    "provider_log_id": self._provider_log_id(
                        payload_hash=payload_hash,
                        inning=inning,
                        half=half,
                        segment_index=segment_index,
                        log_index=-1,
                        text=segment_title,
                    ),
                    "source_row_index": header_index,
                    "source_name": "naver",
                },
            )

            logs = segment.get("textOptions") or []
            count_key = None
            balls = 0
            strikes = 0
            # Naver returns batter segments newest-first within each half-inning,
            # while logs inside a segment are chronological.
            for log_index, log in enumerate(logs):
                state = log.get("currentGameState") or {}
                batter_record = log.get("batterRecord") or {}

                home_score = to_int(state.get("homeScore"))
                away_score = to_int(state.get("awayScore"))
                outs = to_int(state.get("out"))

                base_state = 0
                if to_int(state.get("base1")) > 0:
                    base_state |= 1
                if to_int(state.get("base2")) > 0:
                    base_state |= 2
                if to_int(state.get("base3")) > 0:
                    base_state |= 4

                # Graceful degradation: default to 0 if state fields are missing
                if not state:
                    home_score = 0
                    away_score = 0
                    outs = 0
                    base_state = 0

                description = str(log.get("text") or "")
                if not description.strip():
                    continue

                # Graceful degradation for batter/pitcher names: try multiple sources
                batter_name = batter_record.get("name") if isinstance(batter_record, dict) else None
                if not batter_name:
                    batter_name = log.get("batterName")
                if not batter_name and isinstance(log.get("text"), str) and ":" in log["text"]:
                    batter_name = log["text"].split(":", 1)[0].strip() or None
                pitcher_name = log.get("pitcherName")

                batter_key = (inning, half, batter_name or segment_title or segment_index)
                if batter_key != count_key:
                    count_key = batter_key
                    balls = 0
                    strikes = 0
                balls, strikes, _matched_pitch = advance_pitch_count(description, balls, strikes)

                current_pbp_index = pbp_raw_index
                pbp_raw_index += 1
                provider_log_id = self._provider_log_id(
                    payload_hash=payload_hash,
                    inning=inning,
                    half=half,
                    segment_index=segment_index,
                    log_index=log_index,
                    text=description,
                )

                raw_pbp_rows.append(
                    {
                        "inning": inning,
                        "inning_half": half,
                        "pitcher_name": pitcher_name,
                        "batter_name": batter_name,
                        "play_description": description,
                        "event_type": self._detect_event_type(description),
                        "result": description.split(":", 1)[-1].strip() if ":" in description else None,
                        "provider_log_id": provider_log_id,
                        "source_row_index": current_pbp_index,
                        "source_name": "naver",
                    },
                )

                if not is_relay_result_event_text(description):
                    continue

                event: dict[str, Any] = {
                    "event_seq": sequence,
                    "inning": inning,
                    "inning_half": half,
                    "description": description,
                    "event_type": self._detect_event_type(description),
                    "batter_name": batter_name,
                    "pitcher_name": pitcher_name,
                    "home_score": home_score,
                    "away_score": away_score,
                    "score_diff": home_score - away_score,
                    "base_state": base_state,
                    "outs": outs,
                    "bases_before": "-",
                    "bases_after": self._format_base_string(base_state),
                    "wpa": 0.0,
                    "win_expectancy_before": 0.5,
                    "win_expectancy_after": 0.5,
                }

                event["batter"] = event["batter_name"]
                event["pitcher"] = event["pitcher_name"]
                event["result"] = description.split(":", 1)[-1].strip() if ":" in description else None
                event["provider_log_id"] = provider_log_id
                event["source_row_index"] = current_pbp_index
                event["balls"] = balls
                event["strikes"] = strikes

                parsed_events.append(event)
                sequence += 1
                count_key = None
                balls = 0
                strikes = 0

        self._apply_wpa_transitions(parsed_events)

        # Phase 2: At-bat grouping + ball/strike accumulation
        from src.utils.at_bat_grouper import compute_at_bat_pitch_count, group_events_into_at_bats

        group_events_into_at_bats(parsed_events)
        compute_at_bat_pitch_count(parsed_events)

        return {
            "events": parsed_events,
            "raw_pbp_rows": raw_pbp_rows,
            "parser_version": PARSER_VERSION,
            "source_schema_version": SOURCE_SCHEMA_VERSION,
            "payload_hash": payload_hash,
        }

    def _parse_segment_inning_half(self, segment: dict[str, Any]) -> tuple[int, str | None]:
        title = str(segment.get("title") or "")
        match = re.search(r"(\d+)회\s*(초|말)", title)
        if match:
            return int(match.group(1)), "top" if match.group(2) == "초" else "bottom"

        try:
            inning = int(segment.get("inn") or 0)
        except (TypeError, ValueError):
            inning = 0

        raw_value = segment.get("homeOrAway")
        raw_side = str(raw_value if raw_value is not None else "").strip().upper()
        if raw_side in {"0", "AWAY", "A", "TOP", "초"}:
            return inning, "top"
        if raw_side in {"1", "HOME", "H", "BOTTOM", "말"}:
            return inning, "bottom"
        return inning, None

    def _detect_event_type(self, text: str) -> str:
        return detect_relay_event_type(text)

    def _format_base_string(self, runners: int) -> str:
        return format_base_string(runners)

    def _apply_wpa_transitions(self, events: list[dict[str, Any]]) -> None:
        apply_wpa_transitions(events, calculator=self.wpa_calc)


def _events_to_legacy_innings(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    innings: list[dict[str, Any]] = []
    current_key = None
    current_bucket: dict[str, Any] | None = None
    for event in events:
        key = (event.get("inning"), event.get("inning_half"))
        if key != current_key:
            current_key = key
            current_bucket = {
                "inning": event.get("inning"),
                "half": event.get("inning_half"),
                "plays": [],
            }
            innings.append(current_bucket)
        if current_bucket is not None:
            current_bucket["plays"].append(
                {
                    "description": event.get("description"),
                    "event_type": event.get("event_type"),
                    "batter": event.get("batter_name") or event.get("batter"),
                    "pitcher": event.get("pitcher_name") or event.get("pitcher"),
                    "result": event.get("result_code") or event.get("result"),
                    "outs": event.get("outs"),
                },
            )
    return innings


def _pbp_rows_to_legacy_innings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    innings: list[dict[str, Any]] = []
    current_key = None
    current_bucket: dict[str, Any] | None = None
    for row in rows:
        key = (row.get("inning"), row.get("inning_half"))
        if key != current_key:
            current_key = key
            current_bucket = {
                "inning": row.get("inning"),
                "half": row.get("inning_half"),
                "plays": [],
            }
            innings.append(current_bucket)
        if current_bucket is not None:
            current_bucket["plays"].append(
                {
                    "description": row.get("play_description") or row.get("description"),
                    "event_type": row.get("event_type"),
                    "batter": row.get("batter_name") or row.get("batter"),
                    "pitcher": row.get("pitcher_name") or row.get("pitcher"),
                    "result": row.get("result"),
                    "outs": row.get("outs"),
                },
            )
    return innings


async def fetch_and_parse_relay(game_id: str, game_date: str | None = None) -> dict[str, Any] | None:
    """
    Compatibility helper for older tests and scripts that expect inning-grouped output.
    """
    crawler = RelayCrawler()
    result = await crawler.crawl_game_relay(game_id)
    if not result:
        return None
    events = list(result.get("events") or [])
    raw_pbp_rows = list(result.get("raw_pbp_rows") or [])
    return {
        "game_id": game_id,
        "game_date": game_date or game_id[:8],
        "innings": (_pbp_rows_to_legacy_innings(raw_pbp_rows) if raw_pbp_rows else _events_to_legacy_innings(events)),
    }
