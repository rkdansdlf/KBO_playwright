"""
KBO PBP (Relay) Crawler - Powered by Naver Sports API
Fetches play-by-play data from Naver Sports API instead of KBO website due to access restrictions.
"""
from __future__ import annotations
import httpx
import asyncio
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from src.services.wpa_calculator import WPACalculator
from src.utils.safe_print import safe_print as print
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.team_codes import normalize_kbo_game_id
from src.utils.throttle import throttle


KBO_TO_NAVER_TEAM_CODE = {
    "PA": "PN",
    "DB": "DO",
}


class RelayCrawler:
    schedule_fallback_window_days = 7

    def __init__(self, request_delay: float = 1.0, policy=None, pool: AsyncPlaywrightPool | None = None):
        """
        pool and policy arguments are retained for backward compatibility with GameDetailCrawler but are unused.
        """
        self.api_base_url = "https://api-gw.sports.naver.com/schedule/games/{game_id}/relay"
        self.wpa_calc = WPACalculator()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://m.sports.naver.com"
        }
        self.schedule_api_base_url = "https://api-gw.sports.naver.com/schedule/today-games"
        self.last_resolved_naver_game_id: str | None = None

    async def crawl_game_events(self, game_id: str) -> Optional[Dict[str, Any]]:
        """Backward-compatible alias used by older CLI entrypoints."""
        return await self.crawl_game_relay(game_id)

    def _map_to_naver_id(self, kbo_game_id: str) -> str:
        """
        Convert KBO game ID (e.g., 20260412SKLG0) to Naver ID (e.g., 20260412SKLG02026).
        """
        year = kbo_game_id[:4]
        return f"{kbo_game_id}{year}"

    def _schedule_query_context(self, kbo_game_id: str, query_date: str | None = None) -> dict[str, str]:
        date = query_date or f"{kbo_game_id[:4]}-{kbo_game_id[4:6]}-{kbo_game_id[6:8]}"
        if "20241110" <= kbo_game_id[:8] <= "20241124":
            return {
                "sectionId": "worldbaseball",
                "categoryId": "premier12",
                "seasonYear": kbo_game_id[:4],
                "date": date,
            }
        return {
            "sectionId": "kbaseball",
            "categoryId": "kbo",
            "seasonYear": kbo_game_id[:4],
            "date": date,
        }

    def _naver_team_code(self, code: str) -> str:
        return KBO_TO_NAVER_TEAM_CODE.get(str(code or "").strip(), str(code or "").strip())

    def _schedule_query_dates(self, kbo_game_id: str) -> List[str]:
        base_date = datetime.strptime(kbo_game_id[:8], "%Y%m%d").date()
        query_dates = [base_date.isoformat()]
        for offset in range(1, self.schedule_fallback_window_days + 1):
            query_dates.append((base_date + timedelta(days=offset)).isoformat())
            query_dates.append((base_date - timedelta(days=offset)).isoformat())
        return query_dates

    def _match_schedule_game(
        self,
        kbo_game_id: str,
        games: List[Dict[str, Any]],
        *,
        allow_team_fallback: bool = True,
    ) -> Optional[Dict[str, Any]]:
        away_code = self._naver_team_code(kbo_game_id[8:10])
        home_code = self._naver_team_code(kbo_game_id[10:12])
        exact_suffix = f"{kbo_game_id[4:8]}{away_code}{home_code}0{kbo_game_id[:4]}"

        for game in games:
            game_id = str(game.get("gameId") or "").strip()
            if game_id.endswith(exact_suffix):
                return game

        if not allow_team_fallback:
            return None

        for game in games:
            if (
                str(game.get("awayTeamCode") or "").strip() == away_code
                and str(game.get("homeTeamCode") or "").strip() == home_code
            ):
                return game

        suffix = f"{away_code}{home_code}0{kbo_game_id[:4]}"
        for game in games:
            game_id = str(game.get("gameId") or "").strip()
            if game_id.endswith(suffix):
                return game
        return None

    async def _resolve_naver_game_id(self, client: httpx.AsyncClient, kbo_game_id: str) -> Optional[str]:
        query_dates = self._schedule_query_dates(kbo_game_id)
        for index, query_date in enumerate(query_dates):
            query = self._schedule_query_context(kbo_game_id, query_date=query_date)
            response = await client.get(
                self.schedule_api_base_url,
                params=query,
                headers=self.headers,
                timeout=10.0,
            )
            if response.status_code != 200:
                continue
            payload = response.json()
            games = list((payload.get("result") or {}).get("games") or [])
            matched = self._match_schedule_game(
                kbo_game_id,
                games,
                allow_team_fallback=(index == 0),
            )
            if matched:
                return str(matched.get("gameId") or "").strip() or None
        return None

    async def _fetch_text_relays(
        self,
        client: httpx.AsyncClient,
        naver_id: str,
    ) -> List[Dict[str, Any]]:
        all_text_relays: list[dict[str, Any]] = []
        for inn in range(1, 16):
            url = f"{self.api_base_url.format(game_id=naver_id)}?inning={inn}"
            response = await client.get(
                url,
                headers={**self.headers, "Referer": f"https://m.sports.naver.com/game/{naver_id}/relay"},
                timeout=10.0,
            )
            if response.status_code != 200:
                break
            data = response.json()
            result = data.get("result") or {}
            relay_data = result.get("textRelayData") or {}
            text_relays = relay_data.get("textRelays") or []
            if not text_relays:
                if all_text_relays:
                    break
                continue
            has_logs = any(len(tr.get("textOptions", [])) > 0 for tr in text_relays)
            if not has_logs and all_text_relays:
                break
            all_text_relays.extend(text_relays)
            await asyncio.sleep(0.05)
        return all_text_relays

    async def crawl_game_relay(self, kbo_game_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch and parse ALL PBP events for a given KBO game ID by iterating innings.
        Supports both LIVE and COMPLETED games natively through the API.
        """
        kbo_game_id = normalize_kbo_game_id(kbo_game_id)
        self.last_resolved_naver_game_id = None
        direct_naver_id = self._map_to_naver_id(kbo_game_id)
        
        try:
            await throttle.wait()
            async with httpx.AsyncClient() as client:
                naver_id = direct_naver_id
                all_text_relays = await self._fetch_text_relays(client, naver_id)
                if not all_text_relays:
                    resolved_naver_id = await self._resolve_naver_game_id(client, kbo_game_id)
                    if resolved_naver_id and resolved_naver_id != direct_naver_id:
                        self.last_resolved_naver_game_id = resolved_naver_id
                        all_text_relays = await self._fetch_text_relays(client, resolved_naver_id)
                        naver_id = resolved_naver_id

            if not all_text_relays: return None
            
            events = self._parse_naver_data(all_text_relays)
            # Determine status by heuristic: if 9+ innings and 3 outs recorded, it's completed, but we can just say completed if events exist
            # since game status is handled by GameDetailCrawler anyway.
            return {
                "game_id": kbo_game_id, 
                "naver_game_id": naver_id,
                "game_date": kbo_game_id[:8], 
                "status": "completed", # Assume completed or live, upstream handles the real status
                "events": events
            }
        except Exception as e:
            print(f"[ERROR] Relay API crawl failed for {kbo_game_id}: {e}")
            return None

    def _parse_naver_data(self, text_relays: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        parsed_events = []
        sequence = 1
        processed_segments = []
        seen_keys = set()
        for segment in text_relays:
            title = segment.get("title", "")
            match = re.search(r'(\d+)회\s*(초|말)', title)
            if match:
                inn, side = int(match.group(1)), ("AWAY" if match.group(2) == "초" else "HOME")
            else:
                inn, side = int(segment.get("inn") or 0), segment.get("homeOrAway", "AWAY")
            key = f"{inn}_{side}"
            if not inn or key in seen_keys: continue
            seen_keys.add(key)
            segment["_parsed_inn"], segment["_parsed_side"] = inn, side
            processed_segments.append(segment)

        sorted_segments = sorted(processed_segments, key=lambda x: (x["_parsed_inn"], 0 if x["_parsed_side"] == "AWAY" else 1))

        for segment in sorted_segments:
            inning, half = segment["_parsed_inn"], ("top" if segment["_parsed_side"] == "AWAY" else "bottom")
            logs = segment.get("textOptions") or []
            # In archived mode, logs are chronological. 
            for log in logs:
                state = log.get("currentGameState") or {}
                batter_record = log.get("batterRecord") or {}
                
                # Naver fields are often strings
                def to_int(val, default=0):
                    try: return int(val) if val is not None else default
                    except: return default

                home_score = to_int(state.get("homeScore"))
                away_score = to_int(state.get("awayScore"))
                outs = to_int(state.get("out"))
                
                base_state = 0
                if to_int(state.get("base1")) > 0: base_state |= 1
                if to_int(state.get("base2")) > 0: base_state |= 2
                if to_int(state.get("base3")) > 0: base_state |= 4
                
                # Ensure description length fits DB column (usually text, but to be safe)
                description = str(log.get("text") or "")
                
                event = {
                    "event_seq": sequence,
                    "inning": inning,
                    "inning_half": half,
                    "description": description,
                    "event_type": self._detect_event_type(description),
                    "batter_name": batter_record.get("name") or log.get("batterName"),
                    "pitcher_name": log.get("pitcherName"),
                    "home_score": home_score,
                    "away_score": away_score,
                    "score_diff": home_score - away_score,
                    "base_state": base_state,
                    "outs": outs,
                    "bases_before": "-", 
                    "bases_after": self._format_base_string(base_state),
                    "wpa": 0.0,
                    "win_expectancy_before": 0.5,
                    "win_expectancy_after": 0.5
                }
                
                # Try to emulate the old structure fields if needed, 
                event['batter'] = event['batter_name']
                event['pitcher'] = event['pitcher_name']
                event['result'] = description.split(":")[-1].strip() if ":" in description else None
                
                parsed_events.append(event)
                sequence += 1
                
        self._apply_wpa_transitions(parsed_events)
        return parsed_events

    def _detect_event_type(self, text: str) -> str:
        if ":" in text and ("타자" in text or "안타" in text or "아웃" in text or "홈런" in text): return "batting"
        if "교체" in text: return "substitution"
        if "도루" in text: return "steal"
        return "unknown"

    def _format_base_string(self, runners: int) -> str:
        return f"{'1' if (runners & 1) else '-'}{'2' if (runners & 2) else '-'}{'3' if (runners & 4) else '-'}"

    def _apply_wpa_transitions(self, events: List[Dict[str, Any]]):
        for i, event in enumerate(events):
            is_bottom = (event["inning_half"] == "bottom")
            if i == 0:
                outs_before, runners_before, score_diff_before = 0, 0, 0
            else:
                prev = events[i-1]
                if prev["inning"] != event["inning"] or prev["inning_half"] != event["inning_half"]:
                    outs_before, runners_before = 0, 0
                else:
                    outs_before, runners_before = prev["outs"], prev["base_state"]
                score_diff_before = prev["home_score"] - prev["away_score"]

            we_before = self.wpa_calc.get_win_probability(event["inning"], is_bottom, outs_before, runners_before, score_diff_before)
            we_after = self.wpa_calc.get_win_probability(event["inning"], is_bottom, event["outs"], event["base_state"], event["home_score"] - event["away_score"])
            
            event["win_expectancy_before"] = we_before
            event["win_expectancy_after"] = we_after
            event["wpa"] = round(we_after - we_before if is_bottom else we_before - we_after, 4)
            event["bases_before"] = self._format_base_string(runners_before)


def _events_to_legacy_innings(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    innings: List[Dict[str, Any]] = []
    current_key = None
    current_bucket: Dict[str, Any] | None = None
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
                }
            )
    return innings


async def fetch_and_parse_relay(game_id: str, game_date: str | None = None) -> Optional[Dict[str, Any]]:
    """
    Compatibility helper for older tests and scripts that expect inning-grouped output.
    """
    crawler = RelayCrawler()
    result = await crawler.crawl_game_relay(game_id)
    if not result:
        return None
    events = list(result.get("events") or [])
    return {
        "game_id": game_id,
        "game_date": game_date or game_id[:8],
        "innings": _events_to_legacy_innings(events),
    }
