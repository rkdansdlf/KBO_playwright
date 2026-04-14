"""
KBO PBP (Relay) Crawler - Powered by Naver Sports API
Fetches play-by-play data from Naver Sports API instead of KBO website due to access restrictions.
"""
from __future__ import annotations
import httpx
import json
import os
import asyncio
import re
from typing import List, Dict, Any, Optional
from datetime import datetime

from src.services.wpa_calculator import WPACalculator
from src.utils.safe_print import safe_print as print
from src.utils.playwright_pool import AsyncPlaywrightPool

class RelayCrawler:
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

    async def crawl_game_events(self, game_id: str) -> Optional[Dict[str, Any]]:
        """Backward-compatible alias used by older CLI entrypoints."""
        return await self.crawl_game_relay(game_id)

    def _map_to_naver_id(self, kbo_game_id: str) -> str:
        """
        Convert KBO game ID (e.g., 20260412SKLG0) to Naver ID (e.g., 20260412SKLG02026).
        """
        year = kbo_game_id[:4]
        return f"{kbo_game_id}{year}"

    async def crawl_game_relay(self, kbo_game_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch and parse ALL PBP events for a given KBO game ID by iterating innings.
        Supports both LIVE and COMPLETED games natively through the API.
        """
        naver_id = self._map_to_naver_id(kbo_game_id)
        all_text_relays = []
        
        # print(f"[FETCH] Requesting PBP via Naver API for {naver_id}")
        
        try:
            async with httpx.AsyncClient() as client:
                for inn in range(1, 16):
                    url = f"{self.api_base_url.format(game_id=naver_id)}?inning={inn}"
                    response = await client.get(url, headers={**self.headers, "Referer": f"https://m.sports.naver.com/game/{naver_id}/relay"}, timeout=10.0)
                    if response.status_code != 200: break
                    data = response.json()
                    text_relays = data.get("result", {}).get("textRelayData", {}).get("textRelays", [])
                    if not text_relays:
                        if all_text_relays: break
                        else: continue
                    has_logs = any(len(tr.get("textOptions", [])) > 0 for tr in text_relays)
                    if not has_logs and all_text_relays: break
                    all_text_relays.extend(text_relays)
                    # Respect rate limits even if API
                    await asyncio.sleep(0.05)

            if not all_text_relays: return None
            
            events = self._parse_naver_data(all_text_relays)
            # Determine status by heuristic: if 9+ innings and 3 outs recorded, it's completed, but we can just say completed if events exist
            # since game status is handled by GameDetailCrawler anyway.
            return {
                "game_id": kbo_game_id, 
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
            logs = segment.get("textOptions", [])
            # In archived mode, logs are chronological. 
            for log in logs:
                state = log.get("currentGameState", {})
                
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
                description = log.get("text", "")
                
                event = {
                    "event_seq": sequence,
                    "inning": inning,
                    "inning_half": half,
                    "description": description,
                    "event_type": self._detect_event_type(description),
                    "batter_name": log.get("batterRecord", {}).get("name") or log.get("batterName"),
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
