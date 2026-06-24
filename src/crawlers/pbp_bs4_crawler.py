"""
PBP BS4 Crawler - Fast Play-by-play data collection for backfilling.
Navigates directly to the Live Text View page using httpx and BeautifulSoup.
Computes WPA transitions based on the events.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

import httpx
from bs4 import BeautifulSoup

from src.services.wpa_calculator import WPACalculator
from src.utils.text_parser import KBOTextParser

logger = logging.getLogger(__name__)


@dataclass
class WpaEventContext:
    inning: int
    is_bottom: bool
    outs_before: int
    runners_before: int
    outs_after: int
    runners_after: int
    score_diff_before: int
    score_diff_after: int


@dataclass
class BaseEventContext:
    sequence: int
    info: dict[str, Any]
    p_text: str
    runs_scored: int
    outs_before: int
    runners_before: int
    runners_after: int
    score_diff_before: int
    wp_before: float
    wp_after: float
    wpa: float
    state: dict[str, int]


PBP_BS4_PARSE_EXCEPTIONS = (RuntimeError, ValueError, TypeError, KeyError, IndexError)


class PBPBS4Crawler:
    def __init__(self) -> None:
        self.base_url = "https://www.koreabaseball.com/Game/LiveTextView2.aspx"
        self.wpa_calc = WPACalculator()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        }

    def crawl_game_events(self, game_id: str) -> dict[str, Any] | None:
        """
        Fetches the LiveTextView2 page for a specific game and extracts PBP data.
        Returns a dictionary with 'game_id', 'game_date', and a list of 'events' (GameEvent structs).
        """
        game_date = game_id[:8]
        url = f"{self.base_url}?gameDate={game_date}&gameId={game_id}"

        try:
            logger.info("[FETCH] BS4 PBP Data: %s", url)
            # Use a longer timeout for KBO server stability, though 15s is usually plenty.
            response = httpx.get(url, headers=self.headers, timeout=15.0, follow_redirects=True)
            response.raise_for_status()
            html = response.text

            # If redirected to KBO global Error page
            if "Error.html" in str(response.url):
                logger.info("[WARN] Redirected to Error page for %s (No PBP data available).", game_id)
                return None

            if "경기 준비중" in html or "취소" in html:
                logger.info("[INFO] Game %s seems to have no relay data.", game_id)
                return None

            # Quick check for relay elements before full BS4 parsing
            if "relay-bx" not in html and "relay-txt" not in html:
                logger.info("[WARN] No relay containers found in HTML for %s.", game_id)
                return None

            logger.info("[INFO] Extracting Relay Data via BeautifulSoup...")
            events = self._parse_html_to_events(html)

        except httpx.HTTPError:
            logger.exception("[ERROR] HTTP fetch failed for %s", game_id)
            return None
        except PBP_BS4_PARSE_EXCEPTIONS:
            logger.exception("BS4 PBP crawl failed for %s", game_id)
            return None
        else:
            if not events:
                return None
            return {"game_id": game_id, "game_date": game_date, "events": events}

    def _parse_html_to_events(self, html: str) -> list[dict[str, Any]]:
        """Extract all PBP events using BeautifulSoup and compute states."""
        soup = BeautifulSoup(html, "lxml")
        raw_data = self._extract_raw_play_data(soup)
        if not raw_data:
            return []

        state = {
            "current_outs": 0,
            "current_runners": 0,
            "home_score": 0,
            "away_score": 0,
        }
        sequence = 1
        events = []

        for idx, item in enumerate(raw_data):
            info = self._parse_inning_header(item["full_text"], idx)
            self._reset_inning_state_if_needed(state, raw_data, info, idx)

            for p_text in item["plays"]:
                event = self._build_play_event(sequence, info, p_text, state)
                events.append(event)
                sequence += 1

        return events

    @staticmethod
    def _extract_raw_play_data(soup: BeautifulSoup) -> list[dict[str, Any]]:
        raw_data = []
        for container in soup.select(".relay-bx"):
            full_text = container.get_text(separator=" ", strip=True)
            play_els = container.select(".txt-box, .play-txt, p")
            plays = [el.get_text(strip=True) for el in play_els if el.get_text(strip=True)]
            raw_data.append({"full_text": full_text, "plays": plays})
        return raw_data

    def _reset_inning_state_if_needed(
        self,
        state: dict[str, int],
        raw_data: list[dict[str, Any]],
        info: dict[str, Any],
        idx: int,
    ) -> None:
        if idx <= 0:
            return
        prev_info = self._parse_inning_header(raw_data[idx - 1]["full_text"], idx - 1)
        if prev_info == info:
            return
        state["current_outs"] = 0
        state["current_runners"] = 0

    def _build_play_event(
        self,
        sequence: int,
        info: dict[str, Any],
        p_text: str,
        state: dict[str, int],
    ) -> dict[str, Any]:
        inning = info["inning"]
        is_bottom = info["half"] == "bottom"
        outs_before, runners_before = self._apply_explicit_state(p_text, state)
        score_diff_before = state["home_score"] - state["away_score"]
        runs_scored = KBOTextParser.parse_score_change(p_text)
        self._advance_score(state, is_bottom=is_bottom, runs_scored=runs_scored)
        self._advance_outs(state, p_text)
        outs_after = state["current_outs"]
        runners_after = 0
        score_diff_after = state["home_score"] - state["away_score"]
        wp_before, wp_after, wpa = self._calculate_wpa(
            WpaEventContext(
                inning=inning,
                is_bottom=is_bottom,
                outs_before=outs_before,
                runners_before=runners_before,
                outs_after=outs_after,
                runners_after=runners_after,
                score_diff_before=score_diff_before,
                score_diff_after=score_diff_after,
            )
        )
        event = self._base_event_payload(
            BaseEventContext(
                sequence=sequence,
                info=info,
                p_text=p_text,
                runs_scored=runs_scored,
                outs_before=outs_before,
                runners_before=runners_before,
                runners_after=runners_after,
                score_diff_before=score_diff_before,
                wp_before=wp_before,
                wp_after=wp_after,
                wpa=wpa,
                state=state,
            )
        )
        self._apply_basic_event_parsing(event, p_text)
        return event

    @staticmethod
    def _apply_explicit_state(p_text: str, state: dict[str, int]) -> tuple[int, int]:
        outs_before = state["current_outs"]
        runners_before = state["current_runners"]
        if "사" not in p_text or ("루" not in p_text and "무사" not in p_text):
            return outs_before, runners_before
        parsed_outs = KBOTextParser.parse_outs(p_text)
        parsed_runners = KBOTextParser.parse_runners(p_text)
        if parsed_outs >= 0:
            outs_before = parsed_outs
            state["current_outs"] = outs_before
        if parsed_runners >= 0:
            runners_before = parsed_runners
            state["current_runners"] = runners_before
        return outs_before, runners_before

    @staticmethod
    def _advance_score(state: dict[str, int], *, is_bottom: bool, runs_scored: int) -> None:
        if is_bottom:
            state["home_score"] += runs_scored
        else:
            state["away_score"] += runs_scored

    @staticmethod
    def _advance_outs(state: dict[str, int], p_text: str) -> None:
        out_keywords = ("삼진", "아웃", "플라이", "땅볼", "범타")
        if not any(keyword in p_text for keyword in out_keywords):
            return
        if "병살" in p_text:
            state["current_outs"] += 2
        elif "삼중살" in p_text:
            state["current_outs"] += 3
        else:
            state["current_outs"] += 1
        state["current_outs"] = min(state["current_outs"], 3)

    def _calculate_wpa(
        self,
        ctx: WpaEventContext,
    ) -> tuple[float, float, float]:
        wp_before = self.wpa_calc.get_win_probability(
            ctx.inning,
            is_bottom=ctx.is_bottom,
            outs=ctx.outs_before,
            runners=ctx.runners_before,
            score_diff=ctx.score_diff_before,
        )
        wp_after = self.wpa_calc.get_win_probability(
            ctx.inning,
            is_bottom=ctx.is_bottom,
            outs=ctx.outs_after,
            runners=ctx.runners_after,
            score_diff=ctx.score_diff_after,
        )
        wpa = round(wp_after - wp_before if ctx.is_bottom else wp_before - wp_after, 4)
        return wp_before, wp_after, wpa

    def _base_event_payload(
        self,
        ctx: BaseEventContext,
    ) -> dict[str, Any]:
        return {
            "event_seq": ctx.sequence,
            "inning": ctx.info["inning"],
            "inning_half": ctx.info["half"],
            "description": ctx.p_text,
            "event_type": "unknown",
            "batter": None,
            "pitcher": None,
            "result": None,
            "wpa": ctx.wpa,
            "win_expectancy_before": ctx.wp_before,
            "win_expectancy_after": ctx.wp_after,
            "score_diff": ctx.score_diff_before,
            "home_score": ctx.state["home_score"],
            "away_score": ctx.state["away_score"],
            "base_state": ctx.runners_before,
            "outs": ctx.outs_before,
            "bases_before": self._format_base_string(ctx.runners_before),
            "bases_after": self._format_base_string(ctx.runners_after),
            "result_code": None,
            "rbi": ctx.runs_scored,
        }

    @staticmethod
    def _apply_basic_event_parsing(event: dict[str, Any], p_text: str) -> None:
        if "타자" in p_text and ":" in p_text:
            event["event_type"] = "batting"
            parts = p_text.split(":", 1)
            if len(parts) > 1:
                event["batter"] = parts[0].replace("타자", "").strip()
                event["result"] = parts[1].strip()
                event["result_code"] = parts[1].strip()
        elif "투수" in p_text and "교체" in p_text:
            event["event_type"] = "pitching_change"
        elif "도루" in p_text:
            event["event_type"] = "steal"

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
