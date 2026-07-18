"""Text Parser for KBO Relay.

Extracts:
- Outs (from "1사", "2사", etc.)
- Runners (from "1루", "1,2루", "만루")
- Score Changes (from "1점 득점", "홈런").
- Play details (from "양의지 : 좌중간 1루타" etc.)

"""

from __future__ import annotations

import re

# Priority-ordered (keyword, outcome) pairs. First match wins.
_OUTCOME_KEYWORDS: tuple[tuple[str, str], ...] = (
    ("홈런", "home_run"),
    ("3루타", "triple"),
    ("2루타", "double"),
    ("1루타", "single"),
    ("내야안타", "single"),
    ("삼진", "strikeout"),
    ("스트라이크", "strikeout"),
    ("볼넷", "walk"),
    ("고의4구", "intentional_walk"),
    ("몸에 맞는 볼", "hit_by_pitch"),
    ("사구", "hit_by_pitch"),
    ("희생번트", "sacrifice_hit"),
    ("희번", "sacrifice_hit"),
    ("희생플라이", "sacrifice_fly"),
    ("희플", "sacrifice_fly"),
    ("실책", "error"),
    ("병살", "double_play"),
    ("폭투", "wild_pitch"),
    ("견제사", "runner_out"),
    ("주루사", "runner_out"),
    ("태그아웃", "runner_out"),
    ("터치아웃", "runner_out"),
    ("직선타", "lineout"),
    ("땅볼", "groundout"),
    ("뜬공", "flyout"),
    ("플라이", "flyout"),
)

# Priority-ordered (keyword, direction) pairs. First match wins.
_DIRECTION_KEYWORDS: tuple[tuple[str, str], ...] = (
    ("좌", "left"),
    ("우", "right"),
    ("중", "center"),
    ("2루수", "second_base"),
    ("3루수", "third_base"),
    ("유격수", "shortstop"),
    ("1루수", "first_base"),
    ("포수", "catcher"),
)


def _classify_outcome(desc: str) -> str | None:
    """Classify the play outcome from a relay result description."""
    if "도루" in desc:
        return "caught_stealing" if "실패" in desc else "stolen_base"
    for keyword, outcome in _OUTCOME_KEYWORDS:
        if keyword in desc:
            return outcome
    return None


def _classify_direction(desc: str) -> str | None:
    """Classify the hit/field direction from a relay result description."""
    for keyword, direction in _DIRECTION_KEYWORDS:
        if keyword in desc:
            return direction
    return None


def _classify_hit_type(outcome: str | None) -> str | None:
    """Derive the contact hit type from the classified outcome."""
    if outcome in ("home_run", "single", "double", "triple"):
        return "hit"
    if outcome in ("flyout", "groundout", "lineout", "sacrifice_fly"):
        return "flyout" if outcome == "sacrifice_fly" else outcome
    if outcome == "double_play":
        return "groundout"
    return None


class KBOTextParser:
    """KBO text parser for game data extraction."""

    @staticmethod
    def parse_runners(text: str) -> int:
        """Parse runner state bitmask from text.

        0=Empty, 1=1B, 2=2B, 4=3B
        Combinations: 3=1,2B, 5=1,3B, 6=2,3B, 7=Full.

        Typical text: "1사 1,2루", "무사 만루", "2사 3루"
        This usually appears in the PRE-STATE description or result text.

        Args:
            text: Text.

        """
        if "만루" in text:
            return 7

        runners = 0
        # Normalize comma-separated runners: "1,2루" -> "1루 2루"
        normalized = text.replace(",", "루 ").replace("루루", "루")
        if "1루" in normalized:
            runners |= 1
        if "2루" in normalized:
            runners |= 2
        if "3루" in normalized:
            runners |= 4

        return runners

    @staticmethod
    def parse_outs(text: str) -> int:
        """Parse out count 0, 1, 2.

        Args:
            text: Text.

        """
        if "2사" in text or "투아웃" in text:
            return 2
        if "1사" in text or "원아웃" in text:
            return 1
        if "무사" in text or "노아웃" in text:
            return 0
        return 0  # Default/Fallback

    @staticmethod
    def parse_score_change(text: str) -> int:
        """Parse runs scored from event description.

        e.g. "좌월 1점 홈런", "1타점 적시타", "밀어내기 볼넷 (1점 득점)".

        Args:
            text: Text.

        """
        # Explicit score mention
        match = re.search(r"(\d+)점\s*(?:홈런|득점)", text)
        if match:
            return int(match.group(1))

        # Solo HR without "1점" explicit sometimes?
        # Usually KBO text says "XXX 1점 홈런" or "XXX 솔로 홈런"
        if "솔로 홈런" in text:
            return 1
        if "투런 홈런" in text or "2점 홈런" in text:
            return 2
        if "쓰리런 홈런" in text or "3점 홈런" in text:
            return 3
        if "만루 홈런" in text:
            return 4

        return 0

    @staticmethod
    def parse_play_details(text: str) -> dict[str, str | None]:
        """Parse a KBO relay play-description into an outcome/direction/hit-type dict.

        Input is the ``BatterName : <result>`` form used in KBO text relays
        (e.g. ``양의지 : 좌중간 1루타``). Returns a dict with exactly three keys:
        ``play_outcome``, ``hit_direction``, and ``hit_type``; each is ``None`` when
        the field cannot be determined.

        Args:
            text: Relay play-description text.

        Returns:
            Dict with ``play_outcome``, ``hit_direction``, ``hit_type`` keys.

        """
        desc = text.split(":", 1)[-1].strip() if text else ""
        if not desc or desc == "empty":
            return {"play_outcome": None, "hit_direction": None, "hit_type": None}
        outcome = _classify_outcome(desc)
        direction = _classify_direction(desc)
        hit_type = _classify_hit_type(outcome)
        return {
            "play_outcome": outcome,
            "hit_direction": direction,
            "hit_type": hit_type,
        }
