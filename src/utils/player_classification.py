"""
Utility helpers to classify player rows into active/retired/staff buckets.
"""
from __future__ import annotations

from enum import Enum
from typing import Dict, Optional


class PlayerCategory(str, Enum):
    ACTIVE = "ACTIVE"
    RETIRED = "RETIRED"
    MANAGER = "MANAGER"
    COACH = "COACH"
    STAFF = "STAFF"


STAFF_KEYWORDS = (
    "코치",
    "감독대행",
    "매니저",
    "트레이너",
    "재활",
    "전력분석",
    "불펜포수",
    "불펜",
    "컨디셔닝",
    "수비코디",
    "인스트럭터",
)


def _normalize(value: Optional[str]) -> str:
    return (value or "").strip()


def classify_player(entry: Dict[str, object]) -> PlayerCategory:
    """
    Rough heuristic to classify player search rows.
    - Position strings with 감독/코치 계열 → staff categories
    - Empty team/position or explicit '은퇴' 키워드 → RETIRED
    - Otherwise treated as ACTIVE
    """
    team = _normalize(entry.get("team"))
    position = _normalize(entry.get("position"))

    position_lower = position.lower()
    team_lower = team.lower()

    if "감독" in position:
        return PlayerCategory.MANAGER

    if any(keyword in position for keyword in STAFF_KEYWORDS):
        return PlayerCategory.COACH

    if not team or team_lower in {"", "-", "없음"}:
        return PlayerCategory.RETIRED

    if "은퇴" in team or "retired" in team_lower:
        return PlayerCategory.RETIRED

    if "코치" in team or "감독" in team:
        return PlayerCategory.STAFF

    return PlayerCategory.ACTIVE
