"""Utility helpers to classify player rows into active/retired/staff buckets."""

from __future__ import annotations

from enum import StrEnum


class PlayerCategory(StrEnum):
    """PlayerCategory class."""

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


def _normalize(value: str | None) -> str:
    """
    Normalizes  normalize.

    Args:
        value: Value.

    Returns:
        String result.

    """
    return (value or "").strip()


def classify_player(entry: dict[str, object]) -> PlayerCategory:
    """
    Rough heuristic to classify player search rows.

    - Position strings with 감독/코치 계열 → staff categories
    - Empty team/position or explicit '은퇴' 키워드 → RETIRED
    - Otherwise treated as ACTIVE.
    """
    status_source = entry.get("status_source")
    if status_source == "register":
        role = entry.get("staff_role")
        if role == "manager":
            return PlayerCategory.MANAGER
        if role == "coach":
            return PlayerCategory.COACH
        return PlayerCategory.STAFF

    team = _normalize(entry.get("team"))
    position = _normalize(entry.get("position"))
    position.lower()
    team_lower = team.lower()
    return _classify_active_player(position, team, team_lower)


def _classify_active_player(position: str, team: str, team_lower: str) -> PlayerCategory:
    """
    Classifies active player.

    Args:
        position: Position.
        team: Team.
        team_lower: Team Lower.

    Returns:
        PlayerCategory instance.

    """
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
