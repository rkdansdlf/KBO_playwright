from __future__ import annotations

from typing import Optional, Tuple

PROFILE_RETIRE_KEYWORDS = ("은퇴", "명예의 전당")
PROFILE_MANAGER_KEYWORDS = ("감독", "manager")
PROFILE_COACH_KEYWORDS = ("코치", "coach", "인스트럭터")
PROFILE_STAFF_KEYWORDS = ("트레이너", "분석원", "불펜포수", "재활", "컨디셔닝")


def parse_status_from_text(text: str) -> Optional[Tuple[str, Optional[str]]]:
    """
    Inspect raw profile text and return (status, staff_role) if a profile label is found.
    status: 'staff' or 'retired'; staff_role is lower-case string (manager/coach/staff/None)
    """
    lowered = text.lower()

    def contains(keywords):
        return any(keyword in lowered for keyword in keywords)

    if contains(PROFILE_MANAGER_KEYWORDS):
        return "staff", "manager"
    if contains(PROFILE_COACH_KEYWORDS):
        return "staff", "coach"
    if contains(PROFILE_STAFF_KEYWORDS):
        return "staff", "staff"
    if contains(PROFILE_RETIRE_KEYWORDS):
        return "retired", None
    return None
