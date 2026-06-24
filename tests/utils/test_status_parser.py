from __future__ import annotations

from src.utils.status_parser import (
    PROFILE_COACH_KEYWORDS,
    PROFILE_MANAGER_KEYWORDS,
    PROFILE_RETIRE_KEYWORDS,
    PROFILE_STAFF_KEYWORDS,
    parse_status_from_text,
)


class TestParseStatusFromText:
    def test_manager_keyword(self):
        result = parse_status_from_text("LG 감독")
        assert result == ("staff", "manager")

    def test_manager_english(self):
        result = parse_status_from_text("manager")
        assert result == ("staff", "manager")

    def test_coach_keyword(self):
        result = parse_status_from_text("코치")
        assert result == ("staff", "coach")

    def test_coach_english(self):
        result = parse_status_from_text("coach")
        assert result == ("staff", "coach")

    def test_staff_trainer(self):
        result = parse_status_from_text("트레이너")
        assert result == ("staff", "staff")

    def test_staff_analyst(self):
        result = parse_status_from_text("분석원")
        assert result == ("staff", "staff")

    def test_retired_keyword(self):
        result = parse_status_from_text("은퇴")
        assert result == ("retired", None)

    def test_retired_hall_of_fame(self):
        result = parse_status_from_text("명예의 전당")
        assert result == ("retired", None)

    def test_none_for_unknown(self):
        result = parse_status_from_text("투수")
        assert result is None

    def test_none_for_empty(self):
        result = parse_status_from_text("")
        assert result is None

    def test_case_insensitive(self):
        result = parse_status_from_text("MANAGER")
        assert result == ("staff", "manager")

    def test_manager_takes_priority_over_coach(self):
        result = parse_status_from_text("감독 코치")
        assert result == ("staff", "manager")


class TestProfileKeywords:
    def test_manager_keywords(self):
        assert "감독" in PROFILE_MANAGER_KEYWORDS
        assert "manager" in PROFILE_MANAGER_KEYWORDS

    def test_coach_keywords(self):
        assert "코치" in PROFILE_COACH_KEYWORDS
        assert "coach" in PROFILE_COACH_KEYWORDS

    def test_staff_keywords(self):
        assert "트레이너" in PROFILE_STAFF_KEYWORDS
        assert "재활" in PROFILE_STAFF_KEYWORDS

    def test_retire_keywords(self):
        assert "은퇴" in PROFILE_RETIRE_KEYWORDS
        assert "명예의 전당" in PROFILE_RETIRE_KEYWORDS
