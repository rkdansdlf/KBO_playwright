"""Tests for status_parser — profile status from text."""

import pytest

from src.utils.status_parser import parse_status_from_text


class TestParseStatusFromText:
    def test_manager_korean(self):
        assert parse_status_from_text("감독") == ("staff", "manager")

    def test_manager_english(self):
        assert parse_status_from_text("manager") == ("staff", "manager")

    def test_coach_korean(self):
        assert parse_status_from_text("코치") == ("staff", "coach")

    def test_coach_english(self):
        assert parse_status_from_text("coach") == ("staff", "coach")

    def test_instructor(self):
        assert parse_status_from_text("인스트럭터") == ("staff", "coach")

    def test_trainer(self):
        assert parse_status_from_text("트레이너") == ("staff", "staff")

    def test_analyst(self):
        assert parse_status_from_text("분석원") == ("staff", "staff")

    def test_bullpen_catcher(self):
        assert parse_status_from_text("불펜포수") == ("staff", "staff")

    def test_rehab(self):
        assert parse_status_from_text("재활") == ("staff", "staff")

    def test_conditioning(self):
        assert parse_status_from_text("컨디셔닝") == ("staff", "staff")

    def test_retired_korean(self):
        assert parse_status_from_text("은퇴") == ("retired", None)

    def test_hof(self):
        assert parse_status_from_text("명예의 전당") == ("retired", None)

    def test_text_priority_manager_over_retired(self):
        assert parse_status_from_text("감독 은퇴") == ("staff", "manager")

    def test_text_priority_coach_over_retired(self):
        assert parse_status_from_text("은퇴 코치") == ("staff", "coach")

    def test_text_priority_staff_over_retired(self):
        assert parse_status_from_text("은퇴 트레이너") == ("staff", "staff")

    def test_active_player_returns_none(self):
        assert parse_status_from_text("내야수") is None

    def test_empty_string(self):
        assert parse_status_from_text("") is None
