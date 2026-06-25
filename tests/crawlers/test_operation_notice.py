"""Tests for operation_notice_common."""

from __future__ import annotations

import pytest

from src.crawlers.operation_notice_common import classify_notice, is_urgent


class TestClassifyNotice:
    def test_cancel(self) -> None:
        assert classify_notice("우천 취소 안내") == "CANCEL"
        assert classify_notice("노게임 공지") == "CANCEL"

    def test_delay(self) -> None:
        assert classify_notice("경기 지연 안내") == "DELAY"
        assert classify_notice("연기 공지") == "DELAY"

    def test_gate_change(self) -> None:
        assert classify_notice("게이트 변경 안내") == "GATE_CHANGE"
        assert classify_notice("출입문 변경") == "GATE_CHANGE"

    def test_entry_rule(self) -> None:
        assert classify_notice("입장 제한 안내") == "ENTRY_RULE"
        assert classify_notice("주차 안내") == "PARKING"

    def test_weather(self) -> None:
        assert classify_notice("날씨 안내") == "WEATHER"
        assert classify_notice("강풍 주의보") == "WEATHER"

    def test_general(self) -> None:
        assert classify_notice("일반 공지") == "GENERAL"
        assert classify_notice("이벤트 안내") == "GENERAL"


class TestIsUrgent:
    def test_urgent(self) -> None:
        assert is_urgent("[긴급] 경기 취소") is True
        assert is_urgent("[필독] 안내") is True
        assert is_urgent("즉시 대응 필요") is True

    def test_not_urgent(self) -> None:
        assert is_urgent("일반 공지") is False
        assert is_urgent("이벤트 안내") is False
