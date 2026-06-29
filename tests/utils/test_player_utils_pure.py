"""Unit tests for player_classification and player_validation."""

from __future__ import annotations

import pytest

from src.utils.player_classification import (
    PlayerCategory,
    _classify_active_player,
    _normalize,
    classify_player,
)
from src.utils.player_validation import (
    filter_valid_player_payloads,
    is_invalid_player_name,
    normalize_player_id,
    normalize_player_name,
    validate_player_payload,
)


class TestNormalize:
    def test_string(self) -> None:
        assert _normalize("  hello  ") == "hello"

    def test_none(self) -> None:
        assert _normalize(None) == ""

    def test_empty(self) -> None:
        assert _normalize("") == ""


class TestClassifyPlayer:
    def test_manager(self) -> None:
        entry = {"status_source": "register", "staff_role": "manager"}
        assert classify_player(entry) == PlayerCategory.MANAGER

    def test_coach(self) -> None:
        entry = {"status_source": "register", "staff_role": "coach"}
        assert classify_player(entry) == PlayerCategory.COACH

    def test_staff(self) -> None:
        entry = {"status_source": "register", "staff_role": "trainer"}
        assert classify_player(entry) == PlayerCategory.STAFF

    def test_active(self) -> None:
        entry = {"team": "LG", "position": "투수"}
        assert classify_player(entry) == PlayerCategory.ACTIVE

    def test_retired_empty_team(self) -> None:
        entry = {"team": "", "position": "투수"}
        assert classify_player(entry) == PlayerCategory.RETIRED

    def test_retired_keyword(self) -> None:
        entry = {"team": "은퇴", "position": "투수"}
        assert classify_player(entry) == PlayerCategory.RETIRED


class TestClassifyActivePlayer:
    def test_manager_keyword(self) -> None:
        assert _classify_active_player("감독", "LG", "lg") == PlayerCategory.MANAGER

    def test_coach_keyword(self) -> None:
        assert _classify_active_player("코치", "LG", "lg") == PlayerCategory.COACH

    def test_retired_no_team(self) -> None:
        assert _classify_active_player("투수", "", "") == PlayerCategory.RETIRED

    def test_active_normal(self) -> None:
        assert _classify_active_player("투수", "LG", "lg") == PlayerCategory.ACTIVE


class TestNormalizePlayerName:
    def test_string(self) -> None:
        assert normalize_player_name("  김철수  ") == "김철수"

    def test_none(self) -> None:
        assert normalize_player_name(None) == ""

    def test_number(self) -> None:
        assert normalize_player_name(42) == "42"


class TestIsInvalidPlayerName:
    def test_valid(self) -> None:
        assert is_invalid_player_name("김철수") is False

    def test_empty(self) -> None:
        assert is_invalid_player_name("") is True

    def test_none(self) -> None:
        assert is_invalid_player_name(None) is True

    def test_unknown(self) -> None:
        assert is_invalid_player_name("UNKNOWN") is True

    def test_unknown_with_number(self) -> None:
        assert is_invalid_player_name("Unknown 123") is True


class TestNormalizePlayerId:
    def test_valid(self) -> None:
        assert normalize_player_id("12345") == 12345

    def test_int(self) -> None:
        assert normalize_player_id(12345) == 12345

    def test_none(self) -> None:
        assert normalize_player_id(None) is None

    def test_invalid(self) -> None:
        assert normalize_player_id("abc") is None

    def test_negative(self) -> None:
        assert normalize_player_id("-5") is None

    def test_zero(self) -> None:
        assert normalize_player_id("0") is None


class TestValidatePlayerPayload:
    def test_valid(self) -> None:
        ok, reason = validate_player_payload({"player_id": 12345, "name": "김철수"})
        assert ok is True
        assert reason is None

    def test_invalid_id(self) -> None:
        ok, reason = validate_player_payload({"player_id": None, "name": "김철수"})
        assert ok is False
        assert reason == "invalid_player_id"

    def test_missing_name(self) -> None:
        ok, reason = validate_player_payload({"player_id": 12345, "name": ""})
        assert ok is False

    def test_unknown_name(self) -> None:
        ok, reason = validate_player_payload({"player_id": 12345, "name": "UNKNOWN"})
        assert ok is False
        assert reason == "unknown_player_name"


class TestFilterValidPlayerPayloads:
    def test_mixed(self) -> None:
        payloads = [
            {"player_id": 1, "name": "김철수"},
            {"player_id": None, "name": "이영희"},
            {"player_id": 2, "name": ""},
        ]
        valid, reasons = filter_valid_player_payloads(payloads)
        assert len(valid) == 1
        assert valid[0]["player_id"] == 1
        assert reasons["invalid_player_id"] == 1

    def test_all_valid(self) -> None:
        payloads = [
            {"player_id": 1, "name": "김철수"},
            {"player_id": 2, "name": "이영희"},
        ]
        valid, reasons = filter_valid_player_payloads(payloads)
        assert len(valid) == 2
        assert len(reasons) == 0

    def test_empty(self) -> None:
        valid, reasons = filter_valid_player_payloads([])
        assert len(valid) == 0
        assert len(reasons) == 0
