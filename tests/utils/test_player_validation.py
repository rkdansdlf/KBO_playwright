"""Tests for player_validation — player name/ID validation utilities."""


from src.utils.player_validation import (
    filter_valid_player_payloads,
    is_invalid_player_name,
    normalize_player_id,
    normalize_player_name,
    validate_player_payload,
)


class TestNormalizePlayerName:
    def test_normal_korean_name(self):
        assert normalize_player_name("홍길동") == "홍길동"

    def test_strips_whitespace(self):
        assert normalize_player_name("  홍길동  ") == "홍길동"

    def test_none_becomes_empty(self):
        assert normalize_player_name(None) == ""

    def test_int_becomes_string(self):
        assert normalize_player_name(12345) == "12345"


class TestIsInvalidPlayerName:
    def test_normal_name_valid(self):
        assert not is_invalid_player_name("홍길동")

    def test_unknown_pattern(self):
        assert is_invalid_player_name("Unknown Player")
        assert is_invalid_player_name("UNKNOWN PLAYER")

    def test_placeholder_hangul(self):
        # Note: these are not in INVALID_PLAYER_NAMES set currently
        assert not is_invalid_player_name("알수없음")

    def test_unknown_with_number(self):
        assert is_invalid_player_name("unknown 123")
        assert is_invalid_player_name("Unknown 456")

    def test_empty_name(self):
        assert is_invalid_player_name("")

    def test_none_name(self):
        assert is_invalid_player_name(None)


class TestNormalizePlayerId:
    def test_normal_id(self):
        assert normalize_player_id(10001) == 10001

    def test_string_id(self):
        assert normalize_player_id("12345") == 12345

    def test_none_id(self):
        assert normalize_player_id(None) is None

    def test_negative_id(self):
        assert normalize_player_id(-1) is None

    def test_zero_id(self):
        assert normalize_player_id(0) is None


class TestValidatePlayerPayload:
    def test_valid_payload(self):
        payload = {"name": "홍길동", "player_id": 10001}
        valid, reason = validate_player_payload(payload)
        assert valid
        assert reason is None

    def test_missing_name(self):
        payload = {"player_id": 10001}
        valid, reason = validate_player_payload(payload)
        assert not valid
        assert "name" in reason

    def test_invalid_name(self):
        payload = {"name": "Unknown Player", "player_id": 10001}
        valid, reason = validate_player_payload(payload)
        assert not valid
        assert reason == "unknown_player_name"

    def test_missing_id(self):
        payload = {"name": "홍길동"}
        valid, reason = validate_player_payload(payload)
        assert not valid
        assert "player_id" in reason

    def test_invalid_id(self):
        payload = {"name": "홍길동", "player_id": -1}
        valid, reason = validate_player_payload(payload)
        assert not valid
        assert reason == "invalid_player_id"


class TestFilterValidPlayerPayloads:
    def test_all_valid(self):
        payloads = [
            {"name": "홍길동", "player_id": 10001},
            {"name": "김철수", "player_id": 10002},
        ]
        valid, rejected = filter_valid_player_payloads(payloads)
        assert len(valid) == 2
        assert len(rejected) == 0

    def test_some_invalid(self):
        payloads = [
            {"name": "홍길동", "player_id": 10001},
            {"name": "Unknown Player", "player_id": 10002},
        ]
        valid, rejected = filter_valid_player_payloads(payloads)
        assert len(valid) == 1
        assert len(rejected) == 1

    def test_all_invalid(self):
        payloads = [
            {"name": "Unknown", "player_id": -1},
        ]
        valid, rejected = filter_valid_player_payloads(payloads)
        assert len(valid) == 0
        assert len(rejected) == 1
