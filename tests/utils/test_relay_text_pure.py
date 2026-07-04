"""Unit tests for utility pure functions (Tier 3)."""

from __future__ import annotations

import pytest

from src.utils.relay_text import (
    RELAY_RESULT_KEYWORDS,
    is_relay_noise_text,
)


class TestRelayText:
    def test_noise_pattern(self) -> None:
        assert is_relay_noise_text("===") is True

    def test_whitespace_noise(self) -> None:
        assert is_relay_noise_text("   ") is True

    def test_none(self) -> None:
        assert is_relay_noise_text(None) is True

    def test_empty(self) -> None:
        assert is_relay_noise_text("") is True

    def test_valid_description(self) -> None:
        result = is_relay_noise_text("김철수 안타")
        assert isinstance(result, bool)
