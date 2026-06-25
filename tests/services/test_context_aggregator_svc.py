"""Tests for context_aggregator service."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.services.context_aggregator import ContextAggregator, _classify_final_payload


class TestClassifyFinalPayload:
    def test_ok(self) -> None:
        final_payload = {"found": True, "has_pitching": True, "starter_rows": 1, "bullpen_rows": 0}
        raw = {"bullpen_rows": 0}
        assert _classify_final_payload(final_payload, raw) == "ok"

    def test_missing_payload(self) -> None:
        final_payload = {"found": False, "has_pitching": True, "starter_rows": 0, "bullpen_rows": 0}
        raw = {"bullpen_rows": 0}
        assert _classify_final_payload(final_payload, raw) == "final_review_payload_missing"

    def test_missing_pitching(self) -> None:
        final_payload = {"found": True, "has_pitching": False, "starter_rows": 0, "bullpen_rows": 0}
        raw = {"bullpen_rows": 0}
        assert _classify_final_payload(final_payload, raw) == "final_review_payload_missing_pitching"

    def test_pitching_empty(self) -> None:
        final_payload = {"found": True, "has_pitching": True, "starter_rows": 0, "bullpen_rows": 0}
        raw = {"bullpen_rows": 1}
        assert _classify_final_payload(final_payload, raw) == "final_review_payload_pitching_empty"


class TestPitchingOutsFromValue:
    def test_none_input(self) -> None:
        assert ContextAggregator._pitching_outs_from_value(None) is None

    def test_whole_innings(self) -> None:
        assert ContextAggregator._pitching_outs_from_value(3.0) == 9

    def test_one_third(self) -> None:
        assert ContextAggregator._pitching_outs_from_value(3.1) == 10

    def test_two_thirds(self) -> None:
        assert ContextAggregator._pitching_outs_from_value(3.2) == 11

    def test_invalid_string(self) -> None:
        assert ContextAggregator._pitching_outs_from_value("invalid") is None


class TestInningsDisplayFromOuts:
    def test_zero_outs(self) -> None:
        assert ContextAggregator._innings_display_from_outs(0) == "0.0"

    def test_nine_outs(self) -> None:
        assert ContextAggregator._innings_display_from_outs(9) == "3.0"

    def test_ten_outs(self) -> None:
        assert ContextAggregator._innings_display_from_outs(10) == "3.1"

    def test_eleven_outs(self) -> None:
        assert ContextAggregator._innings_display_from_outs(11) == "3.2"
