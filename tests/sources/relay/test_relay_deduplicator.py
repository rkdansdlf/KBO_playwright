from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

from src.sources.relay.circuit_breaker import SourceCircuitBreaker
from src.sources.relay.relay_deduplicator import RelayDeduplicator


class TestRelayDeduplicator:
    def test_deduplicator_sliding_window(self):
        deduper = RelayDeduplicator(window_size=3)

        events1 = [
            {"provider_log_id": "event_1", "text": "hit"},
            {"provider_log_id": "event_2", "text": "out"},
        ]
        # First crawl: all events should be new
        res1 = deduper.filter_new_events(events1)
        assert len(res1) == 2
        assert res1[0]["provider_log_id"] == "event_1"

        # Second crawl: duplicates should be filtered out
        res2 = deduper.filter_new_events(events1)
        assert len(res2) == 0

        # Third crawl: add new events that exceed window size
        events2 = [
            {"provider_log_id": "event_2", "text": "out"},  # duplicate (still in window)
            {"provider_log_id": "event_3", "text": "walk"},  # new
            {"provider_log_id": "event_4", "text": "strikeout"},  # new
        ]
        res3 = deduper.filter_new_events(events2)
        # event_3, event_4 are new. event_2 is skipped.
        # seen ids now: event_2, event_3, event_4 (window size 3 reached, event_1 dropped)
        assert len(res3) == 2
        assert [e["provider_log_id"] for e in res3] == ["event_3", "event_4"]

        # Fourth crawl: event_1 should be treated as new again since it fell out of the window
        events3 = [{"provider_log_id": "event_1", "text": "hit"}]
        res4 = deduper.filter_new_events(events3)
        assert len(res4) == 1
        assert res4[0]["provider_log_id"] == "event_1"

    def test_deduplicator_fallback_hash(self):
        deduper = RelayDeduplicator(window_size=5)

        # Event with missing provider_log_id should have fallback hash computed
        event_no_id = {"inning": 1, "source_row_index": 1, "play_description": "home run"}
        res1 = deduper.filter_new_events([event_no_id])
        assert len(res1) == 1

        # Identical event should be filtered out
        res2 = deduper.filter_new_events([event_no_id])
        assert len(res2) == 0


class TestCircuitBreakerSettings:
    def test_circuit_breaker_env_var_defaults(self, monkeypatch):
        monkeypatch.setenv("RELAY_BREAKER_THRESHOLD", "5")
        monkeypatch.setenv("RELAY_BREAKER_COOLDOWN", "120.0")

        # Create fresh breaker to load env vars
        cb = SourceCircuitBreaker()
        assert cb._threshold == 5
        assert cb._cooldown == 120.0

    def test_circuit_breaker_parameter_overrides(self, monkeypatch):
        monkeypatch.setenv("RELAY_BREAKER_THRESHOLD", "5")
        monkeypatch.setenv("RELAY_BREAKER_COOLDOWN", "120.0")

        # Explicit params override environment variables
        cb = SourceCircuitBreaker(threshold=2, cooldown_seconds=30.0)
        assert cb._threshold == 2
        assert cb._cooldown == 30.0
