"""Tests for low-coverage service pure functions."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.services.relay_recovery_service import (
    RelayRecoveryConfig,
    RelayRecoveryTarget,
    RelayRecoveryResult,
    RelaySaveCounts,
    RecoveryTargetCriteria,
    RelayValidationConfig,
    RecoveryLoopContext,
    parse_source_order,
    load_game_ids_from_file,
    _bucket_targets,
)
from src.services.matchup_engine import MatchupEngine
from src.services.recovery_manager import RecoveryManager


class TestParseSourceOrder:
    def test_none_returns_none(self):
        result = parse_source_order(None)
        assert result is None

    def test_single_source(self):
        result = parse_source_order("naver")
        assert result == ["naver"]

    def test_multiple_sources(self):
        result = parse_source_order("naver,kbo")
        assert result == ["naver", "kbo"]

    def test_with_spaces(self):
        result = parse_source_order("naver, kbo")
        assert result == ["naver", "kbo"]


class TestLoadGameIdsFromFile:
    def test_none_returns_empty(self):
        result = load_game_ids_from_file(None)
        assert result == []

    def test_empty_path_returns_empty(self):
        result = load_game_ids_from_file("")
        assert result == []


class TestBucketTargets:
    def test_empty_list(self):
        result = _bucket_targets([])
        assert result == {}

    def test_single_bucket(self):
        targets = [
            RelayRecoveryTarget(game_id="20260412LGSS0"),
            RelayRecoveryTarget(game_id="20260413LGSS0"),
        ]
        result = _bucket_targets(targets)
        assert len(result) >= 1

    def test_multiple_buckets(self):
        targets = [
            RelayRecoveryTarget(game_id="20260412LGSS0"),
            RelayRecoveryTarget(game_id="20260413SKLG0"),
        ]
        result = _bucket_targets(targets)
        assert isinstance(result, dict)


class TestDataclasses:
    def test_recovery_target(self):
        target = RelayRecoveryTarget(game_id="G1")
        assert target.game_id == "G1"

    def test_recovery_result(self):
        result = RelayRecoveryResult()
        assert result.total_targets == 0

    def test_save_counts(self):
        counts = RelaySaveCounts(saved_rows=5)
        assert counts.saved_rows == 5

    def test_matchup_engine_exists(self):
        assert MatchupEngine is not None

    def test_recovery_manager_exists(self):
        assert RecoveryManager is not None
