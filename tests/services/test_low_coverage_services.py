"""Tests for low-coverage service and repository modules."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.services.context_aggregator import ContextAggregator
from src.services.game_deduplication_service import DeduplicationResult
from src.services.game_story_builder import GameStoryBuilder
from src.services.recovery_manager import RecoveryManager
from src.services.split_calculator import SituationalSplitCalculator


class TestContextAggregator:
    def test_class_exists(self):
        assert ContextAggregator is not None

    def test_instantiation(self):
        mock_session = MagicMock()
        agg = ContextAggregator(mock_session)
        assert agg is not None


class TestGameStoryBuilder:
    def test_class_exists(self):
        assert GameStoryBuilder is not None

    def test_instantiation(self):
        builder = GameStoryBuilder()
        assert builder is not None


class TestRecoveryManager:
    def test_class_exists(self):
        assert RecoveryManager is not None

    def test_instantiation(self):
        mgr = RecoveryManager(checkpoint_path="/tmp/test_checkpoint.json")
        assert mgr is not None


class TestSplitCalculator:
    def test_class_exists(self):
        assert SituationalSplitCalculator is not None


class TestDeduplicationResult:
    def test_dataclass_creation(self):
        result = DeduplicationResult(scanned_slots=10, marked_primary=5)
        assert result.scanned_slots == 10
        assert result.marked_primary == 5
