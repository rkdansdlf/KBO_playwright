"""Unit tests for batch_calculate_sabermetrics error paths."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.cli.calculate_sabermetrics import (
    SABERMETRICS_CALC_EXCEPTIONS,
    batch_calculate_sabermetrics,
)


class TestBatchCalculateSabermetrics:
    def test_league_constants_exception_continues(self, caplog: pytest.LogCaptureFixture) -> None:
        """When get_league_constants raises, the loop continues to next year."""
        mock_session = MagicMock()
        with (
            patch("src.cli.calculate_sabermetrics.SessionLocal") as mock_factory,
            patch(
                "src.cli.calculate_sabermetrics.SabermetricsCalculator.get_league_constants",
                side_effect=SQLAlchemyError("no data"),
            ),
        ):
            mock_factory.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_factory.return_value.__exit__ = MagicMock(return_value=False)
            with caplog.at_level(logging.ERROR):
                batch_calculate_sabermetrics([2024, 2025])
        assert any("Could not calculate league constants" in record.message for record in caplog.records)
