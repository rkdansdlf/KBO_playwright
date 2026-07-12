from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from src.cli.dashboard import generate_dashboard


def test_generate_dashboard_success(tmp_path):
    # Mock SessionLocal to return simulated SLA metrics
    mock_metric = MagicMock()
    mock_metric.id = 1
    mock_metric.check_time = None
    mock_metric.category = "game"
    mock_metric.sla_threshold_hours = 3
    mock_metric.actual_delay_hours = 1.5
    mock_metric.is_violation = False
    mock_metric.notes = "No issues"

    # We will patch reports directory path to point to tmp_path
    with (
        patch("src.cli.dashboard.SessionLocal") as mock_session_factory,
        patch("src.cli.dashboard.Path") as mock_path_class,
    ):
        # Configure session mock to return our metrics list
        mock_session = mock_session_factory.return_value.__enter__.return_value
        mock_session.query.return_value.order_by.return_value.limit.return_value.all.return_value = [mock_metric]

        # Configure mock_path to save inside tmp_path
        mock_dest = mock_path_class.return_value
        mock_file = mock_dest.__truediv__.return_value

        generate_dashboard()

        # Verify query was called
        mock_session.query.assert_called_once()
        # Verify HTML was written
        mock_file.write_text.assert_called_once()
