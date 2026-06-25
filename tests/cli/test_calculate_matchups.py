from __future__ import annotations

import logging
import os
from unittest.mock import patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.cli.calculate_matchups import batch_calculate_matchups, main


class TestBatchCalculateMatchups:
    def test_executes_all_years(self):
        with patch("src.cli.calculate_matchups.MatchupEngine") as mock_engine_cls:
            mock_engine = mock_engine_cls.return_value
            batch_calculate_matchups([2023, 2024, 2025])
            assert mock_engine.execute_all.call_count == 3
            mock_engine.execute_all.assert_any_call(2023)
            mock_engine.execute_all.assert_any_call(2024)
            mock_engine.execute_all.assert_any_call(2025)

    def test_exception_caught_and_logged(self, caplog):
        with patch("src.cli.calculate_matchups.MatchupEngine") as mock_engine_cls:
            mock_engine = mock_engine_cls.return_value
            mock_engine.execute_all.side_effect = SQLAlchemyError("DB error")
            with caplog.at_level(logging.ERROR):
                batch_calculate_matchups([2025])
            assert "Failed to calculate matchups for 2025" in caplog.text

    def test_runtime_error_caught(self, caplog):
        with patch("src.cli.calculate_matchups.MatchupEngine") as mock_engine_cls:
            mock_engine = mock_engine_cls.return_value
            mock_engine.execute_all.side_effect = RuntimeError("unexpected")
            with caplog.at_level(logging.ERROR):
                batch_calculate_matchups([2025])
            assert "Failed to calculate matchups" in caplog.text

    def test_sync_oci_without_env_var(self, caplog):
        with (
            patch("src.cli.calculate_matchups.MatchupEngine") as mock_engine_cls,
            patch.dict(os.environ, {}, clear=True),
        ):
            mock_engine_cls.return_value.execute_all.return_value = None
            with caplog.at_level(logging.WARNING):
                batch_calculate_matchups([2025], sync_oci=True)
            assert "OCI_DB_URL not set" in caplog.text

    def test_sync_oci_with_env_var(self):
        with (
            patch("src.cli.calculate_matchups.MatchupEngine") as mock_engine_cls,
            patch("src.cli.sync_oci.OCISync") as mock_sync_cls,
            patch("src.cli.calculate_matchups.SessionLocal") as mock_session,
            patch.dict(os.environ, {"OCI_DB_URL": "postgresql://test"}),
        ):
            mock_engine_cls.return_value.execute_all.return_value = None
            mock_session.return_value.__enter__ = lambda s: s
            mock_session.return_value.__exit__ = lambda s, *a: False
            batch_calculate_matchups([2025], sync_oci=True)
            mock_sync_cls.return_value.sync_matchups.assert_called_once()

    def test_empty_years_list(self):
        with patch("src.cli.calculate_matchups.MatchupEngine") as mock_engine_cls:
            batch_calculate_matchups([])
            mock_engine_cls.return_value.execute_all.assert_not_called()


class TestCalculateMatchupsCLI:
    def test_main_default_years(self):
        with patch("src.cli.calculate_matchups.batch_calculate_matchups") as mock_batch:
            result = main([])
            assert result == 0
            mock_batch.assert_called_once_with(list(range(2020, 2027)), sync_oci=False)

    def test_main_custom_year(self):
        with patch("src.cli.calculate_matchups.batch_calculate_matchups") as mock_batch:
            result = main(["--years", "2025"])
            assert result == 0
            mock_batch.assert_called_once_with([2025], sync_oci=False)

    def test_main_custom_range(self):
        with patch("src.cli.calculate_matchups.batch_calculate_matchups") as mock_batch:
            result = main(["--years", "2023-2025"])
            assert result == 0
            mock_batch.assert_called_once_with([2023, 2024, 2025], sync_oci=False)

    def test_main_with_sync(self):
        with patch("src.cli.calculate_matchups.batch_calculate_matchups") as mock_batch:
            result = main(["--sync"])
            assert result == 0
            mock_batch.assert_called_once_with(list(range(2020, 2027)), sync_oci=True)

    def test_main_single_year_no_range(self):
        with patch("src.cli.calculate_matchups.batch_calculate_matchups") as mock_batch:
            result = main(["--years", "2020"])
            assert result == 0
            mock_batch.assert_called_once_with([2020], sync_oci=False)
