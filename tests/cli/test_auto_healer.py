from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.auto_healer import (
    _apply_heal_outcome,
    _find_inconsistent_games,
    _find_recovery_targets,
    _find_stuck_games,
    _log_healer_summary,
    main,
    run_healer_async,
)


class TestAutoHealer:
    def test_dry_run(self):
        with patch("src.cli.auto_healer.run_healer_async", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = 0
            result = main(["--dry-run"])
            assert result == 0

    def test_pbp_dry_run(self):
        with patch("src.cli.auto_healer.run_pbp_healer") as mock_pbp:
            mock_pbp.return_value = 0
            result = main(["--pbp", "--dry-run"])
            assert result == 0

    def test_pbp_with_game_id(self):
        with patch("src.cli.auto_healer.run_pbp_healer") as mock_pbp:
            mock_pbp.return_value = 0
            result = main(["--pbp", "--game-id", "20250401LGSS0"])
            assert result == 0


class TestFindStuckGames:
    def test_returns_empty_when_none(self):
        with patch("src.cli.auto_healer.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalars.return_value.all.return_value = []
            result = _find_stuck_games()
            assert result == []


class TestFindInconsistentGames:
    def test_returns_empty_when_none(self):
        with patch("src.cli.auto_healer.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalars.return_value.all.return_value = []
            result = _find_inconsistent_games()
            assert result == []


class TestApplyHealOutcome:
    def test_returns_status_from_resolution(self):
        with patch("src.cli.auto_healer._send_healer_start_alert"):
            result = _apply_heal_outcome("G1", None)
            assert isinstance(result, str)


class TestLogHealerSummary:
    def test_logs_summary(self, caplog):
        with caplog.at_level(logging.INFO):
            _log_healer_summary({"fixed": 5, "failed": 1, "completed": 5}, dry_run=True)
        assert "5" in caplog.text


class TestRunHealerAsync:
    async def test_returns_zero(self):
        with (
            patch("src.cli.auto_healer._find_stuck_games") as mock_stuck,
            patch("src.cli.auto_healer._find_inconsistent_games") as mock_incon,
            patch("src.cli.auto_healer._find_recovery_targets") as mock_targets,
        ):
            mock_stuck.return_value = []
            mock_incon.return_value = []
            mock_targets.return_value = ([], [], [])
            result = await run_healer_async(dry_run=True)
            assert result == 0
