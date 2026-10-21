"""Auto-healer checkpoint must keep retrying recent stuck games (issue #55)."""

from __future__ import annotations

import asyncio
from datetime import date as _date
from unittest.mock import AsyncMock, MagicMock

import pytest

import src.cli.backfill.auto_healer as auto_healer_module
from src.cli.backfill.auto_healer import _pending_recovery_candidates, _recent_retry_ids
from src.services.recovery_manager import RecoveryManager


class TestRecentRetryIds:
    """Issue #55: recent stuck games are retried after an earlier failed pass."""

    def test_only_games_inside_window(self, monkeypatch):
        monkeypatch.delenv("HEALER_RECENT_RETRY_DAYS", raising=False)
        recent = MagicMock(game_id="20261020HTLG0", game_date=_date(2026, 10, 20))
        old = MagicMock(game_id="20260901LGHT0", game_date=_date(2026, 9, 1))

        assert _recent_retry_ids([recent, old], today=_date(2026, 10, 22)) == ["20261020HTLG0"]

    def test_window_env_override(self, monkeypatch):
        monkeypatch.setenv("HEALER_RECENT_RETRY_DAYS", "0")
        game = MagicMock(game_id="20261020HTLG0", game_date=_date(2026, 10, 20))

        assert _recent_retry_ids([game], today=_date(2026, 10, 22)) == []

    def test_checkpoint_failure_is_retried_end_to_end(self, tmp_path):
        mgr = RecoveryManager(str(tmp_path / "healer_checkpoint.json"))
        older = MagicMock(game_id="20261001LGHT0", game_date=_date(2026, 10, 1))
        mgr.initialize_run("default_healer_run", [older.game_id])
        mgr.mark_failed(older.game_id, "old")

        game = MagicMock(game_id="20261020HTLG0", game_date=_date(2026, 10, 20))
        pending, candidates = _pending_recovery_candidates(
            mgr,
            [older, game],
            retry_ids=_recent_retry_ids([older, game], today=_date(2026, 10, 21)),
        )
        assert pending == {"20261020HTLG0"}
        mgr.mark_failed(game.game_id, "fetch_404")

        pending, candidates = _pending_recovery_candidates(
            mgr,
            [older, game],
            retry_ids=_recent_retry_ids([older, game], today=_date(2026, 10, 22)),
        )
        assert pending == {"20261020HTLG0"}
        assert [c.game_id for c in candidates] == ["20261020HTLG0"]


class TestTargetedRunIgnoresCheckpointOutcome:
    """--game-id must re-crawl the named games whatever the checkpoint says (PR #56 review)."""

    @pytest.mark.parametrize("prior_outcome", ["failed", "completed"])
    def test_target_game_in_checkpoint_is_recovered(self, tmp_path, monkeypatch, prior_outcome):
        checkpoint = tmp_path / "healer_checkpoint.json"
        mgr = RecoveryManager(str(checkpoint))
        mgr.initialize_run("default_healer_run", ["20261020HTLG0", "20260901LGHT0"])
        if prior_outcome == "failed":
            mgr.mark_failed("20261020HTLG0", "fetch_404")
        else:
            mgr.mark_completed("20261020HTLG0")
        mgr.mark_failed("20260901LGHT0", "old")

        game = MagicMock(game_id="20261020HTLG0", game_date=_date(2026, 10, 20))
        recovery = AsyncMock(return_value={"completed": 0, "cancelled": 0, "unresolved": 0, "dry_run": 1})
        monkeypatch.setattr(auto_healer_module, "RecoveryManager", lambda: RecoveryManager(str(checkpoint)))
        monkeypatch.setattr(
            auto_healer_module,
            "_find_recovery_targets",
            MagicMock(return_value=([game], [], [], [])),
        )
        monkeypatch.setattr(auto_healer_module, "_log_anomaly_summary", MagicMock())
        monkeypatch.setattr(auto_healer_module, "_log_healer_summary", MagicMock())
        monkeypatch.setattr(auto_healer_module, "_run_recovery", recovery)

        asyncio.run(auto_healer_module.run_healer_async(dry_run=True, target_game_ids=["20261020HTLG0"]))

        recovery.assert_awaited_once()
        candidates = recovery.await_args.args[0]
        assert [c.game_id for c in candidates] == ["20261020HTLG0"]
        # A targeted run must leave the rest of the automatic checkpoint alone.
        reloaded = RecoveryManager(str(checkpoint))
        assert reloaded.state["failed"] == {"20260901LGHT0": "old"}


class TestAutomaticRunPrunesResolvedGames:
    def test_games_no_longer_anomalous_leave_checkpoint(self, tmp_path):
        mgr = RecoveryManager(str(tmp_path / "healer_checkpoint.json"))
        mgr.initialize_run("default_healer_run", ["20261001LGHT0", "20261002LGHT0"])
        mgr.mark_failed("20261001LGHT0", "old")
        mgr.mark_completed("20261002LGHT0")

        still_stuck = MagicMock(game_id="20261001LGHT0", game_date=_date(2026, 10, 1))
        recent = MagicMock(game_id="20261020HTLG0", game_date=_date(2026, 10, 20))
        pending, _ = _pending_recovery_candidates(
            mgr,
            [still_stuck, recent],
            retry_ids=_recent_retry_ids([still_stuck, recent], today=_date(2026, 10, 22)),
            prune_missing=True,
        )

        assert pending == {"20261020HTLG0"}
        assert mgr.state["completed"] == []
        # An old failure that is still an anomaly stays failed and is not re-crawled.
        assert mgr.state["failed"] == {"20261001LGHT0": "old"}
