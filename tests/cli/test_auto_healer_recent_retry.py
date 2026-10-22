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


def _seed_checkpoint(checkpoint, prior_outcome: str) -> None:
    mgr = RecoveryManager(str(checkpoint))
    mgr.initialize_run("default_healer_run", ["20261020HTLG0", "20260901LGHT0"])
    if prior_outcome == "failed":
        mgr.mark_failed("20261020HTLG0", "fetch_404")
    else:
        mgr.mark_completed("20261020HTLG0")
    mgr.mark_failed("20260901LGHT0", "old")


def _patch_healer(monkeypatch, checkpoint, all_found, stuck):
    recovery = AsyncMock(return_value={"completed": 0, "cancelled": 0, "unresolved": 0, "dry_run": 1})
    monkeypatch.setattr(
        auto_healer_module,
        "RecoveryManager",
        lambda **kwargs: RecoveryManager(str(checkpoint), **kwargs),
    )
    monkeypatch.setattr(
        auto_healer_module,
        "_find_recovery_targets",
        MagicMock(return_value=(all_found, stuck, [], [])),
    )
    monkeypatch.setattr(auto_healer_module, "_log_anomaly_summary", MagicMock())
    monkeypatch.setattr(auto_healer_module, "_log_healer_summary", MagicMock())
    monkeypatch.setattr(auto_healer_module, "_send_healer_start_alert", MagicMock())
    monkeypatch.setattr(auto_healer_module, "_run_recovery", recovery)
    return recovery


def _snapshot(path):
    return (path.read_bytes(), path.stat().st_mtime_ns) if path.exists() else None


class TestTargetedRunIgnoresCheckpointOutcome:
    """--game-id must re-crawl the named games whatever the checkpoint says (PR #56 review)."""

    @pytest.mark.parametrize("prior_outcome", ["failed", "completed"])
    def test_target_game_in_checkpoint_is_recovered(self, tmp_path, monkeypatch, prior_outcome):
        checkpoint = tmp_path / "healer_checkpoint.json"
        _seed_checkpoint(checkpoint, prior_outcome)
        game = MagicMock(game_id="20261020HTLG0", game_date=_date(2026, 10, 20))
        recovery = _patch_healer(monkeypatch, checkpoint, [game], [])

        asyncio.run(auto_healer_module.run_healer_async(dry_run=False, target_game_ids=["20261020HTLG0"]))

        recovery.assert_awaited_once()
        candidates = recovery.await_args.args[0]
        assert [c.game_id for c in candidates] == ["20261020HTLG0"]
        # A targeted run reopens only the named game and leaves the rest of the
        # automatic checkpoint alone.
        reloaded = RecoveryManager(str(checkpoint))
        assert reloaded.state["failed"] == {"20260901LGHT0": "old"}
        assert "20261020HTLG0" in reloaded.get_pending_targets()


class TestDryRunNeverWritesCheckpoint:
    """PR #56 review 2/3: --dry-run must leave the checkpoint file untouched."""

    @pytest.mark.parametrize("reset", [False, True])
    @pytest.mark.parametrize("prior_outcome", ["failed", "completed"])
    def test_targeted_dry_run(self, tmp_path, monkeypatch, prior_outcome, reset):
        checkpoint = tmp_path / "healer_checkpoint.json"
        _seed_checkpoint(checkpoint, prior_outcome)
        before = _snapshot(checkpoint)
        game = MagicMock(game_id="20261020HTLG0", game_date=_date(2026, 10, 20))
        recovery = _patch_healer(monkeypatch, checkpoint, [game], [])

        asyncio.run(
            auto_healer_module.run_healer_async(
                dry_run=True,
                reset_checkpoint=reset,
                target_game_ids=["20261020HTLG0"],
            ),
        )

        # The dry run still reports the game it would re-crawl ...
        assert [c.game_id for c in recovery.await_args.args[0]] == ["20261020HTLG0"]
        # ... but the checkpoint file is byte-for-byte what it was.
        assert _snapshot(checkpoint) == before

    @pytest.mark.parametrize("reset", [False, True])
    def test_automatic_dry_run(self, tmp_path, monkeypatch, reset):
        monkeypatch.delenv("HEALER_RECENT_RETRY_DAYS", raising=False)
        checkpoint = tmp_path / "healer_checkpoint.json"
        mgr = RecoveryManager(str(checkpoint))
        mgr.initialize_run("default_healer_run", ["20261001LGHT0", "20261002LGHT0", "20261019HTLG0"])
        mgr.mark_failed("20261001LGHT0", "old")
        mgr.mark_completed("20261002LGHT0")
        mgr.mark_failed("20261019HTLG0", "fetch_404")
        before = _snapshot(checkpoint)

        today = auto_healer_module.datetime.now(auto_healer_module.KST).date()
        recent = MagicMock(game_id="20261019HTLG0", game_date=today)
        new_game = MagicMock(game_id="20261020HTLG0", game_date=today)
        old = MagicMock(game_id="20261001LGHT0", game_date=_date(2026, 10, 1))
        recovery = _patch_healer(monkeypatch, checkpoint, [old, recent, new_game], [old, recent, new_game])

        asyncio.run(auto_healer_module.run_healer_async(dry_run=True, reset_checkpoint=reset))

        # Retry, merge and prune are all computed (pruning drops 20261002LGHT0,
        # the recent failure is retried, the new game is queued) ...
        candidate_ids = sorted(c.game_id for c in recovery.await_args.args[0])
        expected = ["20261001LGHT0", "20261019HTLG0", "20261020HTLG0"] if reset else ["20261019HTLG0", "20261020HTLG0"]
        assert candidate_ids == expected
        # ... in memory only.
        assert _snapshot(checkpoint) == before

    def test_dry_run_with_no_anomalies_keeps_checkpoint(self, tmp_path, monkeypatch):
        checkpoint = tmp_path / "healer_checkpoint.json"
        _seed_checkpoint(checkpoint, "failed")
        before = _snapshot(checkpoint)
        _patch_healer(monkeypatch, checkpoint, [], [])
        monkeypatch.setattr(auto_healer_module, "reconcile_pending_quarantines", MagicMock(return_value=0))

        assert asyncio.run(auto_healer_module.run_healer_async(dry_run=True)) == 0
        assert _snapshot(checkpoint) == before

    def test_dry_run_without_checkpoint_creates_nothing(self, tmp_path, monkeypatch):
        checkpoint = tmp_path / "recovery" / "healer_checkpoint.json"
        game = MagicMock(game_id="20261020HTLG0", game_date=_date(2026, 10, 20))
        recovery = _patch_healer(monkeypatch, checkpoint, [game], [])

        asyncio.run(auto_healer_module.run_healer_async(dry_run=True, target_game_ids=["20261020HTLG0"]))

        recovery.assert_awaited_once()
        assert not checkpoint.parent.exists()


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


class TestGameIdWithoutPaFormulaWarns:
    def test_warns_that_game_id_is_ignored(self, monkeypatch, caplog):
        run = AsyncMock(return_value=0)
        monkeypatch.setattr(auto_healer_module, "run_healer_async", run)
        monkeypatch.setattr(auto_healer_module, "heal_season_stat_discrepancies", MagicMock(return_value=0))

        with caplog.at_level("WARNING", logger=auto_healer_module.logger.name):
            auto_healer_module.run_healer(["--game-id", "20261020HTLG0", "--dry-run"])

        assert "--game-id is ignored" in caplog.text
        assert "target_game_ids" not in run.await_args.kwargs
