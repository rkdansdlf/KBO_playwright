"""Auto-healer checkpoint must keep retrying recent stuck games (issue #55)."""

from __future__ import annotations

from datetime import date as _date
from unittest.mock import MagicMock

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
