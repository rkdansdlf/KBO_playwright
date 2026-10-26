"""Unit tests for auto_healer pure functions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.cli.auto_healer import _apply_heal_outcome


class TestApplyHealOutcome:
    def test_completed_when_detail_saved(self) -> None:
        mock_item = MagicMock()
        mock_item.detail_saved = True
        mock_item.failure_reason = None
        with (
            patch("src.cli.auto_healer.update_game_status") as mock_update,
            patch("src.cli.auto_healer._read_persisted_status", return_value="COMPLETED"),
        ):
            result = _apply_heal_outcome("G1", mock_item)
        assert result == "completed"
        mock_update.assert_not_called()

    def test_draw_is_audited_as_draw_not_as_completed(self) -> None:
        """The save path derives DRAW for a tie; the audit must repeat that value.

        Announcing COMPLETED here would both mislabel the game and invite a status
        write that turns a correct tie into a win.
        """
        mock_item = MagicMock()
        mock_item.detail_saved = True
        mock_item.failure_reason = None
        with (
            patch("src.cli.auto_healer.update_game_status") as mock_update,
            patch("src.cli.auto_healer._read_persisted_status", return_value="DRAW"),
            patch("src.cli.auto_healer._reconcile_and_audit") as mock_audit,
        ):
            result = _apply_heal_outcome("G1", mock_item, previous_status="SCHEDULED")
        assert result == "completed"
        mock_update.assert_not_called()
        assert mock_audit.call_args.args == ("G1", "DRAW", "score_saved")
        assert mock_audit.call_args.kwargs["raw_status"] == "SCHEDULED"

    def test_the_audit_raw_value_is_the_status_the_row_held(self) -> None:
        """A game found as UNRESOLVED must not be recorded as SCHEDULED -> ..."""
        mock_item = MagicMock()
        mock_item.detail_saved = True
        mock_item.failure_reason = None
        with (
            patch("src.cli.auto_healer._read_persisted_status", return_value="COMPLETED"),
            patch("src.cli.auto_healer._reconcile_and_audit") as mock_audit,
        ):
            _apply_heal_outcome("G1", mock_item, previous_status="UNRESOLVED_MISSING")
        assert mock_audit.call_args.kwargs["raw_status"] == "UNRESOLVED_MISSING"
        assert mock_audit.call_args.args[1] == "COMPLETED"

    def test_saved_detail_that_left_the_row_non_terminal_is_not_a_heal(self) -> None:
        """`detail_saved` alone is not evidence: the row is read back."""
        mock_item = MagicMock()
        mock_item.detail_saved = True
        mock_item.failure_reason = None
        with (
            patch("src.cli.auto_healer.update_game_status") as mock_update,
            patch("src.cli.auto_healer._read_persisted_status", return_value="SCHEDULED"),
            patch("src.cli.auto_healer._reconcile_and_audit") as mock_audit,
        ):
            result = _apply_heal_outcome("G1", mock_item, previous_status="SCHEDULED")
        assert result == "unresolved"
        mock_update.assert_not_called()
        mock_audit.assert_not_called()

    def test_an_unreadable_status_is_not_reported_as_a_heal(self) -> None:
        mock_item = MagicMock()
        mock_item.detail_saved = True
        mock_item.failure_reason = None
        with (
            patch("src.cli.auto_healer.update_game_status") as mock_update,
            patch("src.cli.auto_healer._read_persisted_status", return_value=None),
            patch("src.cli.auto_healer._reconcile_and_audit") as mock_audit,
        ):
            result = _apply_heal_outcome("G1", mock_item)
        assert result == "unresolved"
        mock_update.assert_not_called()
        mock_audit.assert_not_called()

    def test_cancelled_when_failure_reason(self) -> None:
        mock_item = MagicMock()
        mock_item.detail_saved = False
        mock_item.failure_reason = "cancelled"
        with (
            patch("src.cli.auto_healer.update_game_status") as mock_update,
            patch("src.cli.auto_healer._read_persisted_status", return_value="SCHEDULED"),
        ):
            result = _apply_heal_outcome("G1", mock_item)
            assert result == "cancelled"
            mock_update.assert_called_once()

    def test_unresolved_when_no_item(self) -> None:
        with (
            patch("src.cli.auto_healer.update_game_status") as mock_update,
            patch("src.cli.auto_healer._read_persisted_status", return_value="SCHEDULED"),
        ):
            result = _apply_heal_outcome("G1", None)
            assert result == "unresolved"
            mock_update.assert_called_once()

    def test_unresolved_with_other_reason(self) -> None:
        mock_item = MagicMock()
        mock_item.detail_saved = False
        mock_item.failure_reason = "other"
        with (
            patch("src.cli.auto_healer.update_game_status") as mock_update,
            patch("src.cli.auto_healer._read_persisted_status", return_value="SCHEDULED"),
        ):
            result = _apply_heal_outcome("G1", mock_item)
            assert result == "unresolved"
            mock_update.assert_called_once()

    def test_unresolved_with_missing_reason(self) -> None:
        mock_item = MagicMock()
        mock_item.detail_saved = False
        mock_item.failure_reason = None
        with (
            patch("src.cli.auto_healer.update_game_status") as mock_update,
            patch("src.cli.auto_healer._read_persisted_status", return_value="SCHEDULED"),
        ):
            result = _apply_heal_outcome("G1", mock_item)
            assert result == "unresolved"
            mock_update.assert_called_once()

    @pytest.mark.parametrize("failure_reason", ["cancelled", "missing", None])
    def test_past_season_guard_refusal_is_skipped(self, failure_reason) -> None:
        """Guard refusal (allow_historical=False) -> skipped, no audit, no status log."""
        mock_item = MagicMock()
        mock_item.detail_saved = False
        mock_item.failure_reason = failure_reason
        with (
            patch("src.cli.auto_healer.update_game_status", return_value=False) as mock_update,
            patch("src.cli.auto_healer._read_persisted_status", return_value="SCHEDULED"),
            patch("src.cli.auto_healer._reconcile_and_audit") as mock_audit,
        ):
            result = _apply_heal_outcome("20190601LGSS0", mock_item, allow_historical=False)
        assert result == "skipped_past_season"
        mock_update.assert_called_once()
        assert mock_update.call_args.kwargs["allow_historical"] is False
        mock_audit.assert_not_called()


class TestPastSeasonGuardIntegration:
    def test_past_season_game_unchanged_and_unaudited(self, monkeypatch) -> None:
        """Real guard on an in-memory DB: past-season row keeps its status, no audit row."""
        from contextlib import contextmanager
        from datetime import date

        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy.pool import StaticPool

        from src.cli.backfill import auto_healer as healer_mod
        from src.models.base import Base
        from src.models.game import Game
        from src.repositories import game_status

        engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
        Base.metadata.create_all(engine)
        factory = sessionmaker(bind=engine, expire_on_commit=False)

        @contextmanager
        def _session():
            s = factory()
            try:
                yield s
                s.commit()
            finally:
                s.close()

        with factory() as s:
            s.add(
                Game(
                    game_id="20190601LGSS0",
                    game_date=date(2019, 6, 1),
                    home_team="SS",
                    away_team="LG",
                    game_status="SCHEDULED",
                )
            )
            s.commit()

        monkeypatch.setattr(game_status, "get_db_session", _session)
        monkeypatch.setattr(game_status, "current_season_year", lambda today=None: 2026)
        audit = MagicMock()
        monkeypatch.setattr(healer_mod, "_reconcile_and_audit", audit)

        item = MagicMock(detail_saved=False, failure_reason="missing")
        assert healer_mod._apply_heal_outcome("20190601LGSS0", item, allow_historical=False) == "skipped_past_season"
        audit.assert_not_called()
        with factory() as s:
            assert s.query(Game).filter(Game.game_id == "20190601LGSS0").one().game_status == "SCHEDULED"


class TestLogHealerSummary:
    def test_dry_run_no_alert(self, caplog) -> None:
        from src.cli.auto_healer import _log_healer_summary

        _log_healer_summary({"completed": 5, "unresolved": 0}, dry_run=True)
        assert "Auto-Healer Summary" in caplog.text

    def test_success_alert(self) -> None:
        from src.cli.auto_healer import _log_healer_summary

        with patch("src.cli.auto_healer.apply_incidents") as mock_apply:
            _log_healer_summary({"completed": 5, "unresolved": 0}, dry_run=False)

        # A clean run resolves the unresolved incident instead of alerting.
        mock_apply.assert_called_once()
        assert mock_apply.call_args.args[0] == []
        assert mock_apply.call_args.kwargs["resolve_keys"] == ["auto_healer:unresolved"]

    def test_unresolved_alert(self) -> None:
        from src.cli.auto_healer import _log_healer_summary

        with patch("src.cli.auto_healer.apply_incidents") as mock_apply:
            _log_healer_summary({"completed": 3, "unresolved": 2}, dry_run=False)

        events = mock_apply.call_args.args[0]
        assert len(events) == 1
        assert events[0].incident_key == "auto_healer:unresolved"
        assert "2" in events[0].message
