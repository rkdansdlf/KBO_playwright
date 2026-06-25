from __future__ import annotations

from datetime import date

import pytest

from src.utils.game_status import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DELAYED,
    GAME_STATUS_DRAW,
    GAME_STATUS_LIVE,
    GAME_STATUS_POSTPONED,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_SUSPENDED,
    GAME_STATUS_UNRESOLVED,
    GameStatusEvidence,
    derive_stable_game_status,
    is_completed_like_status,
    is_live_status,
    is_terminal_status,
    normalize_game_status,
)


class TestIsTerminalStatus:
    def test_none(self):
        assert is_terminal_status(None) is False

    def test_empty_string(self):
        assert is_terminal_status("") is False

    def test_lowercase(self):
        assert is_terminal_status("completed") is True

    def test_all_terminal(self):
        for status in ("COMPLETED", "DRAW", "CANCELLED", "POSTPONED"):
            assert is_terminal_status(status) is True

    def test_non_terminal(self):
        for status in ("SCHEDULED", "LIVE", "DELAYED", "SUSPENDED"):
            assert is_terminal_status(status) is False


class TestIsCompletedLikeStatus:
    def test_completed(self):
        assert is_completed_like_status("COMPLETED") is True

    def test_draw(self):
        assert is_completed_like_status("DRAW") is True

    def test_cancelled_not_completed_like(self):
        assert is_completed_like_status("CANCELLED") is False

    def test_none(self):
        assert is_completed_like_status(None) is False


class TestIsLiveStatus:
    def test_live(self):
        assert is_live_status("LIVE") is True

    def test_delayed(self):
        assert is_live_status("DELAYED") is True

    def test_suspended(self):
        assert is_live_status("SUSPENDED") is True

    def test_scheduled_not_live(self):
        assert is_live_status("SCHEDULED") is False

    def test_none(self):
        assert is_live_status(None) is False


class TestNormalizeGameStatus:
    def test_none_returns_none(self):
        assert normalize_game_status(None) is None

    def test_valid_status(self):
        assert normalize_game_status("LIVE") == "LIVE"

    def test_strip_whitespace(self):
        assert normalize_game_status("  live  ") == "LIVE"

    def test_canceled_alias(self):
        assert normalize_game_status("CANCELED") == "CANCELLED"

    def test_cancel_alias(self):
        assert normalize_game_status("CANCEL") == "CANCELLED"

    def test_cancelled_game_alias(self):
        assert normalize_game_status("CANCELLED_GAME") == "CANCELLED"

    def test_unknown_returns_none(self):
        assert normalize_game_status("INVALID_STATUS") is None

    def test_lowercase_input(self):
        assert normalize_game_status("completed") == "COMPLETED"


class TestGameStatusEvidence:
    def test_default_values(self):
        ev = GameStatusEvidence(game_date=date(2026, 6, 25))
        assert ev.current_status is None
        assert ev.new_status is None
        assert ev.home_score is None
        assert ev.away_score is None
        assert ev.has_progress_evidence is False
        assert ev.today is None

    def test_frozen(self):
        ev = GameStatusEvidence(game_date=date(2026, 6, 25))
        with pytest.raises(AttributeError):
            ev.game_date = date(2026, 6, 26)


class TestDeriveStableGameStatusEdgeCases:
    def test_both_none_scores_no_evidence_today(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=today,
            current_status=None,
            new_status=None,
            home_score=None,
            away_score=None,
            has_progress_evidence=False,
            today=today,
        )
        assert result == GAME_STATUS_SCHEDULED

    def test_today_with_live_new_status_no_evidence(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=today,
            new_status="LIVE",
            has_progress_evidence=False,
            today=today,
        )
        assert result == GAME_STATUS_SCHEDULED

    def test_today_with_live_new_status_and_evidence(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=today,
            new_status="LIVE",
            has_progress_evidence=True,
            today=today,
        )
        assert result == GAME_STATUS_LIVE

    def test_today_with_delayed_new_status_and_evidence(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=today,
            new_status="DELAYED",
            has_progress_evidence=True,
            today=today,
        )
        assert result == GAME_STATUS_DELAYED

    def test_scores_equal_draw(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=today,
            home_score=5,
            away_score=5,
            today=today,
        )
        assert result == GAME_STATUS_DRAW

    def test_scores_unequal_completed(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=today,
            home_score=10,
            away_score=3,
            today=today,
        )
        assert result == GAME_STATUS_COMPLETED

    def test_terminal_current_not_overridden_by_scores(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=today,
            current_status="COMPLETED",
            new_status="LIVE",
            home_score=3,
            away_score=3,
            today=today,
        )
        assert result == GAME_STATUS_COMPLETED

    def test_past_no_statuses_unresolved(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=date(2026, 6, 24),
            today=today,
        )
        assert result == GAME_STATUS_UNRESOLVED

    def test_past_with_current_terminal(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=date(2026, 6, 24),
            current_status="COMPLETED",
            today=today,
        )
        assert result == GAME_STATUS_COMPLETED

    def test_past_with_new_terminal(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=date(2026, 6, 24),
            new_status="CANCELLED",
            today=today,
        )
        assert result == GAME_STATUS_CANCELLED

    def test_past_with_new_completed(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=date(2026, 6, 24),
            new_status="POSTPONED",
            today=today,
        )
        assert result == GAME_STATUS_POSTPONED

    def test_past_both_none_unresolved(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=date(2026, 6, 24),
            current_status=None,
            new_status=None,
            today=today,
        )
        assert result == GAME_STATUS_UNRESOLVED

    def test_future_game_scheduled(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=date(2026, 12, 31),
            today=today,
        )
        assert result == GAME_STATUS_SCHEDULED

    def test_evidence_from_kwargs(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=today,
            has_progress_evidence=True,
            today=today,
        )
        assert result == GAME_STATUS_LIVE

    def test_both_evidence_and_kwargs_raises(self):
        with pytest.raises(TypeError, match="Pass either GameStatusEvidence or keyword"):
            derive_stable_game_status(
                GameStatusEvidence(game_date=date(2026, 6, 25)),
                today=date(2026, 6, 25),
            )

    def test_resolve_scored_status_terminal_current_keeps(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=today,
            current_status="COMPLETED",
            new_status="SCHEDULED",
            home_score=3,
            away_score=1,
            today=today,
        )
        assert result == GAME_STATUS_COMPLETED

    def test_resolve_scored_status_non_terminal_returns_completed(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=today,
            current_status="LIVE",
            new_status="LIVE",
            home_score=3,
            away_score=1,
            today=today,
        )
        assert result == GAME_STATUS_COMPLETED

    def test_resolve_past_status_both_none(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=date(2026, 6, 20),
            current_status=None,
            new_status=None,
            today=today,
        )
        assert result == GAME_STATUS_UNRESOLVED

    def test_resolve_past_status_current_terminal(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=date(2026, 6, 20),
            current_status="DRAW",
            new_status=None,
            today=today,
        )
        assert result == GAME_STATUS_DRAW

    def test_resolve_past_status_new_terminal(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=date(2026, 6, 20),
            current_status=None,
            new_status="COMPLETED",
            today=today,
        )
        assert result == GAME_STATUS_COMPLETED

    def test_resolve_past_status_current_non_terminal_new_none(self):
        today = date(2026, 6, 25)
        result = derive_stable_game_status(
            game_date=date(2026, 6, 20),
            current_status="LIVE",
            new_status=None,
            today=today,
        )
        assert result == GAME_STATUS_UNRESOLVED
