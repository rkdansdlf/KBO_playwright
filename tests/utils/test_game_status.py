from datetime import date

from src.utils.game_status import (
    ALL_GAME_STATUSES,
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DRAW,
    GAME_STATUS_LIVE,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_UNRESOLVED,
    LIVE_GAME_STATUSES,
    TERMINAL_GAME_STATUSES,
    completed_like_statuses,
    derive_stable_game_status,
    is_completed_like_status,
    is_live_status,
    is_terminal_status,
    normalize_game_status,
)


class TestStatusHelpers:
    def test_is_terminal_status(self):
        assert is_terminal_status("COMPLETED") is True
        assert is_terminal_status("DRAW") is True
        assert is_terminal_status("CANCELLED") is True
        assert is_terminal_status("LIVE") is False
        assert is_terminal_status(None) is False

    def test_is_completed_like_status(self):
        assert is_completed_like_status("COMPLETED") is True
        assert is_completed_like_status("DRAW") is True
        assert is_completed_like_status("CANCELLED") is False

    def test_is_live_status(self):
        assert is_live_status("LIVE") is True
        assert is_live_status("DELAYED") is True
        assert is_live_status("COMPLETED") is False
        assert is_live_status(None) is False

    def test_normalize_game_status(self):
        assert normalize_game_status(None) is None
        assert normalize_game_status("LIVE") == "LIVE"
        assert normalize_game_status(" cancel ") == "CANCELLED"
        assert normalize_game_status("CANCELED") == "CANCELLED"
        assert normalize_game_status("unknown") is None

    def test_completed_like_statuses(self):
        result = completed_like_statuses()
        assert "COMPLETED" in result
        assert "DRAW" in result

    def test_constants_are_consistent(self):
        assert GAME_STATUS_LIVE in ALL_GAME_STATUSES
        assert GAME_STATUS_COMPLETED in ALL_GAME_STATUSES
        assert GAME_STATUS_SCHEDULED in ALL_GAME_STATUSES
        assert GAME_STATUS_UNRESOLVED in ALL_GAME_STATUSES
        assert set() == LIVE_GAME_STATUSES & TERMINAL_GAME_STATUSES


class TestDeriveStableGameStatus:
    def test_future_is_scheduled(self):
        today = date(2026, 5, 14)
        game_date = date(2026, 5, 15)
        assert derive_stable_game_status(game_date=game_date, today=today) == GAME_STATUS_SCHEDULED

    def test_today_no_evidence_scheduled(self):
        today = date(2026, 5, 14)
        assert derive_stable_game_status(game_date=today, has_progress_evidence=False, today=today) == GAME_STATUS_SCHEDULED

    def test_today_with_evidence_live(self):
        today = date(2026, 5, 14)
        assert derive_stable_game_status(game_date=today, has_progress_evidence=True, today=today) == GAME_STATUS_LIVE

    def test_scores_induce_draw(self):
        today = date(2026, 5, 14)
        assert derive_stable_game_status(game_date=today, home_score=3, away_score=3, today=today) == GAME_STATUS_DRAW

    def test_scores_induce_completed(self):
        today = date(2026, 5, 14)
        assert derive_stable_game_status(game_date=today, home_score=5, away_score=3, today=today) == GAME_STATUS_COMPLETED

    def test_terminal_current_not_overridden(self):
        today = date(2026, 5, 14)
        status = derive_stable_game_status(
            game_date=today, current_status="COMPLETED", new_status="LIVE",
            home_score=5, away_score=3, today=today,
        )
        assert status == GAME_STATUS_COMPLETED

    def test_past_no_scores_unresolved(self):
        today = date(2026, 5, 14)
        game_date = date(2026, 5, 13)
        assert derive_stable_game_status(game_date=game_date, today=today) == GAME_STATUS_UNRESOLVED

    def test_past_with_terminal_status_preserved(self):
        today = date(2026, 5, 14)
        game_date = date(2026, 5, 13)
        assert derive_stable_game_status(game_date=game_date, current_status="COMPLETED", today=today) == GAME_STATUS_COMPLETED

    def test_past_with_new_terminal_status(self):
        today = date(2026, 5, 14)
        game_date = date(2026, 5, 13)
        assert derive_stable_game_status(game_date=game_date, new_status="CANCELLED", today=today) == GAME_STATUS_CANCELLED
