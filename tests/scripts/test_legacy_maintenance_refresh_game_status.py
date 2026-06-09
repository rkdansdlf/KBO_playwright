from datetime import date

from scripts.legacy.maintenance.refresh_game_status import (
    derive_game_status,
    parse_date,
)


class TestParseDate:
    def test_date_object(self):
        assert parse_date(date(2025, 4, 1)) == date(2025, 4, 1)

    def test_string(self):
        assert parse_date("2025-04-01") == date(2025, 4, 1)

    def test_none(self):
        import pytest

        with pytest.raises(ValueError):
            parse_date(None)


class TestDeriveGameStatus:
    def test_completed_with_evidence(self):
        status = derive_game_status(
            game_date=date(2025, 4, 1),
            home_score=5,
            away_score=3,
            has_metadata=True,
            has_inning_scores=True,
            has_lineups=True,
            has_batting=True,
            has_pitching=True,
            today=date(2025, 4, 5),
        )
        assert status == "COMPLETED"

    def test_scheduled_future(self):
        status = derive_game_status(
            game_date=date(2025, 5, 1),
            home_score=None,
            away_score=None,
            has_metadata=False,
            has_inning_scores=False,
            has_lineups=False,
            has_batting=False,
            has_pitching=False,
            today=date(2025, 4, 1),
        )
        assert status == "SCHEDULED"

    def test_cancelled_with_metadata(self):
        status = derive_game_status(
            game_date=date(2025, 3, 1),
            home_score=None,
            away_score=None,
            has_metadata=True,
            has_inning_scores=False,
            has_lineups=False,
            has_batting=False,
            has_pitching=False,
            today=date(2025, 4, 1),
        )
        assert status == "CANCELLED"

    def test_unresolved_past(self):
        status = derive_game_status(
            game_date=date(2025, 3, 1),
            home_score=None,
            away_score=None,
            has_metadata=False,
            has_inning_scores=False,
            has_lineups=False,
            has_batting=False,
            has_pitching=False,
            today=date(2025, 4, 1),
        )
        assert status == "UNRESOLVED_MISSING"

    def test_live_today(self):
        status = derive_game_status(
            game_date=date.today(),
            home_score=None,
            away_score=None,
            has_metadata=False,
            has_inning_scores=True,
            has_lineups=False,
            has_batting=False,
            has_pitching=False,
            today=date.today(),
        )
        assert status == "LIVE"
