from unittest.mock import MagicMock, patch

from scripts.legacy.maintenance.seed_data import (
    DEFAULT_TEAMS,
    _is_valid_team_row,
    _seed_default_teams,
    seed_teams,
    to_date_or_none,
    to_int_or_none,
)


class TestToIntOrNone:
    def test_valid(self):
        assert to_int_or_none("42") == 42

    def test_empty(self):
        assert to_int_or_none(None) is None
        assert to_int_or_none("") is None

    def test_invalid(self):
        assert to_int_or_none("abc") is None


class TestToDateOrNone:
    def test_valid(self):
        from datetime import date

        assert to_date_or_none("2025-04-01") == date(2025, 4, 1)

    def test_invalid(self):
        assert to_date_or_none("not-a-date") is None


class TestIsValidTeamRow:
    def test_valid(self):
        assert _is_valid_team_row({"team_id": "LG", "team_name": "LG Twins"}) is True

    def test_missing_id(self):
        assert _is_valid_team_row({"team_id": "", "team_name": "LG"}) is False

    def test_invalid_tokens(self):
        assert _is_valid_team_row({"team_id": "team_id", "team_name": "Test"}) is False


class TestDefaultTeams:
    def test_has_expected_teams(self):
        ids = [t["team_id"] for t in DEFAULT_TEAMS]
        assert "SS" in ids
        assert "LG" in ids
        assert "KIA" in ids
        assert "NC" in ids
        assert "KR" in ids


class TestSeedTeams:
    @patch("scripts.legacy.maintenance.seed_data.Path.exists")
    def test_fallback_to_default(self, mock_exists):
        mock_exists.return_value = False
        mock_session = MagicMock()

        seed_teams(mock_session, "nonexistent.csv")
        assert mock_session.merge.call_count >= len(DEFAULT_TEAMS)
        mock_session.commit.assert_called()


class TestSeedDefaultTeams:
    def test_upserts_all_teams(self):
        mock_session = MagicMock()
        _seed_default_teams(mock_session)
        assert mock_session.merge.call_count == len(DEFAULT_TEAMS)
