from unittest.mock import MagicMock, patch

from scripts.verification.audit_advanced_stats import audit_batting_stats, audit_pitching_stats


class TestAuditBattingStats:
    @patch("scripts.verification.audit_advanced_stats.SessionLocal")
    def test_no_records(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.query.return_value.filter.return_value.limit.return_value.all.return_value = []

        audit_batting_stats(mock_session)
        # No error raised

    @patch("scripts.verification.audit_advanced_stats.SessionLocal")
    def test_with_records(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session

        mock_rec = MagicMock()
        mock_rec.at_bats = 10
        mock_rec.hits = 3
        mock_rec.walks = 2
        mock_rec.hbp = 0
        mock_rec.sacrifice_flies = 0
        mock_rec.sacrifice_hits = 0
        mock_rec.doubles = 0
        mock_rec.triples = 0
        mock_rec.home_runs = 0
        mock_rec.strikeouts = 1
        mock_rec.intentional_walks = 0
        mock_rec.stolen_bases = 0
        mock_rec.caught_stealing = 0
        mock_rec.gdp = 0
        mock_rec.id = 1
        mock_rec.season = 2025
        mock_rec.player_id = 123
        mock_rec.avg = 0.3
        mock_rec.obp = 0.4
        mock_rec.slg = 0.3

        mock_session.query.return_value.filter.return_value.limit.return_value.all.return_value = [mock_rec]

        audit_batting_stats(mock_session)


class TestAuditPitchingStats:
    @patch("scripts.verification.audit_advanced_stats.SessionLocal")
    def test_no_records(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.query.return_value.filter.return_value.limit.return_value.all.return_value = []

        audit_pitching_stats(mock_session)
        # No error raised
