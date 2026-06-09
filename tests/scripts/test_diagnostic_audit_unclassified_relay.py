from unittest.mock import MagicMock, patch

from scripts.diagnostic.audit_unclassified_relay import analyze_texts, collect_unclassified_text


class TestCollectUnclassifiedText:
    @patch("scripts.diagnostic.audit_unclassified_relay.SessionLocal")
    def test_empty(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.all.return_value = []

        result = collect_unclassified_text(days=7)
        assert result == []


class TestAnalyzeTexts:
    def test_no_rows(self, caplog):
        import logging

        caplog.set_level(logging.INFO)
        analyze_texts([])
        assert "No unclassified" in caplog.text

    def test_with_rows(self, caplog):
        import logging

        caplog.set_level(logging.INFO)
        rows = [
            {"id": 1, "game_id": "G1", "inning": 1, "inning_half": "TOP", "text": "test: foo", "event_type": "unknown"},
            {"id": 2, "game_id": "G1", "inning": 2, "inning_half": "BOT", "text": "test: bar", "event_type": "unknown"},
        ]
        analyze_texts(rows)
        assert "Total unclassified entries: 2" in caplog.text
