import json
import logging
from unittest.mock import MagicMock, patch

from src.cli.freshness_gate import main


class TestFreshnessGate:
    def test_default_run(self):
        with patch("src.cli.freshness_gate.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main([])
            assert result == 0

    def test_with_date(self):
        with patch("src.cli.freshness_gate.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--date", "20250101"])
            assert result == 0

    def test_with_json(self):
        with patch("src.cli.freshness_gate.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--json"])
            assert result == 0

    def test_with_max_hours(self):
        with patch("src.cli.freshness_gate.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--max-hours", "48"])
            assert result == 0

    def test_failure_exit_code(self):
        with (
            patch("src.cli.freshness_gate.SessionLocal") as mock_sf,
            patch("src.cli.freshness_gate.collect_freshness_issues") as mock_collect,
            patch("src.cli.freshness_gate.evaluate_freshness_gate") as mock_evaluate,
        ):
            mock_sf.return_value.__enter__.return_value = MagicMock()
            mock_collect.return_value = {"missing_events": ["20250101LGKT"]}
            mock_evaluate.return_value = ["missing_events: 1 game(s) -> 20250101LGKT"]

            result = main([])

            assert result == 1

    def test_json_output_contains_issue_payload(self, caplog):
        with (
            patch("src.cli.freshness_gate.SessionLocal") as mock_sf,
            patch("src.cli.freshness_gate.collect_freshness_issues") as mock_collect,
            patch("src.cli.freshness_gate.evaluate_freshness_gate") as mock_evaluate,
            caplog.at_level(logging.INFO, logger="src.cli.freshness_gate"),
        ):
            mock_sf.return_value.__enter__.return_value = MagicMock()
            mock_collect.return_value = {"missing_events": ["20250101LGKT"]}
            mock_evaluate.return_value = ["missing_events: 1 game(s) -> 20250101LGKT"]

            result = main(["--json"])

            assert result == 1
            payload = json.loads(caplog.records[-1].message)
            assert payload == {"ok": False, "issues": {"missing_events": ["20250101LGKT"]}}
