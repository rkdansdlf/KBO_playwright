from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.cli.gap_report import (
    check_relay_gaps,
    check_profile_gaps,
    check_standings_gaps,
    check_team_stats_gaps,
    main,
    run_gap_report,
    send_gap_alerts,
)


class TestGapReport:
    def test_default_run(self):
        with patch("src.cli.gap_report.run_gap_report") as mock:
            mock.return_value = {"gaps": {}}
            result = main([])
            assert result is None

    def test_no_alert(self):
        with patch("src.cli.gap_report.run_gap_report") as mock:
            mock.return_value = {"gaps": {}}
            result = main(["--no-alert"])
            assert result is None

    def test_dry_run(self):
        with patch("src.cli.gap_report.run_gap_report") as mock:
            mock.return_value = {"gaps": {}}
            result = main(["--dry-run"])
            assert result is None

    def test_send_gap_alerts_formats_relay(self):
        report = {
            "gaps": {
                "RELAY": {
                    "ok": False,
                    "missing_count": 6,
                    "missing_game_ids": ["G1", "G2", "G3", "G4", "G5", "G6"],
                },
            },
        }

        with patch("src.cli.gap_report.SlackWebhookClient.send_gap_alert") as mock:
            send_gap_alerts(report)

        mock.assert_called_once_with("RELAY", "6 games missing PBP", ["G1", "G2", "G3", "G4", "G5"])

    def test_send_gap_alerts_formats_freshness(self):
        report = {
            "gaps": {
                "FRESHNESS": {
                    "ok": False,
                    "total_issues": 3,
                    "details": {"detail": ["G1", "G2"], "relay": ["G3"]},
                },
            },
        }

        with patch("src.cli.gap_report.SlackWebhookClient.send_gap_alert") as mock:
            send_gap_alerts(report)

        mock.assert_called_once_with(
            "FRESHNESS",
            "3 total issues, detail: 2 games, relay: 1 games",
            ["detail: G1", "detail: G2", "relay: G3"],
        )

    def test_send_gap_alerts_formats_team_stats_and_skips_ok(self):
        report = {
            "gaps": {
                "TEAM_STATS": {
                    "ok": False,
                    "total": 2,
                    "batting_mismatches": 1,
                    "pitching_mismatches": 1,
                    "details": {
                        "batting": [{"team_id": "LG", "issue": "hits mismatch", "diffs": ["H +1", "AB -1"]}],
                        "pitching": [{"team_id": "SS", "issue": "era mismatch", "diffs": ["ER +2"]}],
                    },
                },
                "PROFILE": {"ok": True, "missing_count": 0},
            },
        }

        with patch("src.cli.gap_report.SlackWebhookClient.send_gap_alert") as mock:
            send_gap_alerts(report)

        mock.assert_called_once_with(
            "TEAM_STATS",
            "2 team stat mismatches, batting=1, pitching=1",
            ["타격 [LG]: hits mismatch", "  H +1", "  AB -1", "투수 [SS]: era mismatch", "  ER +2"],
        )


class TestCheckRelayGaps:
    def test_no_gaps(self):
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.all.return_value = []

            result = check_relay_gaps()
            assert result["ok"] is True

    def test_finds_gaps(self):
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            row = MagicMock()
            row.__getitem__ = lambda self, idx: f"G{idx}"
            mock_session.execute.return_value.all.return_value = [row]

            result = check_relay_gaps()
            assert result["ok"] is False
            assert result["missing_count"] == 1


class TestCheckProfileGaps:
    def test_no_gaps(self):
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.all.return_value = []

            result = check_profile_gaps()
            assert result["ok"] is True


class TestCheckStandingsGaps:
    def test_no_gaps(self):
        with patch("src.cli.gap_report.validate_standings_integrity") as mock_val:
            mock_val.return_value = []
            result = check_standings_gaps()
            assert result["ok"] is True

    def test_finds_gaps(self):
        with patch("src.cli.gap_report.validate_standings_integrity") as mock_val:
            mock_val.return_value = ["LG: mismatch"]
            result = check_standings_gaps()
            assert result["ok"] is False


class TestCheckTeamStatsGaps:
    def test_no_gaps(self):
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.all.return_value = []

            result = check_team_stats_gaps()
            assert result["ok"] is True


class TestRunGapReport:
    def test_returns_structured_result(self):
        with (
            patch("src.cli.gap_report.check_relay_gaps") as mock_relay,
            patch("src.cli.gap_report.check_profile_gaps") as mock_profile,
            patch("src.cli.gap_report.check_standings_gaps") as mock_standings,
            patch("src.cli.gap_report.check_team_stats_gaps") as mock_team,
            patch("src.cli.gap_report.check_freshness_gaps") as mock_fresh,
        ):
            mock_relay.return_value = {"ok": True, "missing_count": 0}
            mock_profile.return_value = {"ok": True, "missing_count": 0}
            mock_standings.return_value = {"ok": True, "total_issues": 0}
            mock_team.return_value = {"ok": True, "total": 0}
            mock_fresh.return_value = {"ok": True, "total_issues": 0}

            result = run_gap_report()
            assert "gaps" in result
            assert result["ok"] is True


class TestSendGapAlertsEdgeCases:
    def test_skips_ok_categories(self):
        report = {"gaps": {"RELAY": {"ok": True, "missing_count": 0}}}
        with patch("src.cli.gap_report.SlackWebhookClient.send_gap_alert") as mock:
            send_gap_alerts(report)
            mock.assert_not_called()

    def test_empty_gaps(self):
        report = {"gaps": {}}
        with patch("src.cli.gap_report.SlackWebhookClient.send_gap_alert") as mock:
            send_gap_alerts(report)
            mock.assert_not_called()
