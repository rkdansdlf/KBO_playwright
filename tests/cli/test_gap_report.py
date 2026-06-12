from unittest.mock import patch

from src.cli.gap_report import main, send_gap_alerts


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
