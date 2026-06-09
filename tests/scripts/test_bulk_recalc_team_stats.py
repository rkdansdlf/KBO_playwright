from unittest.mock import MagicMock, patch


class TestBulkRecalcTeamStats:
    def test_main_calls_subprocess(self):
        with patch("scripts.bulk_recalc_team_stats.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock()
            from scripts.bulk_recalc_team_stats import main
            main()
            assert mock_run.call_count > 0
            args, kwargs = mock_run.call_args
            assert "src.cli.recalc_team_stats" in args[0]

    def test_main_handles_failure(self):
        import subprocess
        with patch("scripts.bulk_recalc_team_stats.subprocess.run") as mock_run:
            mock_run.side_effect = [subprocess.CalledProcessError(1, ["cmd"])] + [MagicMock()] * 50
            from scripts.bulk_recalc_team_stats import main
            main()
            assert mock_run.call_count > 1
