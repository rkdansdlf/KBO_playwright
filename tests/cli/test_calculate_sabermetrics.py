from unittest.mock import MagicMock, patch

from sqlalchemy.exc import SQLAlchemyError

from src.cli.calculate_sabermetrics import batch_calculate_sabermetrics, main


class TestCalculateSabermetrics:
    def test_default_years(self):
        with patch("src.cli.calculate_sabermetrics.batch_calculate_sabermetrics") as mock:
            result = main([])
            assert result == 0
            mock.assert_called_once()
            args, _ = mock.call_args
            assert 2020 in args[0]

    def test_specific_year(self):
        with patch("src.cli.calculate_sabermetrics.batch_calculate_sabermetrics") as mock:
            result = main(["--years", "2025"])
            assert result == 0
            args, _ = mock.call_args
            assert args[0] == [2025]

    def test_with_sync(self):
        with patch("src.cli.calculate_sabermetrics.batch_calculate_sabermetrics") as mock:
            result = main(["--sync"])
            assert result == 0
            _, kwargs = mock.call_args
            assert kwargs["sync_oci"] is True

    def test_specific_level(self):
        with patch("src.cli.calculate_sabermetrics.batch_calculate_sabermetrics") as mock:
            result = main(["--level", "KBO2"])
            assert result == 0
            _, kwargs = mock.call_args
            assert kwargs["levels"] == ["KBO2"]

    def test_all_levels(self):
        with patch("src.cli.calculate_sabermetrics.batch_calculate_sabermetrics") as mock:
            result = main(["--level", "all"])
            assert result == 0
            _, kwargs = mock.call_args
            assert kwargs["levels"] == ["KBO1", "KBO2"]


class TestBatchCalculateSabermetrics:
    def test_sync_oci_without_url_skips_sync(self):
        with (
            patch("src.cli.calculate_sabermetrics.SessionLocal") as MockSession,
            patch("src.cli.calculate_sabermetrics.os") as mock_os,
        ):
            mock_os.getenv.return_value = None
            mock_session = MagicMock()
            MockSession.return_value.__enter__ = MagicMock(return_value=mock_session)
            MockSession.return_value.__exit__ = MagicMock(return_value=False)

            mock_session.query.return_value.filter.return_value.all.return_value = []

            batch_calculate_sabermetrics([2025], sync_oci=True)

    def test_sync_oci_with_url_calls_syncer(self):
        with (
            patch("src.cli.calculate_sabermetrics.SessionLocal") as MockSession,
            patch("src.cli.calculate_sabermetrics.os") as mock_os,
            patch("src.cli.calculate_sabermetrics.OCISync") as MockSync,
        ):
            mock_os.getenv.return_value = "postgresql://host/db"
            mock_session = MagicMock()
            MockSession.return_value.__enter__ = MagicMock(return_value=mock_session)
            MockSession.return_value.__exit__ = MagicMock(return_value=False)

            mock_session.query.return_value.filter.return_value.all.return_value = []

            batch_calculate_sabermetrics([2025], sync_oci=True)
            MockSync.assert_called_once()

    def test_empty_years_list(self):
        with patch("src.cli.calculate_sabermetrics.SessionLocal") as MockSession:
            mock_session = MagicMock()
            MockSession.return_value.__enter__ = MagicMock(return_value=mock_session)
            MockSession.return_value.__exit__ = MagicMock(return_value=False)
            mock_session.query.return_value.filter.return_value.all.return_value = []

            batch_calculate_sabermetrics([])

    def test_batch_calculates_metrics_for_levels_and_players(self):
        batter = MagicMock()
        pitcher = MagicMock()
        lg_constants = {"lg_woba": 0.320, "fip_constant": 3.10, "lg_r_per_pa": 0.120}
        with (
            patch("src.cli.calculate_sabermetrics.SessionLocal") as MockSession,
            patch("src.cli.calculate_sabermetrics.SabermetricsCalculator") as MockCalc,
        ):
            mock_session = MagicMock()
            MockSession.return_value.__enter__.return_value = mock_session
            MockSession.return_value.__exit__.return_value = False
            MockCalc.get_league_constants.return_value = lg_constants
            MockCalc.calculate_batting_metrics.return_value = {"woba": 0.330}
            MockCalc.calculate_pitching_metrics.return_value = {"fip_adj": 3.50, "lob_pct": 0.70, "war": 2.0}
            mock_session.query.return_value.filter.return_value.all.side_effect = [[batter], [pitcher]]

            batch_calculate_sabermetrics([2025], levels=["KBO2"])

            MockCalc.get_league_constants.assert_called_once_with(mock_session, 2025, level="KBO2")
            MockCalc.calculate_batting_metrics.assert_called_once_with(batter, lg_constants)
            MockCalc.calculate_pitching_metrics.assert_called_once_with(pitcher, lg_constants)
            mock_session.commit.assert_called_once()

    def test_batch_skips_level_when_constants_fail(self):
        with (
            patch("src.cli.calculate_sabermetrics.SessionLocal") as MockSession,
            patch("src.cli.calculate_sabermetrics.SabermetricsCalculator") as MockCalc,
        ):
            mock_session = MagicMock()
            MockSession.return_value.__enter__.return_value = mock_session
            MockSession.return_value.__exit__.return_value = False
            MockCalc.get_league_constants.side_effect = SQLAlchemyError("boom")

            batch_calculate_sabermetrics([2025])

            MockCalc.get_league_constants.assert_called()
            mock_session.query.assert_not_called()
