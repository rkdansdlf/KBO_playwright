from unittest.mock import MagicMock, patch

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
