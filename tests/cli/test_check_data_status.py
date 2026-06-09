from unittest.mock import patch, MagicMock

from src.cli.check_data_status import main


class TestCheckDataStatus:
    def test_default_run(self):
        with patch("sys.argv", ["check_data_status"]), \
             patch("src.cli.check_data_status.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalar.return_value = 0
            mock_session.execute.return_value.all.return_value = []
            mock_session.execute.return_value.first.return_value = (None, None)
            result = main([])
            assert result is None

    def test_verbose(self):
        with patch("sys.argv", ["check_data_status", "--verbose"]), \
             patch("src.cli.check_data_status.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalar.return_value = 0
            mock_session.execute.return_value.all.return_value = []
            result = main(["--verbose"])
            assert result is None

    def test_p0_readiness(self):
        with patch("src.cli.check_data_status.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            with patch("src.cli.check_data_status.build_p0_readiness") as mock_p0:
                mock_p0.return_value = {
                    "start_date": "20250101", "end_date": "20250101",
                    "schedule": 0, "pregame": 0, "live": 0, "postgame": 0,
                    "relay": 0, "roster": 0, "broadcast": 0, "oci": 0,
                    "failures": [],
                }
                result = main(["--p0", "--date", "20250101"])
                assert result is None
