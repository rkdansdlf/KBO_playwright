from unittest.mock import MagicMock, patch

from src.cli.check_data_status import main


def _mock_session():
    sess = MagicMock()
    sess.__enter__.return_value = sess
    sess.execute.return_value.scalar.return_value = 0
    sess.execute.return_value.all.return_value = []
    sess.execute.return_value.first.return_value = (None, None)
    sess.query.return_value.filter.return_value.count.return_value = 0
    sess.query.return_value.filter.return_value.all.return_value = []
    sess.query.return_value.all.return_value = []
    sess.query.return_value.count.return_value = 0
    return sess


class TestCheckDataStatus:
    def test_default_run(self):
        with patch("sys.argv", ["check_data_status"]), patch("src.cli.check_data_status.SessionLocal") as mock_sf:
            mock_sf.return_value = _mock_session()
            result = main([])
            assert result is None

    def test_verbose(self):
        with (
            patch("sys.argv", ["check_data_status", "--verbose"]),
            patch("src.cli.check_data_status.SessionLocal") as mock_sf,
        ):
            mock_sf.return_value = _mock_session()
            result = main(["--verbose"])
            assert result is None

    def test_p0_readiness(self):
        with patch("src.cli.check_data_status.SessionLocal") as mock_sf:
            mock_sf.return_value = _mock_session()
            with patch("src.cli.check_data_status.build_p0_readiness") as mock_p0:
                mock_p0.return_value = {
                    "start_date": "20250101",
                    "end_date": "20250101",
                    "schedule": 0,
                    "pregame": 0,
                    "live": 0,
                    "postgame": 0,
                    "relay": 0,
                    "roster": 0,
                    "broadcast": 0,
                    "oci": 0,
                    "failures": [],
                }
                result = main(["--p0", "--date", "20250101"])
                assert result is None
