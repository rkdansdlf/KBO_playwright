from unittest.mock import MagicMock, patch

from scripts.investigations.check_oci_constraints import main


class TestCheckOciConstraints:
    @patch("scripts.investigations.check_oci_constraints.os.environ", {"OCI_DB_URL": "postgresql://test"})
    @patch("scripts.investigations.check_oci_constraints.create_engine")
    def test_main(self, mock_create_engine):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine
        mock_conn.execute.return_value.fetchall.return_value = []

        main()
        mock_conn.execute.assert_called()

    @patch("scripts.investigations.check_oci_constraints.os.environ", {})
    def test_missing_env(self):
        import pytest

        with pytest.raises(KeyError):
            main()
