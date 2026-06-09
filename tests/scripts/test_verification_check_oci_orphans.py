from unittest.mock import MagicMock, patch

from scripts.verification.check_oci_orphans import check_oci_orphans


class TestCheckOciOrphans:
    @patch("scripts.verification.check_oci_orphans.sqlalchemy.create_engine")
    def test_no_orphans(self, mock_create_engine):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine
        mock_conn.execute.return_value.scalar.side_effect = [0, 0, 0, 0, 0, 5]

        check_oci_orphans()
        assert mock_conn.execute.call_count > 0
