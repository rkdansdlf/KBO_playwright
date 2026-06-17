from unittest.mock import MagicMock, patch

import psycopg2


class TestMigrateFieldingOCI:
    def test_migrate_fielding_success(self):
        with patch("scripts.migrate_fielding_oci.psycopg2.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_cur = MagicMock()
            mock_connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cur
            from scripts.migrate_fielding_oci import migrate_fielding

            migrate_fielding()
            assert mock_cur.execute.call_count == 4

    def test_migrate_fielding_connection_error(self):
        with patch("scripts.migrate_fielding_oci.psycopg2.connect", side_effect=psycopg2.OperationalError("conn fail")):
            from scripts.migrate_fielding_oci import migrate_fielding

            migrate_fielding()
