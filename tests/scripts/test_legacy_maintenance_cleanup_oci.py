from unittest.mock import MagicMock, patch

from scripts.legacy.maintenance.cleanup_oci import DELETE_STEPS, build_arg_parser, cleanup_oci_duplicates


class TestDeleteSteps:
    def test_has_all_tables(self):
        tables = [step[0] for step in DELETE_STEPS]
        assert "game" in tables
        assert "game_events" in tables
        assert "game_batting_stats" in tables


class TestCleanupOciDuplicates:
    @patch("scripts.legacy.maintenance.cleanup_oci.psycopg2.connect")
    def test_dry_run(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (5,)

        result = cleanup_oci_duplicates(database_url="postgresql://test")
        assert result["non_primary_games_before"] == 5
        mock_conn.rollback.assert_called_once()

    @patch("scripts.legacy.maintenance.cleanup_oci.psycopg2.connect")
    def test_apply(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (5,)

        cleanup_oci_duplicates(database_url="postgresql://test", apply=True)
        mock_conn.commit.assert_called()


class TestBuildArgParser:
    def test_defaults(self):
        parser = build_arg_parser()
        args = parser.parse_args([])
        assert args.apply is False
