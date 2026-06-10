from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import ArgumentError

from src.sync.oci_sync import OCISync, main


class TestOCISync:
    def test_class_inheritance(self):
        from src.sync.sync_base import OCISyncBase
        from src.sync.sync_games import GameSyncMixin
        from src.sync.sync_misc import MiscSyncMixin
        from src.sync.sync_players import PlayerSyncMixin
        from src.sync.sync_stats import StatsSyncMixin

        assert issubclass(OCISync, OCISyncBase)
        assert issubclass(OCISync, GameSyncMixin)
        assert issubclass(OCISync, MiscSyncMixin)
        assert issubclass(OCISync, PlayerSyncMixin)
        assert issubclass(OCISync, StatsSyncMixin)

    def test_init_with_mocked_session(self):
        mock_session = MagicMock()
        sync = OCISync("postgresql://user:pass@localhost/db", mock_session)
        assert sync.sqlite_session is mock_session
        sync.close()

    def test_init_oci_engine_created(self):
        mock_session = MagicMock()
        sync = OCISync("postgresql://user:pass@localhost/db", mock_session)
        assert sync.oci_engine is not None
        sync.close()

    def test_init_oci_url_none_raises(self):
        mock_session = MagicMock()
        with pytest.raises(ArgumentError):
            OCISync(None, mock_session)  # type: ignore[arg-type]


class TestOCISyncMain:
    @patch("src.sync.oci_sync.os.getenv", return_value=None)
    @patch("src.sync.oci_sync.logger")
    def test_main_no_oci_url(self, mock_logger, mock_getenv):
        main()
        mock_logger.error.assert_called_once()

    @patch("src.sync.oci_sync.os.getenv", return_value="postgresql://user:pass@localhost/db")
    @patch("src.db.engine.SessionLocal")
    @patch("src.sync.oci_sync.OCISync")
    def test_main_connection_fails(self, mock_sync_cls, mock_session_local, mock_getenv):
        mock_sync = mock_sync_cls.return_value
        mock_sync.test_connection.return_value = False

        main()

        mock_sync.test_connection.assert_called_once()

    @patch("src.sync.oci_sync.os.getenv", return_value="postgresql://user:pass@localhost/db")
    @patch("src.db.engine.SessionLocal")
    @patch("src.sync.oci_sync.OCISync")
    def test_main_no_data(self, mock_sync_cls, mock_session_local, mock_getenv):
        mock_db_session = MagicMock()
        mock_session_local.return_value.__enter__.return_value = mock_db_session
        mock_db_session.query.return_value.count.return_value = 0
        mock_sync = mock_sync_cls.return_value
        mock_sync.test_connection.return_value = True

        main()

        mock_sync.close.assert_not_called()

    @patch("src.sync.oci_sync.os.getenv", return_value="postgresql://user:pass@localhost/db")
    @patch("src.db.engine.SessionLocal")
    @patch("src.sync.oci_sync.OCISync")
    def test_main_with_data(self, mock_sync_cls, mock_session_local, mock_getenv):
        mock_db_session = MagicMock()
        mock_session_local.return_value.__enter__.return_value = mock_db_session
        mock_db_session.query.return_value.count.side_effect = [10, 5]
        mock_sync = mock_sync_cls.return_value
        mock_sync.test_connection.return_value = True
        mock_sync.sync_batting_data.return_value = 10
        mock_sync.sync_pitcher_data.return_value = 5

        main()

        mock_sync.sync_batting_data.assert_called_once()
        mock_sync.sync_pitcher_data.assert_called_once()
        mock_sync.close.assert_called_once()
