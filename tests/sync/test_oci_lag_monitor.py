from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.sync.lag_monitor import (
    _execute_category_resync,
    _parse_timestamp,
    check_and_resync_lagging_tables,
    get_table_max_timestamp,
)
from src.utils.metrics import KBO_OCI_TABLE_SYNC_LAG_SECONDS


def test_parse_timestamp() -> None:
    assert _parse_timestamp(None) is None
    now = datetime.now()
    assert _parse_timestamp(now) == now
    assert _parse_timestamp("2026-07-29T12:00:00") == datetime(2026, 7, 29, 12, 0, 0)
    assert _parse_timestamp("invalid") is None
    assert _parse_timestamp(123) is None


def test_get_table_max_timestamp() -> None:
    mock_session = MagicMock()
    mock_session.execute.return_value.scalar.return_value = "2026-07-29T10:00:00"
    res = get_table_max_timestamp(mock_session, "game", "updated_at")
    assert res == datetime(2026, 7, 29, 10, 0, 0)

    # test query exception handling
    from sqlalchemy.exc import SQLAlchemyError

    mock_session.execute.side_effect = SQLAlchemyError("DB error")
    assert get_table_max_timestamp(mock_session, "game", "updated_at") is None


def test_check_and_resync_lagging_tables_no_url() -> None:
    with patch("src.sync.lag_monitor.get_oci_url", return_value=None):
        res = check_and_resync_lagging_tables(target_url=None)
        assert res["status"] == "skipped"


def test_check_and_resync_lagging_tables_detection_and_resync() -> None:
    mock_sq_session = MagicMock()
    mock_oci_session = MagicMock()

    # Mock timestamp queries: game table has 30h lag (>24h threshold)
    def mock_get_timestamp(session, table, col="updated_at"):
        if session == mock_sq_session:
            return datetime(2026, 7, 29, 12, 0, 0)
        # OCI session returns timestamp 30h behind for game table
        if table == "game":
            return datetime(2026, 7, 28, 6, 0, 0)
        return datetime(2026, 7, 29, 12, 0, 0)

    with (
        patch("src.sync.lag_monitor.get_oci_url", return_value="sqlite:///:memory:"),
        patch("src.sync.lag_monitor.create_engine_for_url"),
        patch("src.sync.lag_monitor.sessionmaker") as mock_sm,
        patch("src.sync.lag_monitor.SessionLocal") as mock_sq_factory,
        patch("src.sync.lag_monitor.get_table_max_timestamp", side_effect=mock_get_timestamp),
        patch("src.sync.oci_sync.OCISync") as mock_oci_sync_cls,
    ):
        mock_sq_factory.return_value.__enter__.return_value = mock_sq_session
        mock_sm.return_value.return_value.__enter__.return_value = mock_oci_session

        mock_syncer = MagicMock()
        mock_syncer.sync_games.return_value = 10
        mock_oci_sync_cls.return_value = mock_syncer

        res = check_and_resync_lagging_tables(
            target_url="sqlite:///:memory:",
            threshold_seconds=86400.0,
            dry_run=False,
        )

        assert res["status"] == "completed"
        assert "game" in res["lagging_tables"]
        assert res["resynced_counts"].get("games") == 10
        mock_syncer.sync_games.assert_called_once()
        assert KBO_OCI_TABLE_SYNC_LAG_SECONDS.labels(table="game")._value.get() > 80000


def test_execute_category_resync() -> None:
    mock_syncer = MagicMock()
    mock_syncer.sync_games.return_value = 5
    mock_syncer.sync_game_details.return_value = 12
    mock_syncer.sync_season_stats.return_value = 20

    assert _execute_category_resync(mock_syncer, "games") == 5
    assert _execute_category_resync(mock_syncer, "game_details") == 12
    assert _execute_category_resync(mock_syncer, "season_stats") == 20
    assert _execute_category_resync(mock_syncer, "unknown") == 0
