from __future__ import annotations

import os
import signal
from unittest.mock import MagicMock, patch

from scripts import scheduler
import pytest

pytestmark = pytest.mark.integration


def test_scheduler_signal_shutdown() -> None:
    """Test that scheduler registers SIGTERM/SIGINT and gracefully shuts down and releases locks."""
    mock_scheduler = MagicMock()
    registered_handlers = {}

    def mock_signal_register(signum: int, handler: object) -> None:
        registered_handlers[signum] = handler

    # Backup STARTUP_RUN environment
    old_startup_run = os.environ.get("STARTUP_RUN")
    os.environ["STARTUP_RUN"] = "0"

    try:
        with (
            patch("scripts.scheduler.BlockingScheduler", return_value=mock_scheduler),
            patch("signal.signal", side_effect=mock_signal_register),
            patch("sys.exit") as mock_exit,
            patch("scripts.scheduler.LIVE_LOCK") as mock_live_lock,
            patch("scripts.scheduler.DAILY_LOCK") as mock_daily_lock,
            patch("scripts.scheduler.MAINTENANCE_LOCK") as mock_maintenance_lock,
            patch("scripts.scheduler.REALTIME_OCI_SYNC_LOCK") as mock_oci_lock,
            patch("sys.argv", ["scheduler.py", "--no-startup-run"]),
        ):
            # Execute main to register signal handlers
            scheduler.main()

            # Verify signal handlers were registered
            assert signal.SIGTERM in registered_handlers
            assert signal.SIGINT in registered_handlers

            # Invoke the SIGTERM shutdown handler
            handler = registered_handlers[signal.SIGTERM]
            handler(signal.SIGTERM, None)

            # Assertions
            mock_scheduler.shutdown.assert_called_once_with(wait=False)
            mock_live_lock.release.assert_called_once()
            mock_daily_lock.release.assert_called_once()
            mock_maintenance_lock.release.assert_called_once()
            mock_oci_lock.release.assert_called_once()
            mock_exit.assert_called_once_with(0)

    finally:
        # Restore STARTUP_RUN environment
        if old_startup_run is not None:
            os.environ["STARTUP_RUN"] = old_startup_run
        else:
            os.environ.pop("STARTUP_RUN", None)
