"""Unit tests for CDCDaemon."""

from __future__ import annotations

import threading

from src.sync.cdc_daemon import CDCDaemon


def test_cdc_sync_once_dry_run() -> None:
    """Single CDC sync cycle in dry-run mode should execute cleanly."""
    daemon = CDCDaemon()
    summary = daemon.sync_once(dry_run=True)

    assert summary.status == "SUCCESS"
    assert summary.duration_seconds >= 0.0
    assert summary.synced_tables_count >= 0


def test_cdc_daemon_loop_stops_on_event() -> None:
    """Daemon loop must stop promptly when stop_event is set."""
    daemon = CDCDaemon(sync_interval=10)
    stop_event = threading.Event()

    # Set stop immediately
    stop_event.set()
    daemon.run_daemon_loop(stop_event, dry_run=True)
