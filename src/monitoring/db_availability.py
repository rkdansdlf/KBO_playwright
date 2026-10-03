"""Database availability/latency probe exposed to Prometheus.

DB availability deliberately lives on the **infrastructure** alert path
(Prometheus -> Alertmanager -> Telegram) rather than the in-process incident
manager: the incident ledger itself lives in the database, so persisting
"database is down" there would create a circular dependency.

A custom collector probes on each scrape, so the metric is always fresh without
a background thread.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from prometheus_client.core import GaugeMetricFamily
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlalchemy.engine import Engine

#: Failures a probe treats as "database unavailable" rather than propagating.
PROBE_EXCEPTIONS = (SQLAlchemyError, OSError, RuntimeError, ValueError, TypeError)


def probe_engine(engine: Engine) -> tuple[bool, float]:
    """Return ``(available, latency_seconds)`` for a single ``SELECT 1`` ping."""
    start = time.monotonic()
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except PROBE_EXCEPTIONS:
        return False, time.monotonic() - start
    return True, time.monotonic() - start


class DatabaseAvailabilityCollector:
    """Prometheus collector that pings the database on every scrape."""

    def __init__(self, engine: Engine | None = None) -> None:
        """Initialize the collector, resolving the default engine lazily."""
        self._engine = engine

    def _resolve_engine(self) -> Engine:
        if self._engine is not None:
            return self._engine
        from src.db.engine import Engine

        return Engine

    def collect(self) -> Iterable[Any]:
        """Yield the availability and latency gauges for one scrape."""
        available, latency = probe_engine(self._resolve_engine())
        yield GaugeMetricFamily(
            "kbo_db_available",
            "1 when the database answered SELECT 1, otherwise 0",
            value=1.0 if available else 0.0,
        )
        yield GaugeMetricFamily(
            "kbo_db_ping_latency_seconds",
            "Round-trip latency of the database SELECT 1 probe in seconds",
            value=latency,
        )
