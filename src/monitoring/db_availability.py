"""Database availability/latency probe exposed to Prometheus.

DB availability deliberately lives on the **infrastructure** alert path
(Prometheus -> Alertmanager -> Telegram) rather than the in-process incident
manager: the incident ledger itself lives in the database, so persisting
"database is down" there would create a circular dependency.

A custom collector probes on each scrape, so the metric is always fresh without
a background thread.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from prometheus_client.core import GaugeMetricFamily
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

#: Failures a probe treats as "database unavailable" rather than propagating.
#:
#: Deliberately broad: any of these means the probe did not get an answer, and
#: the gate's job is to not run the job. What the tuple does **not** say is
#: *why*, which is why the reason is logged rather than returned -- see
#: :func:`probe_engine`.
PROBE_EXCEPTIONS = (SQLAlchemyError, OSError, RuntimeError, ValueError, TypeError)


def _target_label(engine: Engine) -> str:
    """Return ``host:port/db`` for the probe target, without credentials."""
    try:
        url = engine.url
    except AttributeError:  # pragma: no cover - a stubbed engine in tests
        return "unknown"
    host = url.host or "?"
    port = f":{url.port}" if url.port else ""
    return f"{host}{port}/{url.database or '?'}"


def probe_engine(engine: Engine) -> tuple[bool, float]:
    """Return ``(available, latency_seconds)`` for a single ``SELECT 1`` ping.

    A failure returns ``False`` and nothing else, which is the right shape for a
    boolean gate and the wrong shape for an operator. ``SQLAlchemyError`` covers
    a refused connection, a three-second connect timeout, and "too many clients
    already" alike, and the caller logs a single fixed sentence -- "database
    unreachable" -- for all three. On 2026-10-12 that left two consecutive gate
    rejections undiagnosable from the log: the surrounding job had queried the
    same database successfully milliseconds earlier, so the only honest reading
    was a false negative, and nothing recorded which cause it was.

    So the boolean is unchanged and the cause stops being discarded. This is the
    same lesson as the incident ledger: a state nobody can explain is
    indistinguishable from a state nobody understands.
    """
    start = time.monotonic()
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except PROBE_EXCEPTIONS as exc:
        logger.warning(
            "DB probe failed for %s after %.3fs: %s: %s",
            _target_label(engine),
            time.monotonic() - start,
            type(exc).__name__,
            str(exc).replace("\n", " ")[:300],
        )
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
