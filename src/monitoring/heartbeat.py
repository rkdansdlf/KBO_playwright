"""Ping an external dead-man's switch so a host-wide stop is still noticed.

Prometheus and Alertmanager run in the same Docker stack as the services they
watch -- ``monitoring/prometheus/prometheus.yml`` scrapes ``scheduler:8000`` and
``api-server:8000`` -- so a stopped host takes the alert path down with it. That
is what happened on 2026-11-09: Docker Desktop and WSL2 hung, and nothing left
the machine to say so. A service that expects a ping and complains when the pings
stop observes the host from outside it, which is the only vantage point that
survives this failure mode.

Deliberately **not** gated on the database. ``_with_db_fail_fast_guard`` exists
to keep DB-bound jobs from burning a lock while a database is down, and this job
must keep pinging in exactly that situation: otherwise a database outage would
be reported as a dead host. The database has its own alert path
(:mod:`src.monitoring.db_availability`), which the external switch cannot see.

The ping URL is a secret. Anyone holding it can silence the switch by pinging it
on the host's behalf, and ``httpx`` logs the full request URL at INFO, so the URL
is registered with :mod:`src.utils.log_masking` before the first request. Failure
lines name the exception class only -- never the URL, and never ``str(exc)``,
which can carry it too.
"""

from __future__ import annotations

import logging
import os

import httpx

from src.utils.log_masking import register_secret

logger = logging.getLogger(__name__)

#: Environment variable holding the ping URL. Unset means the switch is off.
HEARTBEAT_URL_ENV = "KBO_HEARTBEAT_URL"

#: Seconds to wait for the switch.
#:
#: A heartbeat that blocks is worse than one that misses: APScheduler runs jobs
#: on a shared worker pool, and a hung ping would hold that thread against the
#: real jobs behind it. The switch is a small HTTP endpoint, so a generous but
#: finite bound is enough.
HEARTBEAT_TIMEOUT_SECONDS = 10.0


def heartbeat_url() -> str | None:
    """Return the configured ping URL, or ``None`` when the switch is off.

    Returns:
        The URL with surrounding whitespace removed, or ``None`` for an unset or
        blank value. Blank is treated as unset because a variable exported as an
        empty string is how a compose file disables an optional feature.

    """
    value = os.getenv(HEARTBEAT_URL_ENV, "").strip()
    return value or None


def send_heartbeat(url: str | None = None, *, timeout: float = HEARTBEAT_TIMEOUT_SECONDS) -> bool:
    """Ping the dead-man's switch once.

    Redirects are not followed: the URL is the credential, and a redirect would
    send it to whatever the response names. A 3xx therefore counts as a failed
    ping, which is the honest reading -- the switch did not record it.

    Args:
        url: Ping URL. ``None`` reads :data:`HEARTBEAT_URL_ENV`.
        timeout: Seconds to wait for the switch.

    Returns:
        ``True`` when the switch answered with a 2xx, ``False`` when no URL is
        configured, the request failed, or the status was not a success.

    """
    target = url if url is not None else heartbeat_url()
    if not target:
        return False

    # Before the request, not after: the record that leaks the URL is the one
    # ``httpx`` writes while sending it.
    register_secret(target)

    try:
        response = httpx.get(target, timeout=timeout)
    except Exception as exc:  # noqa: BLE001 - the class name is all that is safe to log
        logger.warning("[Heartbeat] ping failed (%s)", type(exc).__name__)
        return False

    if not response.is_success:
        logger.warning("[Heartbeat] switch did not record the ping (HTTP %s)", response.status_code)
        return False

    logger.debug("[Heartbeat] ping accepted")
    return True
