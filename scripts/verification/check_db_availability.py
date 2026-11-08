"""Probe the operational PostgreSQL from a host that is not the database host.

The 2026-11-07/08 outage ran roughly two and a half days before anyone acted on
it. Every KBO alert path lived on the database host itself -- the scheduler sends
the Telegram alerts and exposes the metrics, so both die with the machine -- and
the backend stack's alerts, which did keep firing, report to a different
audience. Nothing outside the host was asking "is the database answering", so
nothing said no. This probe is that outside; the 30-minute watchdog workflow runs
it with the same read-only URL and ``tag:ci`` path as the weekly live
verification.

It is deliberately dependency-light (``psycopg`` plus the standard library), and
it draws the distinction the outage showed matters: **TCP reachability is not
availability**. For most of that outage the port accepted connections while
PostgreSQL never completed a handshake, so ``tcp_unreachable`` (firewall,
sleeping host) and ``postgres_silent`` (port open, postmaster not answering) are
reported as different reasons.

Exit codes: 0 when the database answers, 1 when it does not (either reason), and
2 on a configuration error (missing URL, or a URL that is not PostgreSQL). The
URL carries a password and is never printed.

Usage:
    LIVE_DB_URL='postgresql+psycopg://...' python3 scripts/verification/check_db_availability.py --json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sys
import time
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import psycopg

if TYPE_CHECKING:
    from collections.abc import Sequence

#: TCP connect budget. Short on purpose: a watchdog may not hang.
TCP_TIMEOUT_SECONDS = 5.0

#: PostgreSQL handshake budget. Longer than the TCP one because the interesting
#: failure mode is a listening port in front of a database that never answers.
CONNECT_TIMEOUT_SECONDS = 10

REACHABLE = "reachable"
TCP_UNREACHABLE = "tcp_unreachable"
POSTGRES_SILENT = "postgres_silent"
NOT_POSTGRES = "not_postgresql"
MISSING_URL = "missing_url"

_POSTGRES_SCHEMES = ("postgresql", "postgres")
_DRIVER_SUFFIX = re.compile(r"^(postgresql|postgres)\+\w+://")


@dataclass(frozen=True)
class ProbeResult:
    """What one probe found, in a shape safe to print (never the URL)."""

    reachable: bool
    reason: str
    host: str
    port: int
    elapsed_ms: int
    detail: str


def normalize_dsn(url: str) -> str:
    """Strip a SQLAlchemy driver suffix (``postgresql+psycopg://``) for libpq."""
    return _DRIVER_SUFFIX.sub(r"\1://", url)


def target_from_url(url: str) -> tuple[str, int] | None:
    """Return ``(host, port)`` for a PostgreSQL URL, or ``None`` when it is not one."""
    parsed = urlsplit(normalize_dsn(url))
    if parsed.scheme not in _POSTGRES_SCHEMES or not parsed.hostname:
        return None
    return parsed.hostname, parsed.port or 5432


def probe(url: str) -> ProbeResult:
    """TCP-connect, then complete a PostgreSQL handshake and run ``SELECT 1``.

    Args:
        url: PostgreSQL URL; SQLAlchemy driver suffixes are accepted.

    Returns:
        The probe result. ``reachable`` is True only after the handshake answers.

    Raises:
        ValueError: when the URL is not a PostgreSQL URL.

    """
    started = time.monotonic()
    target = target_from_url(url)
    if target is None:
        message = "the probe URL is not a PostgreSQL URL"
        raise ValueError(message)
    host, port = target
    try:
        with socket.create_connection((host, port), timeout=TCP_TIMEOUT_SECONDS):
            pass
    except OSError as exc:
        return ProbeResult(False, TCP_UNREACHABLE, host, port, _elapsed_ms(started), type(exc).__name__)
    try:
        with psycopg.connect(normalize_dsn(url), connect_timeout=CONNECT_TIMEOUT_SECONDS) as connection:
            connection.execute("SELECT 1")
    except psycopg.OperationalError as exc:
        return ProbeResult(False, POSTGRES_SILENT, host, port, _elapsed_ms(started), _first_line(str(exc)))
    return ProbeResult(True, REACHABLE, host, port, _elapsed_ms(started), "")


def _elapsed_ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


def _first_line(message: str) -> str:
    """Keep an error message to one bounded line; long libpq paragraphs bury the reason."""
    return message.strip().splitlines()[0][:200] if message.strip() else ""


def resolve_url(explicit: str | None) -> str | None:
    """Return the URL to probe: the flag first, then ``LIVE_DB_URL``, then ``DATABASE_URL``."""
    for candidate in (explicit, os.environ.get("LIVE_DB_URL"), os.environ.get("DATABASE_URL")):
        if candidate:
            return candidate
    return None


def _report(result: ProbeResult, *, as_json: bool) -> None:
    if as_json:
        sys.stdout.write(json.dumps(asdict(result), ensure_ascii=False) + "\n")
        return
    status = "reachable" if result.reachable else f"UNREACHABLE ({result.reason})"
    detail = f" — {result.detail}" if result.detail else ""
    sys.stdout.write(f"{status} {result.host}:{result.port} in {result.elapsed_ms}ms{detail}\n")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the probe and map its outcome to the documented exit codes."""
    parser = argparse.ArgumentParser(description="Probe the operational PostgreSQL for reachability.")
    parser.add_argument("--url", default=None, help="PostgreSQL URL; defaults to LIVE_DB_URL then DATABASE_URL.")
    parser.add_argument("--json", action="store_true", help="Emit the result as JSON.")
    args = parser.parse_args(argv)

    url = resolve_url(args.url)
    if not url:
        sys.stderr.write("error: no URL given; set LIVE_DB_URL or pass --url\n")
        return 2
    if target_from_url(url) is None:
        sys.stderr.write("error: the probe URL is not a PostgreSQL URL\n")
        return 2

    result = probe(url)
    _report(result, as_json=args.json)
    return 0 if result.reachable else 1


if __name__ == "__main__":
    raise SystemExit(main())
