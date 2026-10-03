"""Fail loudly when a unit test opens a non-loopback socket.

The unit-test job is meant to be hermetic: every source that needs HTTP is
mocked at the transport seam (`crawler._http`, session factories, and so on).
A test that stubs an attribute the code never calls is silently hermetic-looking
but still reaches the real internet, so it passes on a developer machine and
fails, or worse passes for the wrong reason, in CI.

`tests/test_metrics.py::test_start_metrics_server` and both
`TestWikiFetchSnapshot` cases in `tests/crawlers/test_award_crawler.py` were
doing exactly that. Rather than rely on reviewing each mock by hand, this
plugin denies outbound connections so the leak becomes a local failure.

Usage:

    python -m pytest tests/ -p no_external_network

Loopback stays reachable because several tests bind a real server or open a
local file through the socket layer.
"""

from __future__ import annotations

import ipaddress
import socket

_original_connect = socket.socket.connect
_original_connect_ex = socket.socket.connect_ex
_original_create_connection = socket.create_connection
_original_getaddrinfo = socket.getaddrinfo

LOOPBACK_HOSTNAMES = frozenset({"localhost", "localhost.localdomain", "", "ip6-localhost"})


class ExternalNetworkBlocked(RuntimeError):
    """Raised when a test attempts to reach a non-loopback host."""


def _is_loopback(host: object) -> bool:
    """Return True when `host` resolves to loopback without leaving the host."""
    if not isinstance(host, str) or host in LOOPBACK_HOSTNAMES:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _host_of(address: object) -> str:
    """Best-effort extraction of the host from a connect()/connect_ex() address."""
    if isinstance(address, tuple) and address:
        return str(address[0])
    return ""


def _deny(host: object, caller: str) -> None:
    if _is_loopback(host):
        return
    detail = (
        f"unit test attempted an external connection via {caller}(host={host!r}); "
        "mock the transport seam the code actually calls instead"
    )
    raise ExternalNetworkBlocked(detail)


def _connect(self: socket.socket, address: object) -> object:
    _deny(_host_of(address), "socket.connect")
    return _original_connect(self, address)


def _connect_ex(self: socket.socket, address: object) -> object:
    _deny(_host_of(address), "socket.connect_ex")
    return _original_connect_ex(self, address)


def _create_connection(address: object, *args: object, **kwargs: object) -> object:
    _deny(address, "socket.create_connection")
    return _original_create_connection(address, *args, **kwargs)


def _getaddrinfo(host: object, port: object, *args: object, **kwargs: object) -> object:
    _deny(host, "socket.getaddrinfo")
    return _original_getaddrinfo(host, port, *args, **kwargs)


def pytest_configure(config: object) -> None:
    """Install the outbound-deny hooks for the whole session."""
    socket.socket.connect = _connect
    socket.socket.connect_ex = _connect_ex
    socket.create_connection = _create_connection
    socket.getaddrinfo = _getaddrinfo


def pytest_unconfigure(config: object) -> None:
    """Restore the stock socket API so the process can shut down cleanly."""
    socket.socket.connect = _original_connect
    socket.socket.connect_ex = _original_connect_ex
    socket.create_connection = _original_create_connection
    socket.getaddrinfo = _original_getaddrinfo
