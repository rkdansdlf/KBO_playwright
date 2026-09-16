"""URL validation utilities for SSRF prevention.

Provides domain allowlisting, private IP blocking, and protocol restriction
to protect crawlers and API endpoints from Server-Side Request Forgery.
"""

from __future__ import annotations

import ipaddress
import logging
import socket
from ipaddress import IPv4Address, IPv4Network, IPv6Address
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# ── Private / Reserved IP ranges that MUST be blocked ─────────────────────────
_BLOCKED_NETWORKS: list[IPv4Network] = [
    IPv4Network("0.0.0.0/8"),
    IPv4Network("10.0.0.0/8"),
    IPv4Network("100.64.0.0/10"),  # Carrier-grade NAT
    IPv4Network("127.0.0.0/8"),  # Loopback
    IPv4Network("169.254.0.0/16"),  # Link-local (includes cloud metadata 169.254.169.254)
    IPv4Network("172.16.0.0/12"),
    IPv4Network("192.0.0.0/24"),
    IPv4Network("192.0.2.0/24"),  # TEST-NET-1
    IPv4Network("192.88.99.0/24"),  # 6to4 relay
    IPv4Network("192.168.0.0/16"),
    IPv4Network("198.18.0.0/15"),  # Benchmarking
    IPv4Network("198.51.100.0/24"),  # TEST-NET-2
    IPv4Network("203.0.113.0/24"),  # TEST-NET-3
    IPv4Network("224.0.0.0/4"),  # Multicast
    IPv4Network("240.0.0.0/4"),  # Reserved
    IPv4Network("255.255.255.255/32"),
]

# ── Allowed URL schemes ───────────────────────────────────────────────────────
_ALLOWED_SCHEMES: frozenset[str] = frozenset({"http", "https"})

# ── Domain allowlist for crawlers ─────────────────────────────────────────────
_DOMAIN_ALLOWLIST: frozenset[str] = frozenset(
    {
        # KBO official
        "koreabaseball.com",
        "www.koreabaseball.com",
        "m.koreabaseball.com",
        "futuresleague.koreabaseball.com",
        # Naver Sports
        "sports.naver.com",
        "api-gw.sports.naver.com",
        "m.sports.naver.com",
        "openapi.naver.com",
        # Wikipedia
        "ko.wikipedia.org",
        "en.wikipedia.org",
        # YouTube / Google APIs
        "www.googleapis.com",
        "youtube.googleapis.com",
        # Seoul Open Data
        "openapi.seoul.go.kr",
        # Yagoonara (award data)
        "yagoonara.co.kr",
        "www.yagoonara.co.kr",
        # NamuWiki
        "namu.wiki",
        # Team sites (operation notices)
        "www.doosanbears.com",
        "www.lgtwins.com",
        # Alerting destinations
        "api.telegram.org",
        "hooks.slack.com",
    }
)


def is_private_ip(ip_str: str) -> bool:
    """Return True if the IP address belongs to a private/reserved range.

    Args:
        ip_str: IP address string to check.

    Returns:
        True if the address is private, reserved, loopback, or link-local.

    """
    try:
        addr = ipaddress.ip_address(ip_str)
    except ValueError:
        return True  # Treat unparseable addresses as suspicious

    if isinstance(addr, IPv6Address):
        if addr.ipv4_mapped is not None:
            return is_private_ip(str(addr.ipv4_mapped))
        return not addr.is_global or addr.is_multicast or addr.is_reserved

    if isinstance(addr, IPv4Address):
        return any(addr in net for net in _BLOCKED_NETWORKS)

    return True  # Unknown type → block


def _validate_scheme_and_host(url: str, schemes: frozenset[str]) -> tuple[bool, str, str | None]:
    """Parse URL and validate scheme and hostname presence.

    Returns:
        Tuple of (is_valid, reason, hostname_or_none).

    """
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname
        _ = parsed.port
    except ValueError:
        return False, "URL parsing failed", None

    if not parsed.scheme or parsed.scheme not in schemes:
        label = f"'{parsed.scheme}'" if parsed.scheme else "(missing)"
        return False, f"Blocked scheme {label} (allowed: {', '.join(sorted(schemes))})", None

    if not hostname:
        return False, "Missing hostname in URL", None

    return True, "OK", hostname


def _check_domain_allowlist(hostname: str, domain_allowlist: frozenset[str]) -> tuple[bool, str]:
    """Return (True, 'OK') if hostname is in the allowlist or is a subdomain of one."""
    normalized = hostname.lower().removeprefix("www.")
    if hostname.lower() in domain_allowlist or f"www.{normalized}" in domain_allowlist:
        return True, "OK"

    is_subdomain = any(
        hostname.lower().endswith(f".{d}") or hostname.lower().endswith(f".{d.removeprefix('www.')}")
        for d in domain_allowlist
    )
    if is_subdomain:
        return True, "OK"
    return False, f"Domain '{hostname}' not in allowlist"


def _check_host_ip(hostname: str, *, resolve_dns: bool) -> tuple[bool, str]:
    """Reject nonpublic literals and optionally check resolved host addresses."""
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        if resolve_dns:
            return _check_resolved_ips(hostname)
        return True, "OK"
    if is_private_ip(str(address)):
        return False, "Blocked private/reserved IP address"
    return True, "OK"


def _check_resolved_ips(hostname: str) -> tuple[bool, str]:
    """Resolve hostname via DNS and reject private/reserved IPs."""
    try:
        resolved_ips = socket.getaddrinfo(hostname, 443, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        return False, f"DNS resolution failed for: {hostname}"

    if not resolved_ips:
        return False, f"DNS resolution returned no addresses for: {hostname}"

    for _family, _type, _proto, _canonname, sockaddr in resolved_ips:
        ip_str = sockaddr[0]
        if is_private_ip(ip_str):
            logger.warning(
                "[SSRF_BLOCK] Host %s resolved to private IP %s - blocked",
                hostname,
                ip_str,
            )
            return False, f"Resolved to private/reserved IP: {ip_str}"

    return True, "OK"


def validate_url(
    url: str,
    *,
    allow_private: bool = False,
    allowed_schemes: frozenset[str] | None = None,
    domain_allowlist: frozenset[str] | None = None,
    resolve_dns: bool = True,
) -> tuple[bool, str]:
    """Validate a URL for safe HTTP request targeting.

    Perform three-layer validation:
      1. Scheme restriction (only http/https by default)
      2. Domain allowlist check (if provided)
      3. DNS resolution and private IP blocking.

    DNS checks are preflight only; connection-time DNS rebinding requires
    transport-level address pinning or network egress restrictions.

    Args:
        url: Target URL to validate.
        allow_private: If True, skip private IP blocking (for local dev).
        allowed_schemes: Override default allowed schemes.
        domain_allowlist: Override default domain allowlist.  Pass None to skip.
        resolve_dns: If True, resolve hostname to IP and check for private ranges.

    Returns:
        Tuple of (is_safe: bool, reason: str).

    """
    schemes = _ALLOWED_SCHEMES if allowed_schemes is None else allowed_schemes

    ok, reason, hostname = _validate_scheme_and_host(url, schemes)
    if not ok or hostname is None:
        return False, reason

    if domain_allowlist is not None:
        ok, reason = _check_domain_allowlist(hostname, domain_allowlist)
        if not ok:
            return False, reason

    if not allow_private:
        return _check_host_ip(hostname, resolve_dns=resolve_dns)

    return True, "OK"


def validate_crawler_url(url: str, *, allow_private: bool = False) -> tuple[bool, str]:
    """Validate a crawler target URL against the built-in domain allowlist.

    Args:
        url: Target crawl URL.
        allow_private: Allow private IPs (for local development).

    Returns:
        Tuple of (is_safe, reason).

    """
    return validate_url(
        url,
        allow_private=allow_private,
        domain_allowlist=_DOMAIN_ALLOWLIST,
    )


__all__ = [
    "is_private_ip",
    "validate_crawler_url",
    "validate_url",
]
