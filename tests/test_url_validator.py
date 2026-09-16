"""Tests for URL validation and SSRF prevention utilities."""

from __future__ import annotations

import socket

import pytest

from src.utils import url_validator

from src.utils.url_validator import is_private_ip, validate_crawler_url, validate_url


class TestIsPrivateIP:
    """Test private/reserved IP detection."""

    @pytest.mark.parametrize(
        "ip",
        [
            "127.0.0.1",
            "10.0.0.1",
            "10.255.255.255",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.0.1",
            "192.168.1.100",
            "169.254.169.254",  # Cloud metadata
            "0.0.0.0",
            "224.0.0.1",  # Multicast
            "255.255.255.255",  # Broadcast
        ],
    )
    def test_private_ips_are_detected(self, ip: str) -> None:
        assert is_private_ip(ip) is True

    @pytest.mark.parametrize(
        "ip",
        [
            "8.8.8.8",
            "1.1.1.1",
            "203.235.200.46",  # Example public IP
            "142.250.80.46",  # google.com
        ],
    )
    def test_public_ips_are_allowed(self, ip: str) -> None:
        assert is_private_ip(ip) is False

    def test_invalid_ip_treated_as_private(self) -> None:
        assert is_private_ip("not-an-ip") is True


class TestValidateUrl:
    """Test URL validation logic."""

    def test_valid_https_url(self) -> None:
        ok, reason = validate_url("https://example.com/path", resolve_dns=False)
        assert ok is True
        assert reason == "OK"

    def test_valid_http_url(self) -> None:
        ok, reason = validate_url("http://example.com/path", resolve_dns=False)
        assert ok is True
        assert reason == "OK"

    @pytest.mark.parametrize(
        "url",
        [
            "file:///etc/passwd",
            "gopher://evil.com",
            "ftp://ftp.example.com/file",
            "dict://attacker.com:1234/",
        ],
    )
    def test_blocked_schemes(self, url: str) -> None:
        ok, reason = validate_url(url, resolve_dns=False)
        assert ok is False
        assert "Blocked scheme" in reason

    def test_missing_scheme(self) -> None:
        ok, reason = validate_url("example.com/path", resolve_dns=False)
        assert ok is False

    def test_missing_hostname(self) -> None:
        ok, reason = validate_url("https://", resolve_dns=False)
        assert ok is False

    def test_domain_allowlist_blocks_unknown(self) -> None:
        ok, reason = validate_url(
            "https://evil.com/steal",
            domain_allowlist=frozenset({"example.com"}),
            resolve_dns=False,
        )
        assert ok is False
        assert "not in allowlist" in reason

    def test_domain_allowlist_allows_listed(self) -> None:
        ok, reason = validate_url(
            "https://example.com/api",
            domain_allowlist=frozenset({"example.com"}),
            resolve_dns=False,
        )
        assert ok is True

    def test_domain_allowlist_allows_subdomain(self) -> None:
        ok, reason = validate_url(
            "https://api.example.com/v1",
            domain_allowlist=frozenset({"example.com"}),
            resolve_dns=False,
        )
        assert ok is True

    def test_none_allowlist_skips_check(self) -> None:
        ok, reason = validate_url(
            "https://any-domain.com/path",
            domain_allowlist=None,
            resolve_dns=False,
        )
        assert ok is True

    def test_allow_private_flag_bypasses_ip_check(self) -> None:
        ok, reason = validate_url(
            "http://127.0.0.1:8080/internal",
            allow_private=True,
            resolve_dns=True,
        )
        assert ok is True


@pytest.mark.parametrize("ip", ["ff02::1", "::ffff:127.0.0.1", "100.64.0.1"])
def test_nonpublic_addresses_blocked(ip):
    assert is_private_ip(ip)


@pytest.mark.parametrize("url", ["http://127.0.0.1", "http://[::1]", "http://169.254.169.254"])
def test_literal_private_ip_blocked_without_dns(url):
    assert validate_url(url, resolve_dns=False)[0] is False


@pytest.mark.parametrize("url", ["https://example.com:bad", "https://example.com:99999", "https://[::1"])
def test_malformed_url_returns_failure(url):
    assert validate_url(url, resolve_dns=False)[0] is False


def test_empty_schemes_fail_closed():
    assert validate_url("https://example.com", allowed_schemes=frozenset(), resolve_dns=False)[0] is False


@pytest.mark.parametrize("addresses", [[], ["8.8.8.8", "127.0.0.1"], ["ff02::1"]])
def test_dns_fail_closed(monkeypatch, addresses):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: [(2, 1, 6, "", (ip, 443)) for ip in addresses])
    assert validate_url("https://example.com")[0] is False


def test_public_dns_allowed(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: [(2, 1, 6, "", ("8.8.8.8", 443))])
    assert validate_url("https://example.com")[0] is True


def test_dns_error_fails_closed(monkeypatch):
    def fail(*args, **kwargs):
        raise socket.gaierror("unavailable")

    monkeypatch.setattr(socket, "getaddrinfo", fail)
    assert validate_url("https://example.com")[0] is False


def test_validation_does_not_log_url_secrets(monkeypatch, caplog):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: [(2, 1, 6, "", ("127.0.0.1", 443))])
    ok, reason = url_validator.validate_url("https://example.com/private-token?api_key=secret-value")
    assert not ok
    assert "private-token" not in caplog.text + reason
    assert "secret-value" not in caplog.text + reason


class TestValidateCrawlerUrl:
    """Test crawler-specific URL validation with domain allowlist."""

    def test_kbo_official_allowed(self) -> None:
        ok, reason = validate_crawler_url(
            "https://www.koreabaseball.com/Schedule/Schedule.aspx",
            allow_private=True,
        )
        assert ok is True

    def test_naver_sports_allowed(self) -> None:
        ok, reason = validate_crawler_url(
            "https://sports.naver.com/kbaseball/record",
            allow_private=True,
        )
        assert ok is True

    def test_wikipedia_allowed(self) -> None:
        ok, reason = validate_crawler_url(
            "https://ko.wikipedia.org/wiki/KBO",
            allow_private=True,
        )
        assert ok is True

    def test_unknown_domain_blocked(self) -> None:
        ok, reason = validate_crawler_url(
            "https://attacker.com/phish",
            allow_private=True,
        )
        assert ok is False
        assert "not in allowlist" in reason

    def test_file_scheme_blocked(self) -> None:
        ok, reason = validate_crawler_url("file:///etc/passwd")
        assert ok is False
        assert "Blocked scheme" in reason

    def test_internal_metadata_blocked(self) -> None:
        ok, reason = validate_crawler_url("http://169.254.169.254/latest/meta-data/")
        assert ok is False
        # Either "not in allowlist" or "private IP" depending on resolution
        assert ok is False
