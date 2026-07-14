from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from src.cli import refresh_source_snapshots as module


class _SessionContext:
    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False

    def __enter__(self) -> _SessionContext:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


def _source(key: str = "lg_twins_ticket", *, active: bool = True) -> SimpleNamespace:
    return SimpleNamespace(
        source_key=key,
        base_url="https://example.com",
        is_active=active,
        last_success_at=None,
    )


def test_dry_run_outputs_selected_source(monkeypatch, capsys):
    source = _source()
    session = _SessionContext()

    class Repo:
        def __init__(self, _session):
            pass

        def get_by_key(self, _key):
            return source

    monkeypatch.setattr(module, "SessionLocal", lambda: session)
    monkeypatch.setattr(module, "DataSourceRepository", Repo)

    result = module.main(["--source-key", "lg_twins_ticket", "--dry-run", "--json"])

    assert result == 0
    assert "lg_twins_ticket" in capsys.readouterr().out
    assert not session.committed


def test_refresh_saves_successful_snapshot(monkeypatch):
    source = _source()
    session = _SessionContext()
    calls = []

    class Repo:
        def __init__(self, _session):
            pass

        def get_active_by_domain(self, _domain):
            return [source]

    async def fake_fetch(_source_obj, _client, *, use_playwright_fallback):
        calls.append(use_playwright_fallback)
        return module.FetchedPage(
            source_key=source.source_key,
            url="https://example.com",
            html="<html></html>",
            status_code=200,
            method="httpx",
        )

    monkeypatch.setattr(module, "SessionLocal", lambda: session)
    monkeypatch.setattr(module, "DataSourceRepository", Repo)
    monkeypatch.setattr(module, "_fetch_source", fake_fetch)
    monkeypatch.setattr(module, "save_raw_snapshots", lambda _session, _pages: 1)

    result = module.main(["--domain", "ticket"])

    assert result == 0
    assert session.committed
    assert calls == [True]


def test_refresh_returns_failure_for_bad_source(monkeypatch):
    source = _source("blocked_source")
    session = _SessionContext()

    class Repo:
        def __init__(self, _session):
            pass

        def get_all_active(self):
            return [source]

    async def fake_fetch(_source_obj, _client, *, use_playwright_fallback):
        return module.FetchedPage(
            source_key=source.source_key,
            url="https://blocked.example",
            html="Forbidden",
            status_code=403,
            method="playwright",
        )

    monkeypatch.setattr(module, "SessionLocal", lambda: session)
    monkeypatch.setattr(module, "DataSourceRepository", Repo)
    monkeypatch.setattr(module, "_fetch_source", fake_fetch)

    result = module.main(["--all"])

    assert result == 1
    assert not session.committed


def test_filter_sources_respects_max_hours():
    args = argparse.Namespace(max_hours=24)
    sources = [_source("active"), _source("inactive", active=False)]

    result = module._filter_sources(sources, args.max_hours)

    assert [source.source_key for source in result] == ["active"]


def test_source_selection_and_staleness_branches():
    now = datetime.now(UTC).replace(tzinfo=None)
    fresh = _source("fresh")
    fresh.last_success_at = now - timedelta(hours=1)
    old = _source("old")
    old.last_success_at = now - timedelta(hours=48)
    repo = SimpleNamespace(
        get_by_key=lambda _key: fresh,
        get_active_by_domain=lambda _domain: [old],
        get_all_active=lambda: [fresh, old],
    )

    assert module._select_sources(repo, argparse.Namespace(source_key="fresh", domain=None)) == [fresh]
    assert module._select_sources(repo, argparse.Namespace(source_key=None, domain="ticket")) == [old]
    assert module._select_sources(repo, argparse.Namespace(source_key=None, domain=None)) == [fresh, old]
    assert module._is_stale(fresh, 24) is False
    assert module._is_stale(old, 24) is True
    assert module._is_stale(fresh, None) is True
    assert module._host_for_url("not a URL") == "unknown"


@pytest.mark.asyncio
async def test_fetch_with_httpx_validates_url_and_waits(monkeypatch):
    source = _source()
    response = SimpleNamespace(url="https://redirected.example", text="body", status_code=200)
    client = MagicMock()
    client.get = AsyncMock(return_value=response)
    wait = AsyncMock()
    monkeypatch.setattr(module.throttle, "wait", wait)

    page = await module._fetch_with_httpx(source, client)

    assert page == module.FetchedPage("lg_twins_ticket", "https://redirected.example", "body", 200, "httpx")
    wait.assert_awaited_once_with("example.com")
    client.get.assert_awaited_once_with("https://example.com")

    source.base_url = ""
    with pytest.raises(ValueError, match="base_url is empty"):
        await module._fetch_with_httpx(source, client)


@pytest.mark.asyncio
async def test_fetch_with_playwright_releases_resources(monkeypatch):
    source = _source()
    page = AsyncMock()
    page.url = "https://rendered.example"
    page.goto.return_value = SimpleNamespace(status=201)
    page.content.return_value = "<html>rendered</html>"
    pool = MagicMock()
    pool.start = AsyncMock()
    pool.acquire = AsyncMock(return_value=page)
    pool.release = AsyncMock()
    pool.close = AsyncMock()
    monkeypatch.setattr(module, "AsyncPlaywrightPool", lambda **_kwargs: pool)

    result = await module._fetch_with_playwright(source)

    assert result.status_code == 201
    assert result.method == "playwright"
    pool.start.assert_awaited_once()
    pool.release.assert_awaited_once_with(page)
    pool.close.assert_awaited_once()

    page.goto.return_value = None
    result = await module._fetch_with_playwright(source)
    assert result.status_code == 200


@pytest.mark.asyncio
async def test_fetch_source_handles_httpx_error_and_status_fallback(monkeypatch):
    source = _source()
    client = MagicMock()
    fallback_page = module.FetchedPage("lg_twins_ticket", source.base_url, "rendered", 200, "playwright")

    async def raise_http_error(*_args, **_kwargs):
        raise httpx.ConnectError("TLS failure")

    async def return_blocked(*_args, **_kwargs):
        return module.FetchedPage("lg_twins_ticket", source.base_url, "blocked", 403, "httpx")

    async def return_fallback(*_args, **_kwargs):
        return fallback_page

    monkeypatch.setattr(module, "_fetch_with_httpx", raise_http_error)
    with pytest.raises(httpx.ConnectError):
        await module._fetch_source(source, client, use_playwright_fallback=False)

    monkeypatch.setattr(module, "_fetch_with_playwright", return_fallback)
    result = await module._fetch_source(source, client, use_playwright_fallback=True)
    assert result == fallback_page

    monkeypatch.setattr(module, "_fetch_with_httpx", return_blocked)
    result = await module._fetch_source(source, client, use_playwright_fallback=False)
    assert result.status_code == 403
    result = await module._fetch_source(source, client, use_playwright_fallback=True)
    assert result == fallback_page


def test_refresh_rolls_back_fetch_errors_and_writes_log_output(monkeypatch, caplog):
    source = _source("error_source")
    session = _SessionContext()

    class Repo:
        def __init__(self, _session):
            pass

        def get_all_active(self):
            return [source]

    async def failing_fetch(*_args, **_kwargs):
        raise ValueError("bad source")

    monkeypatch.setattr(module, "SessionLocal", lambda: session)
    monkeypatch.setattr(module, "DataSourceRepository", Repo)
    monkeypatch.setattr(module, "_fetch_source", failing_fetch)

    result = module.main(["--all", "--delay", "0", "--no-playwright-fallback"])

    assert result == 1
    assert session.rolled_back
    assert module.throttle.default_delay == 0

    caplog.clear()
    module._write_results(
        [
            module.RefreshResult("saved", "saved", method="httpx", status_code=200, snapshots_saved=1),
            module.RefreshResult("preview", "dry_run", url="https://example.com"),
            module.RefreshResult("failed", "failed", error="network"),
        ],
        json_output=False,
    )
    assert "saved" in caplog.text
    assert "preview" in caplog.text
    assert "failed" in caplog.text
