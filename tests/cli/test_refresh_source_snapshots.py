from __future__ import annotations

import argparse
from types import SimpleNamespace

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
