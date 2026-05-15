from __future__ import annotations

import asyncio
from types import SimpleNamespace

import src.cli.run_weekly_maintenance as weekly


class _SessionContext:
    def __enter__(self):
        return "sqlite-session"

    def __exit__(self, exc_type, exc, tb):
        return False


class _RecordingSyncer:
    created = []

    def __init__(self, oci_url, session):
        self.oci_url = oci_url
        self.session = session
        self.calls = []
        self.closed = False
        _RecordingSyncer.created.append(self)

    def sync_player_basic(self):
        self.calls.append("player_basic")
        return 1

    def sync_players(self):
        self.calls.append("players")
        return 1

    def close(self):
        self.closed = True


def test_weekly_maintenance_syncs_player_basic_before_players(monkeypatch):
    _RecordingSyncer.created = []

    async def _collect_profiles(*_args, **_kwargs):
        return None

    monkeypatch.setattr(weekly, "collect_profiles", _collect_profiles)
    monkeypatch.setattr(weekly, "healthcheck_main", lambda _argv: None)
    monkeypatch.setattr(
        weekly.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout="", stderr=""),
    )
    monkeypatch.setattr(weekly, "SessionLocal", lambda: _SessionContext())
    monkeypatch.setattr(weekly, "OCISync", _RecordingSyncer)
    monkeypatch.setenv("OCI_DB_URL", "postgresql://example")

    asyncio.run(weekly.run_weekly_maintenance(profile_limit=1, sync=True))

    assert len(_RecordingSyncer.created) == 1
    syncer = _RecordingSyncer.created[0]
    assert syncer.calls == ["player_basic", "players"]
    assert syncer.closed is True
