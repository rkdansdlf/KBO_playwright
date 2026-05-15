from __future__ import annotations

import asyncio
import subprocess
from types import SimpleNamespace

import src.cli.run_periodic_extras as periodic


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

    def sync_player_season_batting(self, *, year):
        self.calls.append(("season_batting", year))
        return 1

    def sync_player_season_pitching(self, *, year):
        self.calls.append(("season_pitching", year))
        return 1

    def close(self):
        self.closed = True


def test_periodic_extras_syncs_player_basic_before_dependent_stats(monkeypatch):
    _RecordingSyncer.created = []

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout="", stderr=""),
    )
    monkeypatch.setattr(periodic, "SessionLocal", lambda: _SessionContext())
    monkeypatch.setattr(periodic, "OCISync", _RecordingSyncer)
    monkeypatch.setenv("OCI_DB_URL", "postgresql://example")

    asyncio.run(periodic.run_periodic_extras(2026, sync=True))

    assert len(_RecordingSyncer.created) == 1
    syncer = _RecordingSyncer.created[0]
    assert syncer.calls == [
        "player_basic",
        "players",
        ("season_batting", 2026),
        ("season_pitching", 2026),
    ]
    assert syncer.closed is True
