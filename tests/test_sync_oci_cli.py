from __future__ import annotations

import scripts.maintenance.reset_oci_sequences as reset_oci_sequences
import src.cli.sync_oci as sync_oci_cli


def test_players_sync_runs_player_basic_before_master_players(monkeypatch):
    calls = []

    class _SessionContext:
        def __enter__(self):
            calls.append("session_enter")
            return "sqlite-session"

        def __exit__(self, exc_type, exc, tb):
            calls.append("session_exit")
            return False

    class _FakeOCISync:
        def __init__(self, target_url, session):
            calls.append(("init", target_url, session))

        def sync_player_basic(self):
            calls.append("sync_player_basic")
            return 1

        def sync_players(self):
            calls.append("sync_players")
            return 1

    monkeypatch.setattr(sync_oci_cli, "SessionLocal", lambda: _SessionContext())
    monkeypatch.setattr(sync_oci_cli, "OCISync", _FakeOCISync)
    monkeypatch.setattr(
        reset_oci_sequences, "reset_sequences", lambda target_url: calls.append(("reset_sequences", target_url))
    )

    sync_oci_cli.main(["--target-url", "postgresql://oci.example/kbo", "--players"])

    assert calls == [
        "session_enter",
        ("init", "postgresql://oci.example/kbo", "sqlite-session"),
        "sync_player_basic",
        "sync_players",
        "session_exit",
        ("reset_sequences", "postgresql://oci.example/kbo"),
    ]


def test_teams_sync_runs_reference_tables_in_dependency_order(monkeypatch):
    calls = []

    class _SessionContext:
        def __enter__(self):
            calls.append("session_enter")
            return "sqlite-session"

        def __exit__(self, exc_type, exc, tb):
            calls.append("session_exit")
            return False

    class _FakeOCISync:
        def __init__(self, target_url, session):
            calls.append(("init", target_url, session))

        def sync_franchises(self):
            calls.append("sync_franchises")
            return 1

        def sync_teams(self):
            calls.append("sync_teams")
            return 1

        def sync_team_history(self):
            calls.append("sync_team_history")
            return 1

        def sync_team_code_map(self):
            calls.append("sync_team_code_map")
            return 1

    monkeypatch.setattr(sync_oci_cli, "SessionLocal", lambda: _SessionContext())
    monkeypatch.setattr(sync_oci_cli, "OCISync", _FakeOCISync)
    monkeypatch.setattr(
        reset_oci_sequences, "reset_sequences", lambda target_url: calls.append(("reset_sequences", target_url))
    )

    sync_oci_cli.main(["--target-url", "postgresql://oci.example/kbo", "--teams"])

    assert calls == [
        "session_enter",
        ("init", "postgresql://oci.example/kbo", "sqlite-session"),
        "sync_franchises",
        "sync_teams",
        "sync_team_history",
        "sync_team_code_map",
        "session_exit",
        ("reset_sequences", "postgresql://oci.example/kbo"),
    ]
