from __future__ import annotations

import json

import scripts.maintenance.reset_oci_sequences as reset_oci_sequences
import src.cli.sync_oci as sync_oci_cli
from src.sync.sync_games import _compact_metadata_source_payload_for_limit


def test_game_metadata_source_payload_is_compacted_for_oci_varchar_limit():
    payload = {
        "start_time": "17:01",
        "stadium": "고척",
        "attendance": 11068,
        "end_time": "20:55",
        "game_time": "3:54",
        "duration_minutes": 234,
        "pbp_validation_status": "provisionally_valid",
        "pbp_validation_error": "none",
        "parser_version": "2026-05-31-v1",
        "source_schema_version": "naver-relay-v1",
        "payload_hash": "ff66d33854bb",
    }

    compacted = _compact_metadata_source_payload_for_limit(payload, 255)

    assert len(json.dumps(compacted, ensure_ascii=False)) <= 255
    assert compacted == {
        "pbp_validation_status": "provisionally_valid",
        "pbp_validation_error": "none",
        "parser_version": "2026-05-31-v1",
        "source_schema_version": "naver-relay-v1",
        "payload_hash": "ff66d33854bb",
    }
    assert "attendance" not in compacted


def test_game_metadata_source_payload_drops_long_error_for_tight_oci_limit():
    payload = {
        "pbp_validation_status": "provisionally_valid",
        "pbp_validation_error": "x" * 300,
        "parser_version": "2026-05-31-v1",
    }

    compacted = _compact_metadata_source_payload_for_limit(payload, 80)

    assert len(json.dumps(compacted, ensure_ascii=False)) <= 80
    assert compacted == {"pbp_validation_status": "provisionally_valid"}


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

        def close(self):
            calls.append("close")

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
        "close",
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

        def close(self):
            calls.append("close")

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
        "close",
        "session_exit",
        ("reset_sequences", "postgresql://oci.example/kbo"),
    ]


def test_daily_roster_sync_passes_date_scope(monkeypatch):
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

        def sync_daily_rosters(self, *, start_date=None, end_date=None):
            calls.append(("sync_daily_rosters", start_date, end_date))
            return 7

        def close(self):
            calls.append("close")

    monkeypatch.setattr(sync_oci_cli, "SessionLocal", lambda: _SessionContext())
    monkeypatch.setattr(sync_oci_cli, "OCISync", _FakeOCISync)
    monkeypatch.setattr(
        reset_oci_sequences, "reset_sequences", lambda target_url: calls.append(("reset_sequences", target_url))
    )

    sync_oci_cli.main(
        [
            "--target-url",
            "postgresql://oci.example/kbo",
            "--daily-roster",
            "--roster-date",
            "20260531",
        ]
    )

    assert calls == [
        "session_enter",
        ("init", "postgresql://oci.example/kbo", "sqlite-session"),
        ("sync_daily_rosters", "20260531", "20260531"),
        "close",
        "session_exit",
        ("reset_sequences", "postgresql://oci.example/kbo"),
    ]
