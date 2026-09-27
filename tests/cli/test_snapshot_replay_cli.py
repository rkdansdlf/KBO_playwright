"""Tests for the read-only `kbo snapshot replay` command."""

from __future__ import annotations

import json

from src.cli.kbo import main as kbo_main
from src.cli.snapshot_replay import main as snapshot_main
from src.services.snapshot_replay import (
    SnapshotNotFoundError,
    SnapshotReplayError,
    SnapshotReplayResult,
    SnapshotReplayRunResult,
)


def _result(*, snapshot_id: int = 5, success: bool = True, error: str | None = None) -> SnapshotReplayResult:
    return SnapshotReplayResult(
        snapshot_id=snapshot_id,
        source_key="lg_twins_events",
        parser_version="team-event-v1",
        parsed_count=3 if success else 0,
        success=success,
        error=error,
    )


def test_single_snapshot_success_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr("src.cli.snapshot_replay.replay_snapshot", lambda _sid, **_k: _result())
    assert snapshot_main(["--snapshot-id", "5", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["snapshot_id"] == 5
    assert payload[0]["parsed_count"] == 3


def test_single_snapshot_not_found(monkeypatch, capsys) -> None:
    def _raise(*_a: object, **_k: object) -> SnapshotReplayResult:
        raise SnapshotNotFoundError(5)

    monkeypatch.setattr("src.cli.snapshot_replay.replay_snapshot", _raise)
    assert snapshot_main(["--snapshot-id", "5"]) == 1
    assert "not found" in capsys.readouterr().err


def test_single_snapshot_replay_error(monkeypatch, capsys) -> None:
    def _raise(*_a: object, **_k: object) -> SnapshotReplayResult:
        raise SnapshotReplayError("no parser registered")

    monkeypatch.setattr("src.cli.snapshot_replay.replay_snapshot", _raise)
    assert snapshot_main(["--snapshot-id", "5"]) == 2
    assert "no parser" in capsys.readouterr().err


def test_recent_batch_human_output(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "src.cli.snapshot_replay.replay_recent_snapshots",
        lambda **_k: [_result(snapshot_id=1), _result(snapshot_id=2, success=False, error="boom")],
    )
    assert snapshot_main(["--limit", "2"]) == 0
    out = capsys.readouterr().out
    assert "ok" in out
    assert "failed" in out


def test_master_cli_routes_snapshot_replay(monkeypatch, capsys) -> None:
    monkeypatch.setattr("src.cli.snapshot_replay.replay_snapshot", lambda _sid, **_k: _result())
    assert kbo_main(["snapshot", "replay", "--snapshot-id", "5"]) == 0
    assert "lg_twins_events" in capsys.readouterr().out


def _run_result(*, snapshot_id: int = 5, success: bool = True) -> SnapshotReplayRunResult:
    return SnapshotReplayRunResult(
        snapshot_id=snapshot_id,
        run_id="run-replay",
        status="success" if success else "failed",
        parsed_count=2 if success else 0,
        success=success,
    )


def test_apply_without_env_is_denied(monkeypatch, capsys) -> None:
    monkeypatch.delenv("KBO_ALLOW_SNAPSHOT_REPLAY", raising=False)
    called: list[int] = []
    monkeypatch.setattr("src.cli.snapshot_replay.record_snapshot_replay", lambda *a, **k: called.append(1))
    assert snapshot_main(["--snapshot-id", "5", "--apply"]) == 3
    assert called == []
    assert "KBO_ALLOW_SNAPSHOT_REPLAY" in capsys.readouterr().err


def test_apply_records_ledger_run_json(monkeypatch, capsys) -> None:
    monkeypatch.setenv("KBO_ALLOW_SNAPSHOT_REPLAY", "1")
    monkeypatch.setattr("src.cli.snapshot_replay.record_snapshot_replay", lambda _sid, **_k: _run_result())
    assert snapshot_main(["--snapshot-id", "5", "--apply", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["run_id"] == "run-replay"
    assert payload[0]["status"] == "success"


def test_apply_batch_records(monkeypatch, capsys) -> None:
    monkeypatch.setenv("KBO_ALLOW_SNAPSHOT_REPLAY", "1")
    monkeypatch.setattr(
        "src.cli.snapshot_replay.record_recent_snapshot_replays",
        lambda **_k: [_run_result(snapshot_id=1), _run_result(snapshot_id=2, success=False)],
    )
    assert snapshot_main(["--limit", "2", "--apply"]) == 0
    out = capsys.readouterr().out
    assert "run-replay" in out
