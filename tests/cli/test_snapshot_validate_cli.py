"""Tests for the read-only `kbo snapshot validate` command."""

from __future__ import annotations

import json

import pytest

from src.cli.kbo import main as kbo_main
from src.cli.snapshot_validate import main as validate_main
from src.services.snapshot_replay import SnapshotNotFoundError, SnapshotReplayError, SnapshotValidationResult


def _result(
    *,
    snapshot_id: int = 5,
    baseline: int | None = 3,
    replayed: int = 3,
    drifted: bool = False,
    success: bool = True,
    error: str | None = None,
) -> SnapshotValidationResult:
    return SnapshotValidationResult(
        snapshot_id=snapshot_id,
        source_key="lg_twins_events",
        baseline_count=baseline,
        replayed_count=replayed,
        delta=(replayed - baseline) if baseline is not None else None,
        drifted=drifted,
        success=success,
        error=error,
    )


def test_single_match_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr("src.cli.snapshot_validate.validate_snapshot", lambda _sid: _result())
    assert validate_main(["--snapshot-id", "5", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["drifted"] is False
    assert payload[0]["delta"] == 0


def test_fail_on_drift_returns_non_zero(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "src.cli.snapshot_validate.validate_snapshot",
        lambda _sid: _result(baseline=5, replayed=3, drifted=True),
    )
    assert validate_main(["--snapshot-id", "5", "--fail-on-drift"]) == 3
    assert "DRIFT" in capsys.readouterr().out


def test_drift_without_flag_is_zero_exit(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "src.cli.snapshot_validate.validate_snapshot",
        lambda _sid: _result(baseline=5, replayed=3, drifted=True),
    )
    assert validate_main(["--snapshot-id", "5"]) == 0


def test_not_found(monkeypatch, capsys) -> None:
    def _raise(_sid: int) -> SnapshotValidationResult:
        raise SnapshotNotFoundError(5)

    monkeypatch.setattr("src.cli.snapshot_validate.validate_snapshot", _raise)
    assert validate_main(["--snapshot-id", "5"]) == 1
    assert "not found" in capsys.readouterr().err


def test_replay_error(monkeypatch, capsys) -> None:
    def _raise(_sid: int) -> SnapshotValidationResult:
        raise SnapshotReplayError("no parser registered")

    monkeypatch.setattr("src.cli.snapshot_validate.validate_snapshot", _raise)
    assert validate_main(["--snapshot-id", "5"]) == 2
    assert "no parser" in capsys.readouterr().err


def test_recent_batch_human(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "src.cli.snapshot_validate.validate_recent_snapshots",
        lambda **_k: [_result(snapshot_id=1), _result(snapshot_id=2, baseline=None, replayed=4)],
    )
    assert validate_main(["--limit", "2"]) == 0
    out = capsys.readouterr().out
    assert "match" in out
    assert "no baseline" in out


def test_master_cli_routes_snapshot_validate(monkeypatch, capsys) -> None:
    monkeypatch.setattr("src.cli.snapshot_validate.validate_snapshot", lambda _sid: _result())
    assert kbo_main(["snapshot", "validate", "--snapshot-id", "5"]) == 0
    assert "lg_twins_events" in capsys.readouterr().out


def test_explicit_limit_zero_is_honored(monkeypatch) -> None:
    captured: dict[str, int] = {}

    def _fake(*, limit: int, **_k: object) -> list[SnapshotValidationResult]:
        captured["limit"] = limit
        return []

    monkeypatch.setattr("src.cli.snapshot_validate.validate_recent_snapshots", _fake)
    assert validate_main(["--limit", "0"]) == 0
    assert captured["limit"] == 0


def test_limit_defaults_to_fifty(monkeypatch) -> None:
    captured: dict[str, int] = {}

    def _fake(*, limit: int, **_k: object) -> list[SnapshotValidationResult]:
        captured["limit"] = limit
        return []

    monkeypatch.setattr("src.cli.snapshot_validate.validate_recent_snapshots", _fake)
    assert validate_main([]) == 0
    assert captured["limit"] == 50


def test_negative_limit_is_rejected(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        validate_main(["--limit", "-1"])
    assert exc.value.code == 2
    assert ">= 0" in capsys.readouterr().err
