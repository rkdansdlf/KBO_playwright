from __future__ import annotations

from pathlib import Path

import scripts.auto_recover_2020 as recovery


def _result(game_id: str, ok: bool) -> recovery.GameRunResult:
    return recovery.GameRunResult(game_id, ok, 0 if ok else 1, None if ok else "failed", None, None)


def test_recovery_succeeds_when_scores_and_core_details_exist() -> None:
    snapshot = recovery.GameSnapshot("G1", "COMPLETED", 1, 2, 9, 3, 9, 10, 20)

    assert recovery.recovery_succeeded(snapshot) is True


def test_recovery_rejects_missing_core_details() -> None:
    snapshot = recovery.GameSnapshot("G1", "COMPLETED", 1, 2, 9, 0, 9, 10, 20)

    assert recovery.recovery_succeeded(snapshot) is False


def test_process_batches_stops_after_three_consecutive_failures(monkeypatch) -> None:
    calls: list[str] = []

    def run_one(game_id: str, _timeout: int) -> recovery.GameRunResult:
        calls.append(game_id)
        return _result(game_id, False)

    monkeypatch.setattr(recovery, "run_one_game", run_one)

    results, reason = recovery._process_game_batches(["G1", "G2", "G3", "G4"], batch_size=10, timeout=1)

    assert [result.game_id for result in results] == ["G1", "G2", "G3"]
    assert calls == ["G1", "G2", "G3"]
    assert reason == "three_consecutive_failures"


def test_run_recovery_pauses_and_restarts_running_scheduler(monkeypatch, tmp_path: Path) -> None:
    events: list[str] = []
    monkeypatch.setattr(recovery, "scheduler_is_running", lambda: True)
    monkeypatch.setattr(recovery, "pause_scheduler", lambda: events.append("pause"))
    monkeypatch.setattr(recovery, "start_scheduler", lambda: events.append("start"))
    monkeypatch.setattr(recovery, "load_unresolved_game_ids", list)

    report = recovery.run_recovery(output_dir=tmp_path, timeout=1)

    assert events == ["pause", "start"]
    assert report["scheduler_was_running"] is True
    assert report["total_attempted"] == 0
    assert list(tmp_path.glob("auto_recover_2020_*.json"))
