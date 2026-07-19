from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from src.cli.historical_import import main


def _write_archive(tmp_path: Path, *, game_id: str, payload: str, season: int = 2001) -> Path:
    payload_path = tmp_path / f"{game_id}.json"
    payload_path.write_text(payload, encoding="utf-8")
    manifest_path = tmp_path / "manifest.csv"
    manifest_path.write_text(
        "game_id,season,source_type,locator,format,priority,sha256,captured_at,notes\n"
        f"{game_id},{season},json_archive,{payload_path.name},normalized_events_json,1,"
        f"{hashlib.sha256(payload.encode()).hexdigest()},2026-07-19T00:00:00Z,fixture\n",
        encoding="utf-8",
    )
    return manifest_path


def test_main_writes_valid_dry_run_report(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    game_id = "20010405LTHU0"
    manifest_path = _write_archive(
        tmp_path,
        game_id=game_id,
        payload='{"events": [{"id": 1}], "raw_pbp_rows": [{"text": "play"}]}',
    )
    report_path = tmp_path / "report.json"

    assert main(["--manifest", str(manifest_path), "--dry-run", "--report-out", str(report_path)]) == 0

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["summary"] == {"selected_games": 1, "valid": 1, "empty": 0, "errors": 0}
    assert report["games"][0]["game_id"] == game_id
    assert report["games"][0]["event_rows"] == 1
    assert report["games"][0]["pbp_rows"] == 1
    assert "dry-run" in capsys.readouterr().out


def test_main_strict_fails_for_checksum_error(tmp_path: Path) -> None:
    game_id = "20010405LTHU0"
    manifest_path = tmp_path / "manifest.csv"
    payload_path = tmp_path / f"{game_id}.json"
    payload_path.write_text('{"events": [{"id": 1}]}', encoding="utf-8")
    manifest_path.write_text(
        "game_id,season,source_type,locator,format,priority,sha256,captured_at,notes\n"
        f"{game_id},2001,json_archive,{payload_path.name},normalized_events_json,1,"
        f"{'0' * 64},2026-07-19T00:00:00Z,tampered\n",
        encoding="utf-8",
    )

    assert main(["--manifest", str(manifest_path), "--dry-run", "--strict"]) == 1


def test_main_filters_by_season_and_game_id(tmp_path: Path) -> None:
    game_a = "20010405LTHU0"
    game_b = "20020405HTOB0"
    payload_a = '{"events": [{"id": 1}]}'
    payload_b = '{"events": [{"id": 2}]}'
    path_a = tmp_path / "a.json"
    path_b = tmp_path / "b.json"
    path_a.write_text(payload_a, encoding="utf-8")
    path_b.write_text(payload_b, encoding="utf-8")
    manifest_path = tmp_path / "manifest.csv"
    manifest_path.write_text(
        "game_id,season,source_type,locator,format,priority,sha256,captured_at,notes\n"
        f"{game_a},2001,json_archive,{path_a.name},normalized_events_json,1,"
        f"{hashlib.sha256(payload_a.encode()).hexdigest()},,\n"
        f"{game_b},2002,json_archive,{path_b.name},normalized_events_json,1,"
        f"{hashlib.sha256(payload_b.encode()).hexdigest()},,\n",
        encoding="utf-8",
    )

    report_path = tmp_path / "filtered.json"
    assert (
        main(
            [
                "--manifest",
                str(manifest_path),
                "--dry-run",
                "--season",
                "2002",
                "--game-ids",
                game_b,
                "--report-out",
                str(report_path),
            ],
        )
        == 0
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert [item["game_id"] for item in report["games"]] == [game_b]
