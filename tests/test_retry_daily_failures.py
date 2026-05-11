from __future__ import annotations

import json
from pathlib import Path

import src.cli.retry_daily_failures as retry_cli


def _write_summary(tmp_path: Path, date: str, *, detail=None, relay=None):
    payload = {
        "phase": "postgame_finalize",
        "target_date": date,
        "stability": {
            "retry_candidates": {
                "detail": list(detail or []),
                "relay": list(relay or []),
            }
        },
    }
    path = tmp_path / f"{date}.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_build_retry_commands_groups_detail_candidates_by_month_and_adds_relay_and_sync():
    summary = {
        "stability": {
            "retry_candidates": {
                "detail": ["20250201LGSS0", "20250101KTSS0", "20250101KTSS0"],
                "relay": ["20250101KTSS0"],
            }
        }
    }

    commands = retry_cli.build_retry_commands(summary, sync=True, python_bin="python")

    assert commands == [
        [
            "python",
            "-m",
            "src.cli.crawl_game_details",
            "--year",
            "2025",
            "--month",
            "1",
            "--game-ids",
            "20250101KTSS0",
            "--force",
            "--concurrency",
            "1",
        ],
        [
            "python",
            "-m",
            "src.cli.crawl_game_details",
            "--year",
            "2025",
            "--month",
            "2",
            "--game-ids",
            "20250201LGSS0",
            "--force",
            "--concurrency",
            "1",
        ],
        [
            "python",
            "scripts/fetch_kbo_pbp.py",
            "--game-ids",
            "20250101KTSS0",
            "--force",
        ],
        [
            "python",
            "-m",
            "src.cli.sync_oci",
            "--game-details",
            "--game-ids",
            "20250101KTSS0,20250201LGSS0",
        ],
    ]


def test_run_retry_dry_run_does_not_execute_commands(tmp_path, capsys):
    _write_summary(tmp_path, "20250101", detail=["20250101LGSS0"])
    calls = []

    code = retry_cli.run_retry(
        target_date="20250101",
        summary_dir=tmp_path,
        apply=False,
        runner=lambda command: calls.append(list(command)),
        python_bin="python",
    )

    output = capsys.readouterr().out
    assert code == 0
    assert calls == []
    assert "Dry run only" in output
    assert "python -m src.cli.crawl_game_details" in output


def test_run_retry_apply_executes_commands_in_order(tmp_path):
    _write_summary(
        tmp_path,
        "20250101",
        detail=["20250101LGSS0"],
        relay=["20250101LGSS0"],
    )
    calls = []

    code = retry_cli.run_retry(
        target_date="20250101",
        summary_dir=tmp_path,
        apply=True,
        sync=True,
        runner=lambda command: calls.append(list(command)),
        python_bin="python",
    )

    assert code == 0
    assert [command[:3] for command in calls] == [
        ["python", "-m", "src.cli.crawl_game_details"],
        ["python", "scripts/fetch_kbo_pbp.py", "--game-ids"],
        ["python", "-m", "src.cli.sync_oci"],
    ]


def test_run_retry_no_candidates_is_noop(tmp_path, capsys):
    _write_summary(tmp_path, "20250101")
    calls = []

    code = retry_cli.run_retry(
        target_date="20250101",
        summary_dir=tmp_path,
        apply=True,
        runner=lambda command: calls.append(list(command)),
    )

    assert code == 0
    assert calls == []
    assert "No retry candidates" in capsys.readouterr().out


def test_main_reports_missing_or_malformed_summary(tmp_path, capsys):
    code = retry_cli.main(["--date", "20250101", "--summary-dir", str(tmp_path)])

    assert code == 2
    assert "Daily summary not found" in capsys.readouterr().out
