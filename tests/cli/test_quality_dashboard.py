from __future__ import annotations

import json

from src.cli.quality_dashboard import build_quality_dashboard, load_quality_records, main


def _write_report(path, *, date: str, gate_ok: bool = True, relay_ok: bool = True, total_games: int = 1) -> None:
    payload = {
        "metrics": {
            "date": date,
            "total_games": total_games,
            "completed_count": total_games,
            "relay_integrity": {"ok": relay_ok},
            "standings_integrity": {"ok": True},
            "parity": {"ok": True},
            "pa_formula_integrity": {"ok": True},
        },
        "quality_gate": {"ok": gate_ok},
        "generated_at": "2026-06-23T00:00:00+09:00",
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_load_quality_records_sorts_and_limits(tmp_path):
    _write_report(tmp_path / "20260622.json", date="20260622", total_games=2)
    _write_report(tmp_path / "20260620.json", date="20260620", total_games=3)
    (tmp_path / "historical_audit.json").write_text("{}", encoding="utf-8")

    records = load_quality_records(tmp_path, limit=1)

    assert [record["date"] for record in records] == ["20260622"]
    assert records[0]["total_games"] == 2


def test_build_quality_dashboard_counts_failures(tmp_path):
    _write_report(tmp_path / "ok.json", date="20260620", gate_ok=True, relay_ok=True, total_games=2)
    _write_report(tmp_path / "bad.json", date="20260621", gate_ok=False, relay_ok=False, total_games=4)

    dashboard = build_quality_dashboard(tmp_path)

    assert dashboard["report_count"] == 2
    assert dashboard["totals"] == {"games": 6, "completed_games": 6, "failing_reports": 1}
    assert dashboard["failure_counts"] == {"quality_gate": 1, "relay_integrity": 1}
    assert dashboard["latest"]["date"] == "20260621"


def test_main_json_output(tmp_path, caplog):
    _write_report(tmp_path / "20260620.json", date="20260620", total_games=2)

    assert main(["--report-dir", str(tmp_path), "--json"]) == 0
    assert '"report_count": 1' in caplog.text
