import json

from src.cli.generate_quality_report import (
    format_telegram_report,
    get_auto_remediation_summary,
)


def test_get_auto_remediation_summary_no_issues(tmp_path):
    summary = get_auto_remediation_summary("20260531", audit_dir=tmp_path / "logs" / "audit_fixes")
    assert summary["status"] == "no_issues"
    assert summary["total_fixed"] == 0


def test_get_auto_remediation_summary_fixed(tmp_path):
    audit_fixes_dir = tmp_path / "logs" / "audit_fixes"
    audit_fixes_dir.mkdir(parents=True, exist_ok=True)

    # 1. Fixed player file
    player_data = [
        {
            "timestamp": "2026-05-31T12:00:00",
            "player_id": "10001",
            "type": "batting",
            "player_name": "홍길동",
            "original": {"hits": 5, "at_bats": 10},
            "calculated": {"hits": 2, "at_bats": 10},
        }
    ]
    with (audit_fixes_dir / "20260531_10001_batting.json").open("w", encoding="utf-8") as f:
        json.dump(player_data, f)

    summary = get_auto_remediation_summary("20260531", audit_dir=audit_fixes_dir)

    assert summary["status"] == "fixed"
    assert summary["total_fixed"] == 1
    assert len(summary["players_fixed"]) == 1
    assert summary["players_fixed"][0]["name"] == "홍길동"
    assert summary["players_fixed"][0]["diffs"] == ["hits: 5→2"]
    assert "BATTING" in summary["categories_fixed"]


def test_get_auto_remediation_summary_warning(tmp_path):
    audit_fixes_dir = tmp_path / "logs" / "audit_fixes"
    audit_fixes_dir.mkdir(parents=True, exist_ok=True)

    warning_data = {
        "year": 2026,
        "series": "regular",
        "mismatches": [
            {"player_id": "10001", "name": "홍길동", "diffs": ["hits: 5 vs 2"]},
        ],
    }
    with (audit_fixes_dir / "20260531_warning_batting.json").open("w", encoding="utf-8") as f:
        json.dump(warning_data, f)

    summary = get_auto_remediation_summary("20260531", audit_dir=audit_fixes_dir)

    assert summary["status"] == "warning"
    assert summary["total_warning"] == 1
    assert summary["players_warning"][0]["name"] == "홍길동"


def test_get_auto_remediation_summary_aborted(tmp_path):
    audit_fixes_dir = tmp_path / "logs" / "audit_fixes"
    audit_fixes_dir.mkdir(parents=True, exist_ok=True)

    abort_data = {"year": 2026, "series": "regular", "reason": "Too many mismatches"}
    with (audit_fixes_dir / "20260531_abort_batting.json").open("w", encoding="utf-8") as f:
        json.dump(abort_data, f)

    summary = get_auto_remediation_summary("20260531", audit_dir=audit_fixes_dir)

    assert summary["status"] == "aborted"
    assert len(summary["abort_reasons"]) == 1
    assert "BATTING: Too many mismatches" in summary["abort_reasons"][0]


def test_format_telegram_report_with_remediation():
    metrics = {
        "date": "20260531",
        "total_games": 5,
        "completed_count": 5,
        "status_counts": {"COMPLETED": 5},
        "detail_integrity": [{"game_id": "G1", "is_complete": True}],
        "new_players": [],
        "auto_remediation": {
            "status": "fixed",
            "total_fixed": 1,
            "players_fixed": [{"name": "홍길동", "category": "BATTING", "diffs": ["hits: 5→2"]}],
        },
    }
    gate_result = {"ok": True}

    msg = format_telegram_report(metrics, gate_result)
    assert "🔧 <b>Auto-Remediation</b>" in msg
    assert "1건 수정 완료" in msg
    assert "홍길동 (BATTING): hits: 5→2" in msg


def test_format_telegram_report_with_warning():
    metrics = {
        "date": "20260531",
        "total_games": 5,
        "completed_count": 5,
        "status_counts": {"COMPLETED": 5},
        "detail_integrity": [{"game_id": "G1", "is_complete": True}],
        "new_players": [],
        "auto_remediation": {
            "status": "warning",
            "total_warning": 1,
            "players_warning": [{"name": "이순신", "category": "PITCHING", "diffs": ["wins: 2 vs 1"]}],
        },
    }
    gate_result = {"ok": True}

    msg = format_telegram_report(metrics, gate_result)
    assert "⚠️ <b>Auto-Remediation</b>" in msg
    assert "mismatch 1건 발견" in msg
    assert "이순신 (PITCHING): wins: 2 vs 1" in msg


def test_format_telegram_report_with_aborted():
    metrics = {
        "date": "20260531",
        "total_games": 5,
        "completed_count": 5,
        "status_counts": {"COMPLETED": 5},
        "detail_integrity": [{"game_id": "G1", "is_complete": True}],
        "new_players": [],
        "auto_remediation": {
            "status": "aborted",
            "categories_aborted": ["BATTING"],
            "abort_reasons": ["BATTING: Too many mismatches"],
        },
    }
    gate_result = {"ok": True}

    msg = format_telegram_report(metrics, gate_result)
    assert "🛑 <b>Auto-Remediation</b>" in msg
    assert "작업 중단 (BATTING)" in msg
    assert "BATTING: Too many mismatches" in msg
