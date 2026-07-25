from pathlib import Path

from scripts.maintenance import audit_oracle_migration_preflight as preflight


def test_object_checks_distinguish_missing_source_columns_from_planned_columns() -> None:
    checks = preflight._object_checks(
        {
            ("PLAYERS", "KBO_PERSON_ID"),
            ("PLAYER_BASIC", "PLAYER_ID"),
        }
    )

    by_name = {check["name"]: check for check in checks}

    assert by_name["required_object:PLAYERS"]["status"] == "PASS"
    assert by_name["required_object:PLAYER_MOVEMENTS"]["status"] == "BLOCK"
    assert by_name["planned_column:PLAYERS.PLAYER_BASIC_ID"]["status"] == "PLAN"


def test_count_check_marks_nonzero_counts_as_warning_when_nonblocking(monkeypatch) -> None:
    monkeypatch.setattr(preflight, "_scalar_count", lambda conn, query: (3, None))

    result = preflight._count_check(
        object(),
        "unresolved:teams",
        "SELECT COUNT(*) FROM teams",
        "clean",
        blocking=False,
    )

    assert result == {
        "name": "unresolved:teams",
        "status": "WARN",
        "detail": "found 3 row(s) requiring review",
        "count": 3,
    }


def test_run_preflight_reports_blocking_checks(monkeypatch) -> None:
    monkeypatch.setattr(preflight, "_user_columns", lambda conn: set())
    monkeypatch.setattr(preflight, "_identity_checks", lambda conn, columns: [])
    monkeypatch.setattr(preflight, "_movement_checks", lambda conn, columns: [])
    monkeypatch.setattr(preflight, "_orphan_checks", lambda conn, columns: [])
    monkeypatch.setattr(
        preflight,
        "_trigger_states",
        lambda conn: ([], {"name": "trigger_states", "status": "PASS", "detail": "none"}),
    )

    report = preflight.run_preflight(object())

    assert report["read_only"] is True
    assert report["preflight_clear"] is False
    assert "required_object:PLAYERS" in report["blocking_checks"]


def test_write_report_outputs_json_and_file(tmp_path: Path, capsys) -> None:
    report = {
        "migration": preflight.MIGRATION_NAME,
        "read_only": True,
        "preflight_clear": True,
        "blocking_checks": [],
        "checks": [],
        "trigger_states": [],
    }
    output = tmp_path / "preflight.json"

    preflight._write_report(report, output, as_json=True)

    assert '"preflight_clear": true' in capsys.readouterr().out
    assert '"migration": "024_deletion_anomaly_integrity"' in output.read_text()
