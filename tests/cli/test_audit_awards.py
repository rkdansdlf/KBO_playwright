"""CLI tests for the read-only awards audit."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.cli.audit_awards import main
from src.cli.audit_awards import _load_awards


def test_audit_awards_writes_json_report(tmp_path, capsys) -> None:
    """Render and persist an audit report without saving source data."""
    session = MagicMock()
    session.__enter__.return_value = session
    empty_result = MagicMock()
    empty_result.scalars.return_value.all.return_value = []
    empty_result.__iter__.return_value = iter(())
    session.execute.return_value = empty_result
    report = {
        "source": "awards",
        "status": "missing_upstream",
        "raw_records": 0,
        "parsed_records": None,
        "stored_records": 0,
        "quarantined_records": 0,
        "quality": {"quality_ok": True},
        "source_details": [],
        "ok": False,
    }
    output = tmp_path / "awards-audit.json"
    with (
        patch("src.cli.audit_awards.create_engine_for_url"),
        patch("src.cli.audit_awards.Session", return_value=session),
        patch("src.cli.audit_awards._load_awards", return_value=([], {})),
        patch("src.cli.audit_awards.build_award_audit", return_value=report),
    ):
        assert main(["--json", "--output", str(output)]) == 1

    rendered = json.loads(capsys.readouterr().out)
    assert rendered["status"] == "missing_upstream"
    assert json.loads(output.read_text(encoding="utf-8"))["stored_records"] == 0


def test_load_awards_reports_legacy_optional_link_columns() -> None:
    """Allow read-only audit against pre-link awards schemas."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE awards ("
                "id INTEGER PRIMARY KEY, year INTEGER NOT NULL, award_type VARCHAR(50) NOT NULL, "
                "category VARCHAR(50), player_name VARCHAR(100) NOT NULL, team_name VARCHAR(50) NOT NULL)"
            ),
        )
    with Session(engine) as session:
        rows, schema = _load_awards(session)

    assert rows == []
    assert schema["missing_optional_columns"] == ["player_id", "team_code"]
