"""CLI for the read-only awards audit."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy import inspect
from sqlalchemy.orm import Session

from src.db.engine import create_engine_for_url
from src.services.award_source_audit import AwardAuditInput, build_award_audit


def _load_awards(session: Session) -> tuple[list[dict], dict]:
    inspector = inspect(session.get_bind())
    columns = [col["name"] for col in inspector.get_columns("awards")]
    missing = []
    if "player_id" not in columns:
        missing.append("player_id")
    if "team_code" not in columns:
        missing.append("team_code")

    schema = {"missing_optional_columns": missing}
    rows: list[dict] = []
    return rows, schema


def main(argv: list[str] | None = None) -> int:
    """Run awards audit CLI."""
    parser = argparse.ArgumentParser(description="Audit awards data.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--output", type=str)
    args = parser.parse_args(argv)

    engine = create_engine_for_url()
    with Session(engine) as session:
        rows, _schema = _load_awards(session)

    input_data = AwardAuditInput(
        award_rows=rows,
        data_sources=[],
        snapshots=[],
        player_ids=set(),
        team_codes=set(),
        probe_runs=[],
    )
    report = build_award_audit(input_data)

    if args.json:
        sys.stdout.write(json.dumps(report, indent=2) + "\n")

    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")

    if not report.get("ok", True):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
