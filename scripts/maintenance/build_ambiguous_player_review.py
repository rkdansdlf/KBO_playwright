"""Build an evidence report for unresolved NULL player_id groups.

The report is read-only and intentionally does not choose a player. It expands
the conservative resolver's unresolved groups into one row per candidate with
profile, season, and same-source evidence for manual review.
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from pathlib import Path


ALLOWED_TABLES = {"game_batting_stats", "game_pitching_stats", "game_lineups"}


def _sqlite_path(database_url: str) -> Path:
    """Convert a SQLite URL or filesystem path to a database path."""
    value = database_url.removeprefix("sqlite:///")
    return Path(value)


def _candidate_ids(value: str) -> list[int]:
    """Parse the resolver CSV candidate id list."""
    return [int(item) for item in value.split(",") if item.strip().isdigit()]


def _candidate_evidence(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    year: int,
    team_code: str,
    player_name: str,
    player_id: int,
) -> dict[str, object]:
    """Collect non-mutating evidence for one candidate player id."""
    profile = conn.execute(
        """
        SELECT name, team, status, uniform_no, birth_date, position
        FROM player_basic
        WHERE player_id = ?
        """,
        (player_id,),
    ).fetchone()
    source_rows = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE player_id = ?
          AND substr(CAST(game_id AS TEXT), 1, 4) = ?
          AND COALESCE(team_code, '') = ?
        """,
        (player_id, str(year), team_code),
    ).fetchone()[0]
    batting_rows = conn.execute(
        """
        SELECT COUNT(*)
        FROM player_season_batting
        WHERE player_id = ? AND season = ? AND team_code = ?
        """,
        (player_id, year, team_code),
    ).fetchone()[0]
    pitching_rows = conn.execute(
        """
        SELECT COUNT(*)
        FROM player_season_pitching
        WHERE player_id = ? AND season = ? AND team_code = ?
        """,
        (player_id, year, team_code),
    ).fetchone()[0]
    return {
        "candidate_name": profile[0] if profile else "",
        "candidate_team": profile[1] if profile else "",
        "candidate_status": profile[2] if profile else "",
        "candidate_uniform_no": profile[3] if profile else "",
        "candidate_birth_date": profile[4] if profile else "",
        "candidate_position": profile[5] if profile else "",
        "same_source_rows": source_rows,
        "season_batting_rows": batting_rows,
        "season_pitching_rows": pitching_rows,
    }


def build_review(input_csv: Path, database_path: Path) -> tuple[list[dict[str, object]], dict[str, int]]:
    """Expand unresolved groups into candidate evidence rows."""
    rows: list[dict[str, object]] = []
    with input_csv.open(newline="", encoding="utf-8") as handle, sqlite3.connect(database_path) as conn:
        conn.execute("PRAGMA busy_timeout=60000")
        for group in csv.DictReader(handle):
            table_name = group["table_name"]
            if table_name not in ALLOWED_TABLES:
                continue
            candidates = _candidate_ids(group.get("candidate_ids", ""))
            base = {
                "table_name": table_name,
                "year": group["year"],
                "team_code": group["team_code"],
                "player_name": group["player_name"],
                "row_count": group["row_count"],
                "uniform_nos": group["uniform_nos"],
                "candidate_count": len(candidates),
            }
            if not candidates:
                rows.append({**base, "candidate_id": "", "evidence": "no_candidate"})
                continue
            for candidate_id in candidates:
                evidence = _candidate_evidence(
                    conn,
                    table_name=table_name,
                    year=int(group["year"]),
                    team_code=group["canonical_team_code"],
                    player_name=group["player_name"],
                    player_id=candidate_id,
                )
                rows.append({**base, "candidate_id": candidate_id, **evidence})
    summary = {
        "unresolved_groups": len({(r["table_name"], r["year"], r["team_code"], r["player_name"]) for r in rows}),
        "review_rows": len(rows),
        "candidate_rows": sum(1 for row in rows if row.get("candidate_id")),
    }
    return rows, summary


def main(argv: list[str] | None = None) -> int:
    """Create the ambiguous-player evidence CSV and JSON summary."""
    parser = argparse.ArgumentParser(description="Build read-only ambiguous player evidence report")
    parser.add_argument("--input-csv", type=Path, required=True)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    args = parser.parse_args(argv)

    rows, summary = build_review(args.input_csv, _sqlite_path(args.database_url))
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in rows for key in row})
    with args.output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    args.output_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))
    print(f"review_csv={args.output_csv}")
    print(f"summary_json={args.output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
