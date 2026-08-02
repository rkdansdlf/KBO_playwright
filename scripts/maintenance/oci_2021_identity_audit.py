"""Read-only pilot audit for OCI 2021 pitching identity collisions.

The audit compares OCI season-level pitching rows with local game/season
evidence. It never mutates either database. ``exact`` results are emitted to a
separate CSV only when explicitly requested; the existing override files are
never modified automatically.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import inspect, text

from src.constants import KST
from src.db.engine import create_engine_for_url

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OVERRIDE_CSV = PROJECT_ROOT / "data/player_id_overrides.csv"
TARGET_TABLE = "player_season_pitching"


def _columns(conn: Connection, table_name: str) -> set[str]:
    """Return the columns available on a database table."""
    return {str(column["name"]).lower() for column in inspect(conn).get_columns(table_name)}


def _team_expression(alias: str, columns: set[str]) -> str:
    """Build a portable team expression using canonical team data when present."""
    if {"canonical_team_code", "team_code"}.issubset(columns):
        return f"COALESCE({alias}.canonical_team_code, {alias}.team_code)"
    if "canonical_team_code" in columns:
        return f"{alias}.canonical_team_code"
    if "team_code" in columns:
        return f"{alias}.team_code"
    return "NULL"


def _source_expression(columns: set[str]) -> str:
    """Select the source column used by local or OCI schemas."""
    if "data_source" in columns:
        return "s.data_source"
    if "source" in columns:
        return "s.source"
    return "NULL"


def _as_int(value: object) -> int:
    """Convert a nullable database value to an integer."""
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _as_float(value: object) -> float:
    """Convert a nullable database value to a float."""
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _target_rows(conn: Connection, year: int) -> list[dict[str, object]]:
    """Load regular-season OCI pitching rows with player profile names."""
    columns = _columns(conn, TARGET_TABLE)
    team_expression = _team_expression("s", columns)
    source_expression = _source_expression(columns)
    rows = conn.execute(
        text(
            f"""
            SELECT
                s.player_id,
                {team_expression} AS team_code,
                COALESCE(pb.name, '') AS player_name,
                COALESCE(pb.team, '') AS profile_team,
                COALESCE(pb.position, '') AS profile_position,
                {source_expression} AS data_source,
                COALESCE(s.games, 0) AS games,
                COALESCE(s.innings_outs, 0) AS innings_outs,
                COALESCE(s.innings_pitched, 0) AS innings_pitched,
                s.era AS era
            FROM {TARGET_TABLE} s
            LEFT JOIN player_basic pb ON pb.player_id = s.player_id
            WHERE s.season = :year
              AND COALESCE(s.league, 'REGULAR') = 'REGULAR'
            """,
        ),
        {"year": year},
    ).mappings()
    return [dict(row) for row in rows]


def _duplicate_groups(rows: list[dict[str, object]]) -> dict[tuple[str, str], list[dict[str, object]]]:
    """Group OCI rows by effective team and player name, retaining collisions only."""
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        key = (str(row.get("team_code") or ""), str(row.get("player_name") or ""))
        grouped[key].append(row)
    return {
        key: members
        for key, members in grouped.items()
        if key[0] and key[1] and len({int(member["player_id"]) for member in members}) > 1
    }


def _team_ranking(groups: dict[tuple[str, str], list[dict[str, object]]]) -> list[dict[str, object]]:
    """Rank teams by duplicate-group count and affected pitching volume."""
    metrics: dict[str, dict[str, int]] = defaultdict(
        lambda: {"duplicate_group_count": 0, "row_count": 0, "games": 0, "innings_outs": 0}
    )
    for (team_code, _), rows in groups.items():
        metric = metrics[team_code]
        metric["duplicate_group_count"] += 1
        metric["row_count"] += len(rows)
        metric["games"] += sum(_as_int(row.get("games")) for row in rows)
        metric["innings_outs"] += sum(_as_int(row.get("innings_outs")) for row in rows)
    ranked = [{"team_code": team_code, **values} for team_code, values in metrics.items()]
    return sorted(
        ranked,
        key=lambda item: (
            -int(item["duplicate_group_count"]),
            -int(item["innings_outs"]),
            -int(item["row_count"]),
            str(item["team_code"]),
        ),
    )


def _select_teams(
    ranking: list[dict[str, object]],
    requested: tuple[str, ...],
    pilot_limit: int,
) -> list[str]:
    """Select explicit teams or the highest-impact pilot teams."""
    if requested:
        return sorted({team.strip() for team in requested if team.strip()})
    return [str(item["team_code"]) for item in ranking[:pilot_limit]]


def _local_game_evidence(
    conn: Connection | None,
    *,
    player_id: int,
    year: int,
    team_code: str,
) -> dict[str, int | bool]:
    """Count matching local game-level pitching evidence for one candidate."""
    empty = {"available": False, "rows": 0, "games": 0, "innings_outs": 0}
    if conn is None or not inspect(conn).has_table("game_pitching_stats"):
        return empty
    columns = _columns(conn, "game_pitching_stats")
    team_expression = _team_expression("g", columns)
    row = (
        conn.execute(
            text(
                f"""
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT g.game_id) AS games,
                COALESCE(SUM(g.innings_outs), 0) AS innings_outs
            FROM game_pitching_stats g
            WHERE g.player_id = :player_id
              AND SUBSTR(g.game_id, 1, 4) = :year
              AND {team_expression} = :team_code
            """,
            ),
            {"player_id": player_id, "year": str(year), "team_code": team_code},
        )
        .mappings()
        .one()
    )
    return {
        "available": True,
        "rows": _as_int(row["rows"]),
        "games": _as_int(row["games"]),
        "innings_outs": _as_int(row["innings_outs"]),
    }


def _local_season_evidence(
    conn: Connection | None,
    *,
    player_id: int,
    year: int,
    team_code: str,
) -> dict[str, int | bool]:
    """Count matching local season-level pitching evidence for one candidate."""
    empty = {"available": False, "rows": 0, "innings_outs": 0, "innings_pitched": 0.0}
    if conn is None or not inspect(conn).has_table(TARGET_TABLE):
        return empty
    columns = _columns(conn, TARGET_TABLE)
    team_expression = _team_expression("s", columns)
    row = (
        conn.execute(
            text(
                f"""
            SELECT
                COUNT(*) AS rows,
                COALESCE(SUM(s.innings_outs), 0) AS innings_outs,
                COALESCE(SUM(s.innings_pitched), 0) AS innings_pitched
            FROM {TARGET_TABLE} s
            WHERE s.player_id = :player_id
              AND s.season = :year
              AND COALESCE(s.league, 'REGULAR') = 'REGULAR'
              AND {team_expression} = :team_code
            """,
            ),
            {"player_id": player_id, "year": year, "team_code": team_code},
        )
        .mappings()
        .one()
    )
    return {
        "available": True,
        "rows": _as_int(row["rows"]),
        "innings_outs": _as_int(row["innings_outs"]),
        "innings_pitched": _as_float(row["innings_pitched"]),
    }


def _load_curated_ids(path: Path, year: int, team_code: str, player_name: str) -> set[int]:
    """Load matching curated game-level IDs without modifying the override file."""
    if not path.exists():
        return set()
    with path.open(newline="", encoding="utf-8") as handle:
        return {
            int(row["resolved_player_id"])
            for row in csv.DictReader(handle)
            if row.get("source_table") == "game_pitching_stats"
            and row.get("year") == str(year)
            and row.get("team_code") == team_code
            and row.get("player_name") == player_name
            and str(row.get("resolved_player_id") or "").isdigit()
        }


def _candidate_evidence(
    rows: list[dict[str, object]],
    local_conn: Connection | None,
    *,
    year: int,
    team_code: str,
    player_name: str,
    override_path: Path,
) -> tuple[list[dict[str, object]], set[int], list[int], list[int]]:
    """Build evidence rows and identify exact local game candidates."""
    candidates = sorted({int(row["player_id"]) for row in rows})
    curated_ids = _load_curated_ids(override_path, year, team_code, player_name)
    details: list[dict[str, object]] = []
    for player_id in candidates:
        candidate_rows = [row for row in rows if int(row["player_id"]) == player_id]
        details.append(
            {
                "player_id": player_id,
                "target_sources": sorted({str(row.get("data_source") or "") for row in candidate_rows}),
                "target_row_count": len(candidate_rows),
                "target_games": sum(_as_int(row.get("games")) for row in candidate_rows),
                "target_innings_outs": sum(_as_int(row.get("innings_outs")) for row in candidate_rows),
                "target_innings_pitched": sum(_as_float(row.get("innings_pitched")) for row in candidate_rows),
                "local_game": _local_game_evidence(
                    local_conn,
                    player_id=player_id,
                    year=year,
                    team_code=team_code,
                ),
                "local_season": _local_season_evidence(
                    local_conn,
                    player_id=player_id,
                    year=year,
                    team_code=team_code,
                ),
            },
        )
    game_candidates = [
        int(detail["player_id"])
        for detail in details
        if int(detail["local_game"]["rows"]) > 0  # type: ignore[index]
    ]
    season_candidates = [
        int(detail["player_id"])
        for detail in details
        if int(detail["local_season"]["rows"]) > 0  # type: ignore[index]
    ]
    return details, curated_ids, game_candidates, season_candidates


def _classify_group(
    *,
    candidate_ids: list[int],
    curated_ids: set[int],
    game_candidates: list[int],
    season_candidates: list[int],
) -> tuple[str, int | None, str, str]:
    """Classify a group using only explicit or unique local game evidence."""
    matching_curated = sorted(curated_ids.intersection(candidate_ids))
    if len(matching_curated) == 1:
        return "exact", matching_curated[0], "curated player ID override", "curated_override"
    if len(game_candidates) == 1:
        return "exact", game_candidates[0], "unique local game pitching evidence", "local_game_pitching_stats"
    if len(game_candidates) > 1:
        return "ambiguous", None, "multiple local candidates have game evidence", "local_game_pitching_stats"
    if len(season_candidates) == 1:
        return "exact", season_candidates[0], "unique local season pitching evidence", "local_player_season_pitching"
    if len(season_candidates) > 1:
        return "ambiguous", None, "multiple local candidates have season evidence", "local_player_season_pitching"
    return "unresolved", None, "no exact local game evidence", "none"


def _group_report(
    key: tuple[str, str],
    rows: list[dict[str, object]],
    local_conn: Connection | None,
    *,
    year: int,
    override_path: Path,
) -> dict[str, object]:
    """Create one identity audit group report."""
    team_code, player_name = key
    details, curated_ids, game_candidates, season_candidates = _candidate_evidence(
        rows,
        local_conn,
        year=year,
        team_code=team_code,
        player_name=player_name,
        override_path=override_path,
    )
    candidate_ids = [int(detail["player_id"]) for detail in details]
    classification, resolved_id, reason, evidence_source = _classify_group(
        candidate_ids=candidate_ids,
        curated_ids=curated_ids,
        game_candidates=game_candidates,
        season_candidates=season_candidates,
    )
    return {
        "team_code": team_code,
        "player_name": player_name,
        "candidate_ids": candidate_ids,
        "classification": classification,
        "resolved_player_id": resolved_id,
        "reason": reason,
        "evidence_source": evidence_source,
        "candidates": details,
    }


def audit_identity(
    target_conn: Connection,
    local_conn: Connection | None,
    *,
    year: int = 2021,
    teams: tuple[str, ...] = (),
    pilot_limit: int = 3,
    override_path: Path = DEFAULT_OVERRIDE_CSV,
) -> dict[str, object]:
    """Run the read-only identity audit and return a JSON-compatible report."""
    rows = _target_rows(target_conn, year)
    all_groups = _duplicate_groups(rows)
    ranking = _team_ranking(all_groups)
    selected_teams = _select_teams(ranking, teams, pilot_limit)
    selected_groups = [
        _group_report(key, members, local_conn, year=year, override_path=override_path)
        for key, members in sorted(all_groups.items())
        if key[0] in selected_teams
    ]
    counts = {
        classification: sum(group["classification"] == classification for group in selected_groups)
        for classification in ("exact", "ambiguous", "unresolved")
    }
    return {
        "report_version": 1,
        "generated_at": datetime.now(KST).isoformat(),
        "read_only": True,
        "year": year,
        "target_dialect": target_conn.dialect.name,
        "local_dialect": local_conn.dialect.name if local_conn is not None else None,
        "pilot_limit": pilot_limit,
        "selected_teams": selected_teams,
        "team_ranking": ranking,
        "summary": {
            "all_duplicate_groups": len(all_groups),
            "pilot_groups": len(selected_groups),
            **counts,
        },
        "groups": selected_groups,
    }


def write_exact_override_candidates(report: dict[str, object], output: Path) -> int:
    """Write exact candidates to a separate, reviewable override CSV."""
    groups = report.get("groups", [])
    rows = [
        {
            "source_table": TARGET_TABLE,
            "year": report.get("year", 2021),
            "team_code": group["team_code"],
            "player_name": group["player_name"],
            "resolved_player_id": group["resolved_player_id"],
            "reason": group["reason"],
            "evidence_source": group["evidence_source"],
        }
        for group in groups
        if group.get("classification") == "exact" and group.get("resolved_player_id") is not None
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "source_table",
        "year",
        "team_code",
        "player_name",
        "resolved_player_id",
        "reason",
        "evidence_source",
    ]
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def _default_output(year: int) -> Path:
    """Return a timestamped audit output path."""
    stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    return PROJECT_ROOT / "data/audit" / f"oci_{year}_identity_audit_{stamp}.json"


def main(argv: list[str] | None = None) -> int:
    """Run the OCI identity audit CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2021)
    parser.add_argument("--team", action="append", default=[], help="Pilot team code; repeat to select multiple teams")
    parser.add_argument("--pilot-limit", type=int, default=3, help="Number of top-impact teams when --team is omitted")
    parser.add_argument("--target-url", default=None, help="OCI URL; defaults to OCI_DB_URL or TARGET_DATABASE_URL")
    parser.add_argument("--local-url", default=None, help="Local URL; defaults to DATABASE_URL")
    parser.add_argument("--override-csv", type=Path, default=DEFAULT_OVERRIDE_CSV)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--exact-overrides-output", type=Path, default=None)
    args = parser.parse_args(argv)
    if args.pilot_limit < 1:
        parser.error("--pilot-limit must be positive")

    target_url = args.target_url or os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    local_url = args.local_url or os.getenv("DATABASE_URL")
    if not target_url:
        parser.error("target URL is required via --target-url, OCI_DB_URL, or TARGET_DATABASE_URL")
    if not local_url:
        parser.error("local URL is required via --local-url or DATABASE_URL")

    target_engine = create_engine_for_url(target_url)
    local_engine = create_engine_for_url(local_url)
    with target_engine.connect() as target_conn, local_engine.connect() as local_conn:
        report = audit_identity(
            target_conn,
            local_conn,
            year=args.year,
            teams=tuple(args.team),
            pilot_limit=args.pilot_limit,
            override_path=args.override_csv,
        )

    output = args.output or _default_output(args.year)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(f"identity_audit_report={output}")
    if args.exact_overrides_output:
        count = write_exact_override_candidates(report, args.exact_overrides_output)
        print(f"exact_override_candidates={count}")
        print(f"exact_override_report={args.exact_overrides_output}")
    print(json.dumps(report["summary"], ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
