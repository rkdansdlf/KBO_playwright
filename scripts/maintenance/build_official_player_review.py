#!/usr/bin/env python3
"""Build a Markdown review document for official player ID candidates."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import select

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal
from src.models.game import Game, GameBattingStat, GameLineup, GamePitchingStat

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


@dataclass(frozen=True)
class DbEvidence:
    """Aggregate database evidence for one player name and team."""

    row_count: int
    player_ids: tuple[int, ...]
    uniform_nos: tuple[str, ...]
    positions: tuple[str, ...]
    table_counts: tuple[tuple[str, int], ...]


def _text(value: object) -> str:
    return str(value or "").strip()


def _candidate_value(candidate: Mapping[str, object], key: str) -> str:
    return _text(candidate.get(key)) or "-"


def load_resolver_evidence(path: Path | None) -> dict[tuple[str, str, int], dict[str, str]]:
    """Load resolver dry-run rows keyed by player name, team, and season."""
    if path is None:
        return {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = csv.DictReader(handle)
        return {
            (_text(row.get("player_name")), _text(row.get("team_code")), int(row["year"])): row
            for row in rows
            if _text(row.get("player_name")) and _text(row.get("team_code")) and _text(row.get("year"))
        }


def load_official_report(path: Path) -> dict[str, object]:
    """Load the JSON report emitted by lookup_official_players."""
    return json.loads(path.read_text(encoding="utf-8"))


def _model_specs() -> tuple[tuple[str, type[object], object], ...]:
    return (
        ("game_batting_stats", GameBattingStat, GameBattingStat.position),
        ("game_pitching_stats", GamePitchingStat, GamePitchingStat.standard_position),
        ("game_lineups", GameLineup, GameLineup.position),
    )


def load_db_evidence(target: Mapping[str, object]) -> DbEvidence:
    """Read all matching game-level rows for one report target."""
    target_data = target["target"]
    name = _text(target_data["name"])
    team_code = _text(target_data.get("team_code"))
    season = int(target_data["season"])
    start = date(season, 1, 1)
    end = date(season + 1, 1, 1)
    player_ids: set[int] = set()
    uniform_nos: set[str] = set()
    positions: set[str] = set()
    table_counts: dict[str, int] = {}
    row_count = 0
    with SessionLocal() as session:
        for table_name, model, position_column in _model_specs():
            rows = session.execute(
                select(model.player_id, model.uniform_no, position_column)
                .join(Game, model.game_id == Game.game_id)
                .where(
                    Game.game_date >= start,
                    Game.game_date < end,
                    model.player_name == name,
                    model.team_code == team_code,
                ),
            ).all()
            table_counts[table_name] = len(rows)
            row_count += len(rows)
            for player_id, uniform_no, position in rows:
                if player_id is not None:
                    player_ids.add(int(player_id))
                if _text(uniform_no):
                    uniform_nos.add(_text(uniform_no))
                if _text(position):
                    positions.add(_text(position))
    return DbEvidence(
        row_count=row_count,
        player_ids=tuple(sorted(player_ids)),
        uniform_nos=tuple(sorted(uniform_nos)),
        positions=tuple(sorted(positions)),
        table_counts=tuple(sorted(table_counts.items())),
    )


def _candidate_markdown(candidate: Mapping[str, object], evidence: DbEvidence) -> str:
    player_id = candidate.get("player_id")
    db_id_match = player_id in evidence.player_ids
    uniform_match = bool(_text(candidate.get("uniform_no")) in evidence.uniform_nos)
    position_match = bool(_text(candidate.get("position")) in evidence.positions)
    review = "DB_ID_MATCH" if db_id_match else "REVIEW"
    values = (
        _candidate_value(candidate, "player_id"),
        _candidate_value(candidate, "uniform_no"),
        _candidate_value(candidate, "name"),
        _candidate_value(candidate, "team"),
        _candidate_value(candidate, "position"),
        "Y" if candidate.get("current_team_match") else "N",
        "Y" if candidate.get("career_team_match") else "N",
        "Y" if uniform_match else "N",
        "Y" if position_match else "N",
        "Y" if db_id_match else "N",
        review,
        _candidate_value(candidate, "career"),
    )
    return "| " + " | ".join(value.replace("|", "/") for value in values) + " |"


def render_review_markdown(
    report: Mapping[str, object],
    *,
    resolver_rows: Mapping[tuple[str, str, int], Mapping[str, str]] | None = None,
) -> str:
    """Render official candidates and DB evidence into one Markdown document."""
    resolver_rows = resolver_rows or {}
    results = report.get("results", [])
    lines = [
        "# Official Player ID Review",
        "",
        "This document is read-only evidence. No override or database update is performed.",
        "",
        f"- Generated: {_text(report.get('generated_at'))}",
        f"- Source: {_text(report.get('source'))}",
        f"- Targets: {len(results)}",
        "",
        "## Summary",
        "",
        "| Name | Team | Season | DB rows | DB player_ids | Uniforms | Positions | Resolver ID | Candidate count |",
        "|---|---|---:|---:|---|---|---|---:|---:|",
    ]
    details: list[str] = []
    for result in results:
        target = result["target"]
        evidence = load_db_evidence(result)
        resolver = resolver_rows.get((_text(target["name"]), _text(target.get("team_code")), int(target["season"])), {})
        details.extend(_render_target_detail(result, evidence, resolver))
        lines.append(
            "| "
            + " | ".join(
                (
                    _text(target["name"]),
                    _text(target.get("team_code")) or "-",
                    _text(target["season"]),
                    str(evidence.row_count),
                    ", ".join(map(str, evidence.player_ids)) or "-",
                    ", ".join(evidence.uniform_nos) or "-",
                    ", ".join(evidence.positions) or "-",
                    _text(resolver.get("resolved_player_id")) or "-",
                    _text(result.get("candidate_count")) or "0",
                ),
            )
            + " |",
        )
    lines.extend(["", "## Candidate Details", "", *details])
    return "\n".join(lines) + "\n"


def _render_target_detail(
    result: Mapping[str, object],
    evidence: DbEvidence,
    resolver: Mapping[str, str],
) -> list[str]:
    target = result["target"]
    lines = [
        f"### {_text(target['name'])} ({_text(target.get('team_code'))}, {target['season']})",
        "",
        f"- Official search: {_text(result.get('search_url'))}",
        f"- DB rows: {evidence.row_count} ({', '.join(f'{name}={count}' for name, count in evidence.table_counts)})",
        f"- DB player IDs: {', '.join(map(str, evidence.player_ids)) or '-'}",
        f"- DB uniforms: {', '.join(evidence.uniform_nos) or '-'}",
        f"- DB positions: {', '.join(evidence.positions) or '-'}",
        f"- Resolver result: {_text(resolver.get('resolved_player_id')) or '-'} ({_text(resolver.get('resolution_reason')) or 'none'})",
        "",
        "| Official ID | Uniform | Name | Current team | Position | Current team match | Career team match | Uniform match | Position match | DB ID match | Review | Career |",
        "|---:|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    lines.extend(_candidate_markdown(candidate, evidence) for candidate in result.get("candidates", []))
    lines.extend(
        ["", "**Manual decision:** approve one candidate only after reviewing official profile and DB evidence.", ""]
    )
    return lines


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the Markdown review argument parser."""
    parser = argparse.ArgumentParser(description="Build a combined official player review document.")
    parser.add_argument("--official-report", type=Path, required=True)
    parser.add_argument("--resolver-report", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Build the combined player review document."""
    args = build_arg_parser().parse_args(argv)
    report = load_official_report(args.official_report)
    resolver_rows = load_resolver_evidence(args.resolver_report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render_review_markdown(report, resolver_rows=resolver_rows), encoding="utf-8")
    sys.stdout.write(f"Wrote player review: {args.output}\n")


if __name__ == "__main__":
    main()
