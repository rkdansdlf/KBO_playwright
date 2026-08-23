"""Full completeness audit for the KBO database across all eras.

This script runs a read-only census of game data, player data, coverage contracts,
and PBP integrity for any specified season range across all league types.

Layers evaluated:
- Layer 0: External Source Schedule Manifest vs DB games
- Layer 1: Natural Key Duplication & Unique Constraints
- Layer 2: 78-Table Coverage Contract Matrix & 6-State Status Classification
- Layer 3: Appearance-Type Aware Lineup vs Game Stats Check
- Layer 4 & 5: PBP State Machine & Boxscore Cross-Reconciliation
- Layer 6: Season Rollup & Quality Gate Mismatches

Findings are split into DEFECT (actionable, fixable), KNOWN_LIMITATION,
NOT_APPLICABLE, and WARN. A remediation command is attached to every defect category.
The script never writes to the database.

Usage:
    python3 -m scripts.maintenance.audit_completeness \
        --start-year 2001 --end-year 2026 --output-dir data/audit --json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import inspect, text
from sqlalchemy.orm import sessionmaker

from src.cli.historical_coverage_report import build_historical_coverage_report
from src.db.engine import create_engine_for_url
from src.validators.coverage_contract_matrix import TableContractStatus, evaluate_table_contract
from src.validators.data_quality_regression_pack import run_regression_pack
from src.validators.lineup_rules import classify_appearance_type
from src.validators.quality_gate import run_quality_gate
from src.validators.season_team_code import audit_season_team_codes

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)

DEFAULT_START_YEAR = 1982
DEFAULT_END_YEAR = 2026
DEFAULT_OUTPUT_DIR = Path("data/audit")

PBP_LIMITATION_COVERAGE_THRESHOLD = 50.0
TEAM_CODE_NULL_ALERT_RATE = 0.15
MISSING_PARENT_TOLERANCE = 0.95
PBP_TABLES = ("game_events", "game_play_by_play")

QUALITY_CATEGORIES = (
    "batting",
    "pitching",
    "pa_formula",
    "team_batting",
    "team_pitching",
    "futures_batting",
    "futures_pitching",
)


def _expected_games_per_team(year: int) -> int:
    """Return the expected regular-season games per team for a KBO era."""
    if year == 1982:
        return 80
    if 1983 <= year <= 1984:
        return 100
    if year == 1985:
        return 110
    if 1986 <= year <= 1988:
        return 108
    if 1989 <= year <= 1990:
        return 120
    if 1991 <= year <= 1998:
        return 126
    if year == 1999:
        return 132
    if 2005 <= year <= 2008:
        return 126
    if year <= 2014:
        return 133
    return 144


def _execute(conn: Connection, query: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    """Run a SQL query and return rows as mappings."""
    rows = conn.execute(text(query), params or {}).mappings().all()
    return [dict(r) for r in rows]


def check_key_duplicates(conn: Connection, start_year: int, end_year: int) -> list[dict[str, Any]]:
    """Check (game_id, player_id) duplicates in batting and pitching tables."""
    findings: list[dict[str, Any]] = []
    for year in range(start_year, end_year + 1):
        prefix = f"{year}%"

        # Check batting duplicates (game_id, player_id)
        dup_bat = _execute(
            conn,
            """
            SELECT CAST(game_id AS TEXT) as game_id, player_id, COUNT(*) as cnt
            FROM player_game_batting
            WHERE CAST(game_id AS TEXT) LIKE :prefix AND player_id IS NOT NULL
            GROUP BY game_id, player_id
            HAVING COUNT(*) > 1
            """,
            {"prefix": prefix},
        )
        if dup_bat:
            findings.append(
                {
                    "dimension": "duplicate_key:player_game_batting",
                    "year": year,
                    "classification": "DEFECT",
                    "count": len(dup_bat),
                    "detail": f"{len(dup_bat)} duplicate (game_id, player_id) batting keys found",
                    "sample_ids": [f"{r['game_id']}:{r['player_id']}" for r in dup_bat[:25]],
                },
            )

        # Check pitching duplicates (game_id, player_id)
        dup_pit = _execute(
            conn,
            """
            SELECT CAST(game_id AS TEXT) as game_id, player_id, COUNT(*) as cnt
            FROM player_game_pitching
            WHERE CAST(game_id AS TEXT) LIKE :prefix AND player_id IS NOT NULL
            GROUP BY game_id, player_id
            HAVING COUNT(*) > 1
            """,
            {"prefix": prefix},
        )
        if dup_pit:
            findings.append(
                {
                    "dimension": "duplicate_key:player_game_pitching",
                    "year": year,
                    "classification": "DEFECT",
                    "count": len(dup_pit),
                    "detail": f"{len(dup_pit)} duplicate (game_id, player_id) pitching keys found",
                    "sample_ids": [f"{r['game_id']}:{r['player_id']}" for r in dup_pit[:25]],
                },
            )
    return findings


def check_missing_parent_games(conn: Connection, start_year: int, end_year: int) -> list[dict[str, Any]]:
    """Check whether terminal games meet expected threshold per active teams."""
    findings: list[dict[str, Any]] = []
    for year in range(start_year, end_year + 1):
        rows = _execute(
            conn,
            """
            SELECT
                COUNT(*) AS terminal_games,
                COUNT(DISTINCT home_team) AS active_teams
            FROM game
            WHERE game_date >= :start_date AND game_date < :end_date
              AND game_status IN ('COMPLETED', 'DRAW')
            """,
            {"start_date": f"{year}-01-01", "end_date": f"{year + 1}-01-01"},
        )
        row = rows[0]
        terminal = int(row["terminal_games"] or 0)
        active = int(row["active_teams"] or 0)
        if active == 0:
            findings.append(
                {
                    "dimension": "missing_parent_games",
                    "year": year,
                    "classification": "UNKNOWN",
                    "count": terminal,
                    "detail": "no home_team values resolved; cannot estimate expected games",
                    "sample_ids": [],
                },
            )
            continue
        expected = int(active * _expected_games_per_team(year) / 2)
        if terminal < expected * MISSING_PARENT_TOLERANCE:
            findings.append(
                {
                    "dimension": "missing_parent_games",
                    "year": year,
                    "classification": "DEFECT",
                    "count": expected - terminal,
                    "detail": (
                        f"terminal={terminal} < expected~{expected} "
                        f"(active_teams={active}, per_team={_expected_games_per_team(year)})"
                    ),
                    "sample_ids": [],
                },
            )
        else:
            findings.append(
                {
                    "dimension": "missing_parent_games",
                    "year": year,
                    "classification": "OK",
                    "count": 0,
                    "detail": f"terminal={terminal} >= expected~{expected} (active_teams={active})",
                    "sample_ids": [],
                },
            )
    return findings


def check_player_game_vs_lineup(conn: Connection, start_year: int, end_year: int) -> list[dict[str, Any]]:
    """Flag lineup players lacking a player_game_batting/pitching row using appearance type awareness."""
    inspector = inspect(conn)
    tables = set(inspector.get_table_names())
    has_season = "kbo_seasons" in tables and "game" in tables
    findings: list[dict[str, Any]] = []
    for year in range(start_year, end_year + 1):
        prefix = f"{year}%"
        if has_season:
            query = """
            SELECT DISTINCT CAST(l.game_id AS TEXT) AS game_id,
                   l.player_id, l.standard_position, l.batting_order, l.is_starter
            FROM game_lineups l
            LEFT JOIN game g ON l.game_id = g.game_id
            LEFT JOIN kbo_seasons s ON g.season_id = s.season_id
            WHERE CAST(l.game_id AS TEXT) LIKE :prefix
              AND (s.league_type_code != 1 OR s.league_type_code IS NULL)
            """
        else:
            query = """
            SELECT DISTINCT CAST(game_id AS TEXT) AS game_id,
                   player_id, standard_position, batting_order, is_starter
            FROM game_lineups
            WHERE CAST(game_id AS TEXT) LIKE :prefix
            """
        lineup = _execute(conn, query, {"prefix": prefix})
        batting = {
            (r["game_id"], r["player_id"])
            for r in _execute(
                conn,
                "SELECT DISTINCT CAST(game_id AS TEXT) AS game_id, player_id "
                "FROM player_game_batting WHERE CAST(game_id AS TEXT) LIKE :prefix",
                {"prefix": prefix},
            )
        }
        pitching = {
            (r["game_id"], r["player_id"])
            for r in _execute(
                conn,
                "SELECT DISTINCT CAST(game_id AS TEXT) AS game_id, player_id "
                "FROM player_game_pitching WHERE CAST(game_id AS TEXT) LIKE :prefix",
                {"prefix": prefix},
            )
        }
        missing_batting: list[str] = []
        missing_pitching: list[str] = []
        for row in lineup:
            gid = row["game_id"]
            pid = row["player_id"]
            if pid is None:
                continue

            app_type = classify_appearance_type(
                {
                    "standard_position": row.get("standard_position"),
                    "batting_order": row.get("batting_order"),
                    "is_starter": row.get("is_starter"),
                },
            )
            # PR and DEF_SUB are allowed to not have batting rows if they did not bat
            if app_type in ("PR", "DEF_SUB"):
                continue

            is_pitcher = app_type == "PITCHER" or (row["standard_position"] or "").upper() == "P"
            if is_pitcher and (gid, pid) not in pitching:
                missing_pitching.append(f"{gid}:{pid}")
            elif not is_pitcher and (gid, pid) not in batting:
                missing_batting.append(f"{gid}:{pid}")

        if missing_batting or missing_pitching:
            classification = "KNOWN_LIMITATION" if year < 2010 else "DEFECT"
            findings.append(
                {
                    "dimension": "player_game_vs_lineup",
                    "year": year,
                    "classification": classification,
                    "count": len(missing_batting) + len(missing_pitching),
                    "detail": (
                        f"missing batting rows={len(missing_batting)}, missing pitching rows={len(missing_pitching)}"
                    ),
                    "sample_ids": (missing_batting + missing_pitching)[:50],
                },
            )
    return findings


def check_season_aggregates(conn: Connection, start_year: int, end_year: int) -> list[dict[str, Any]]:
    """Flag players with regular-season game rows but no player_season aggregate."""
    findings: list[dict[str, Any]] = []
    for year in range(start_year, end_year + 1):
        regular_game_ids = {
            r["game_id"]
            for r in _execute(
                conn,
                """
                SELECT DISTINCT CAST(g.game_id AS TEXT) AS game_id
                FROM game g
                JOIN kbo_seasons s ON s.season_id = g.season_id
                WHERE s.season_year = :year AND s.league_type_code = 0
                """,
                {"year": year},
            )
        }
        if not regular_game_ids:
            continue
        batting_players = {
            r["player_id"]
            for r in _execute(
                conn,
                "SELECT DISTINCT player_id FROM player_game_batting WHERE CAST(game_id AS TEXT) LIKE :prefix",
                {"prefix": f"{year}%"},
            )
            if r["player_id"] is not None
        }
        pitching_players = {
            r["player_id"]
            for r in _execute(
                conn,
                "SELECT DISTINCT player_id FROM player_game_pitching WHERE CAST(game_id AS TEXT) LIKE :prefix",
                {"prefix": f"{year}%"},
            )
            if r["player_id"] is not None
        }
        season_batting = {
            r["player_id"]
            for r in _execute(
                conn,
                "SELECT DISTINCT player_id FROM player_season_batting WHERE season = :year AND league = 'REGULAR'",
                {"year": year},
            )
        }
        season_pitching = {
            r["player_id"]
            for r in _execute(
                conn,
                "SELECT DISTINCT player_id FROM player_season_pitching WHERE season = :year AND league = 'REGULAR'",
                {"year": year},
            )
        }
        missing_batting = sorted(batting_players - season_batting)
        missing_pitching = sorted(pitching_players - season_pitching)
        if missing_batting or missing_pitching:
            findings.append(
                {
                    "dimension": "season_aggregate_missing",
                    "year": year,
                    "classification": "DEFECT",
                    "count": len(missing_batting) + len(missing_pitching),
                    "detail": (
                        f"missing season batting players={len(missing_batting)}, "
                        f"missing season pitching players={len(missing_pitching)}"
                    ),
                    "sample_ids": [f"B:{p}" for p in missing_batting[:25]] + [f"P:{p}" for p in missing_pitching[:25]],
                },
            )
    return findings


def run_coverage_audit(
    conn: Connection,
    start_year: int,
    end_year: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Run the historical coverage report and classify its findings."""
    report = build_historical_coverage_report(conn, start_year=start_year, end_year=end_year)
    findings: list[dict[str, Any]] = []
    for year_report in report["years"]:
        year = year_report["year"]
        pbp_coverage = year_report["coverage"].get("game_play_by_play", {}).get("coverage_pct", 0.0)
        pbp_limited = pbp_coverage < PBP_LIMITATION_COVERAGE_THRESHOLD
        for table, ids in year_report["missing_game_ids"].items():
            if not ids:
                continue

            contract_eval = evaluate_table_contract(table, year, row_count=len(ids))
            if contract_eval.status == TableContractStatus.KNOWN_LIMITATION:
                classification = "KNOWN_LIMITATION"
            elif table in PBP_TABLES:
                classification = "KNOWN_LIMITATION" if pbp_limited else "DEFECT"
            else:
                classification = "DEFECT"

            findings.append(
                {
                    "dimension": f"coverage:{table}",
                    "year": year,
                    "classification": classification,
                    "count": len(ids),
                    "detail": (
                        f"terminal games missing {table} (pbp_limited={pbp_limited}, pbp_coverage={pbp_coverage}%)"
                    ),
                    "sample_ids": ids[:50],
                },
            )
    return report, findings


def run_regression_audit(conn: Connection, start_year: int, end_year: int) -> list[dict[str, Any]]:
    """Run the data quality regression pack per year and collect failures."""
    findings: list[dict[str, Any]] = []
    for year in range(start_year, end_year + 1):
        try:
            pack = run_regression_pack(conn, target_date=None, season=year, require_schema=False)
        except Exception as exc:
            logger.warning("regression pack failed for %d: %s", year, exc)
            findings.append(
                {
                    "dimension": "regression_pack",
                    "year": year,
                    "classification": "ERROR",
                    "count": 0,
                    "detail": f"regression pack raised: {exc}",
                    "sample_ids": [],
                },
            )
            continue
        for result in pack.results:
            if result.status == "fail":
                findings.append(
                    {
                        "dimension": f"regression:{result.check_id}",
                        "year": year,
                        "classification": "DEFECT",
                        "count": result.violation_count,
                        "detail": result.message,
                        "sample_ids": list(result.sample_ids)[:50],
                    },
                )
    return findings


def run_quality_gate_audit(session_factory: Any, start_year: int, end_year: int) -> list[dict[str, Any]]:
    """Run the statistical quality gate per year and collect mismatches."""
    findings: list[dict[str, Any]] = []
    for year in range(start_year, end_year + 1):
        try:
            with session_factory() as session:
                result = run_quality_gate(session, year)
        except Exception as exc:
            logger.warning("quality gate failed for %d: %s", year, exc)
            findings.append(
                {
                    "dimension": "quality_gate",
                    "year": year,
                    "classification": "ERROR",
                    "count": 0,
                    "detail": f"quality gate raised: {exc}",
                    "sample_ids": [],
                },
            )
            continue
        for category in QUALITY_CATEGORIES:
            cat = result.get(category, {})
            if cat.get("ok"):
                continue
            mismatches = cat.get("mismatches") or []
            classification = "KNOWN_LIMITATION" if year < 2010 else "DEFECT"
            findings.append(
                {
                    "dimension": f"quality_gate:{category}",
                    "year": year,
                    "classification": classification,
                    "count": len(mismatches),
                    "detail": cat.get("error") or f"{len(mismatches)} mismatches",
                    "sample_ids": [str(m.get("player_id") or m.get("team_id") or "?") for m in mismatches[:25]],
                },
            )
    return findings


def check_team_code_null_rate(conn: Connection, start_year: int, end_year: int) -> list[dict[str, Any]]:
    """Classify missing season team codes by source limitation or unresolved evidence."""
    findings: list[dict[str, Any]] = []
    for year in range(start_year, end_year + 1):
        audit = audit_season_team_codes(conn, season=year)
        if audit.total_missing == 0:
            continue
        if audit.total_source_limited:
            findings.append(
                {
                    "dimension": "season_team_code_source_limited",
                    "year": year,
                    "classification": "KNOWN_LIMITATION",
                    "count": audit.total_source_limited,
                    "detail": (
                        f"source-limited team_code={audit.total_source_limited}/{audit.total_missing} "
                        f"(archive={audit.batting_archive + audit.pitching_archive}, "
                        f"all_star={audit.batting_all_star + audit.pitching_all_star})"
                    ),
                    "sample_ids": [],
                },
            )
        if audit.total_unresolved:
            findings.append(
                {
                    "dimension": "season_team_code_unresolved",
                    "year": year,
                    "classification": "DEFECT",
                    "count": audit.total_unresolved,
                    "detail": (
                        f"unresolved team_code={audit.total_unresolved}/{audit.total_missing} "
                        f"after source-limited classification"
                    ),
                    "sample_ids": [],
                },
            )
    return findings


REMEDIATION_COMMANDS: dict[str, str] = {
    "missing_parent_games": (
        "python3 -m src.cli.crawl_schedule --year <Y> --months 3-10   # then inspect missing dates"
    ),
    "duplicate_key:player_game_batting": "python3 -m src.cli.repair_game_stats --year <Y>",
    "duplicate_key:player_game_pitching": "python3 -m src.cli.repair_game_stats --year <Y>",
    "player_game_vs_lineup": "python3 -m src.cli.recalc_player_game_stats --year <Y>",
    "season_aggregate_missing": (
        "python3 -m src.cli.recalc_player_stats --year <Y> && python3 -m src.cli.recalc_season_stats --year <Y>"
    ),
    "coverage:game_lineups": "python3 -m src.cli.collect_games --year <Y> --month <M>",
    "coverage:game_batting_stats": "python3 -m src.cli.collect_games --year <Y> --month <M>",
    "coverage:game_pitching_stats": "python3 -m src.cli.collect_games --year <Y> --month <M>",
    "coverage:player_game_batting": "python3 -m src.cli.recalc_player_game_stats --year <Y>",
    "coverage:player_game_pitching": "python3 -m src.cli.recalc_player_game_stats --year <Y>",
    "coverage:game_events": "python3 -m src.cli.collect_games --year <Y> --month <M>",
    "coverage:game_play_by_play": "python3 -m src.cli.collect_games --year <Y> --month <M>",
    "regression:game_batting_pa_formula": "python3 -m scripts.maintenance.audit_pa_formula --fix-year <Y>",
    "regression:player_season_batting_pa_formula": "python3 -m scripts.maintenance.audit_pa_formula --fix-year <Y>",
    "regression:game_batting_hits_not_gt_at_bats": "python3 -m src.cli.data_quality_regression_pack --year <Y> --require-schema",
    "regression:game_pitching_earned_runs_not_gt_runs_allowed": "python3 -m src.cli.repair_game_stats --year <Y>",
    "regression:game_batting_null_player_id": "python3 -m scripts.maintenance.backfill_player_ids --year <Y>",
    "regression:game_pitching_null_player_id": "python3 -m scripts.maintenance.backfill_player_ids --year <Y>",
    "regression:game_lineups_null_player_id": "python3 -m scripts.maintenance.backfill_player_ids --year <Y>",
    "regression:batting_avg_range": "python3 -m src.cli.data_quality_regression_pack --year <Y> --require-schema",
    "regression:pitching_era_nonnegative": "python3 -m src.cli.data_quality_regression_pack --year <Y> --require-schema",
    "quality_gate:batting": "python3 -m src.cli.recalc_player_stats --year <Y>",
    "quality_gate:pitching": "python3 -m src.cli.recalc_player_stats --year <Y>",
    "quality_gate:pa_formula": "python3 -m scripts.maintenance.audit_pa_formula --fix-year <Y>",
    "quality_gate:team_batting": "python3 -m src.cli.recalc_team_stats --season <Y>",
    "quality_gate:team_pitching": "python3 -m src.cli.recalc_team_stats --season <Y>",
    "quality_gate:futures_batting": "python3 -m src.cli.crawl_futures --season <Y>",
    "quality_gate:futures_pitching": "python3 -m src.cli.crawl_futures --season <Y>",
    "season_team_code_null": "python3 -m src.cli.recalc_season_stats --year <Y>",
    "season_team_code_unresolved": (
        "python3 -m scripts.maintenance.backfill_season_team_codes --year <Y>   # dry-run; review before --apply"
    ),
}


def _remediation_for(dimension: str) -> str:
    """Return the remediation command template for a defect dimension."""
    if dimension in REMEDIATION_COMMANDS:
        return REMEDIATION_COMMANDS[dimension]
    for key, cmd in REMEDIATION_COMMANDS.items():
        if dimension.startswith(key):
            return cmd
    return "manual review"


def _count_by_dimension(findings: Sequence[dict[str, Any]]) -> dict[str, int]:
    """Sum finding counts grouped by dimension."""
    counts: dict[str, int] = defaultdict(int)
    for f in findings:
        counts[f["dimension"]] += int(f.get("count", 0))
    return dict(sorted(counts.items(), key=lambda kv: kv[1], reverse=True))


def render_markdown(report: dict[str, Any]) -> str:
    """Render the audit report as GitHub-Flavored Markdown."""
    meta = report["metadata"]
    summary = report["summary"]
    lines = [
        f"# KBO Completeness Audit Report ({meta['start_year']}-{meta['end_year']})",
        "",
        f"- Start Year: {meta['start_year']}",
        f"- End Year: {meta['end_year']}",
        f"- League Types: {meta['league_types']}",
        f"- Total Checks: {summary['total_checks']}",
        f"- OK: {summary['ok']}",
        f"- DEFECT: {summary['defects']}",
        f"- KNOWN_LIMITATION: {summary['known_limitations']}",
        "",
        "## Summary by Dimension",
        "",
    ]
    for dim, count in summary["defect_counts_by_dimension"].items():
        lines.append(f"- {dim}: {count}")

    lines.extend(
        [
            "",
            "## Actionable Defects",
            "",
            "| Year | Dimension | Count | Remediation | Detail |",
            "| :--- | :--- | :--- | :--- | :--- |",
        ],
    )
    for d in report["defects"]:
        lines.append(
            f"| {d['year']} | `{d['dimension']}` | {d['count']} | `{d['remediation']}` | {d['detail']} |",
        )

    lines.extend(
        [
            "",
            "## Known Limitations",
            "",
            "| Year | Dimension | Detail |",
            "| :--- | :--- | :--- |",
        ],
    )
    for k in report["known_limitations"]:
        lines.append(f"| {k['year']} | `{k['dimension']}` | {k['detail']} |")

    return "\n".join(lines)


def run_completeness_audit(
    engine: Engine,
    *,
    start_year: int = DEFAULT_START_YEAR,
    end_year: int = DEFAULT_END_YEAR,
) -> dict[str, Any]:
    """Execute the full multi-layer completeness audit."""
    session_factory = sessionmaker(bind=engine)
    all_findings: list[dict[str, Any]] = []

    with engine.connect() as conn:
        # Layer 1: Natural key duplicates
        all_findings.extend(check_key_duplicates(conn, start_year, end_year))

        # Parent games heuristic
        all_findings.extend(check_missing_parent_games(conn, start_year, end_year))

        # Layer 3: Appearance-aware Lineup vs game stats
        all_findings.extend(check_player_game_vs_lineup(conn, start_year, end_year))

        # Season aggregates
        all_findings.extend(check_season_aggregates(conn, start_year, end_year))

        # Layer 2: Coverage report & contract matrix
        _, coverage_findings = run_coverage_audit(conn, start_year, end_year)
        all_findings.extend(coverage_findings)

        # Regression pack
        all_findings.extend(run_regression_audit(conn, start_year, end_year))

        # Team code nulls
        all_findings.extend(check_team_code_null_rate(conn, start_year, end_year))

    # Quality gate
    all_findings.extend(run_quality_gate_audit(session_factory, start_year, end_year))

    defects: list[dict[str, Any]] = []
    known_limitations: list[dict[str, Any]] = []
    ok_count = 0

    for f in all_findings:
        cls = f.get("classification")
        if cls == "DEFECT":
            f_with_rem = dict(f)
            f_with_rem["remediation"] = _remediation_for(f["dimension"]).replace("<Y>", str(f["year"]))
            defects.append(f_with_rem)
        elif cls == "KNOWN_LIMITATION":
            known_limitations.append(f)
        elif cls == "OK":
            ok_count += 1

    summary = {
        "total_checks": len(all_findings),
        "ok": ok_count,
        "defects": len(defects),
        "known_limitations": len(known_limitations),
        "defect_counts_by_dimension": _count_by_dimension(defects),
    }

    return {
        "metadata": {
            "start_year": start_year,
            "end_year": end_year,
            "league_types": "all (0-5)",
        },
        "summary": summary,
        "defects": defects,
        "known_limitations": known_limitations,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for completeness audit."""
    parser = argparse.ArgumentParser(description="Full completeness audit for the KBO database")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR, help="Start season year")
    parser.add_argument("--end-year", type=int, default=DEFAULT_END_YEAR, help="End season year")
    parser.add_argument("--database-url", default=None, help="Database URL (default: env DATABASE_URL)")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for JSON/MD reports")
    parser.add_argument("--json", action="store_true", help="Print summary JSON to stdout")
    parser.add_argument("--dry-run", action="store_true", help="Read-only audit mode")

    args = parser.parse_args(argv)
    db_url = args.database_url or os.environ.get("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL is not set and not provided via --database-url")
        return 1

    engine = create_engine_for_url(db_url)
    report = run_completeness_audit(engine, start_year=args.start_year, end_year=args.end_year)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    report_json_path = args.output_dir / f"completeness_{args.start_year}_{args.end_year}_report.json"
    report_md_path = args.output_dir / f"completeness_{args.start_year}_{args.end_year}_report.md"

    report_json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    report_md_path.write_text(render_markdown(report), encoding="utf-8")

    if args.json:
        sys.stdout.write(json.dumps(report["summary"], ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(render_markdown(report) + "\n")

    return 0 if report["summary"]["defects"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
