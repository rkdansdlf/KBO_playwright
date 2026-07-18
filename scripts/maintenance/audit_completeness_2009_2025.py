"""Full completeness audit for the local KBO database (2009-2025).

This script runs a read-only census of game data and player data for every
season from 2009 through 2025 across all league types (regular season,
exhibition, wildcard, semi-playoff, playoff, Korean Series). It reuses the
existing coverage / quality / regression tooling and adds three checks that no
existing tool covers:

1. Missing parent games (schedule completeness vs an expected-game heuristic).
2. Player-game rows missing for players present in the lineup.
3. Player-season aggregate rows missing for players with game appearances.

Findings are split into DEFECT (actionable, fixable) and KNOWN_LIMITATION
(source-blocked gaps such as early-era play-by-play). A remediation command is
attached to every defect category. The script never writes to the database.

Usage:
    python3 -m scripts.maintenance.audit_completeness_2009_2025 \
        --start-year 2009 --end-year 2025 --output-dir data/audit --json
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

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from src.cli.historical_coverage_report import build_historical_coverage_report
from src.db.engine import create_engine_for_url
from src.validators.data_quality_regression_pack import run_regression_pack
from src.validators.quality_gate import run_quality_gate

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)

DEFAULT_START_YEAR = 2009
DEFAULT_END_YEAR = 2025
DEFAULT_OUTPUT_DIR = Path("data/audit")

# A year whose play-by-play coverage is below this percent is treated as
# source-limited; its missing PBP/relay rows become KNOWN_LIMITATION instead of
# DEFECT (early-era Naver relay returns 404 / relay_not_found).
PBP_LIMITATION_COVERAGE_THRESHOLD = 50.0

# player_season_batting.team_code NULL rate above this is flagged as a defect;
# below it the residual is accepted as a known limitation.
TEAM_CODE_NULL_ALERT_RATE = 0.15

# Tolerance applied to the expected-game heuristic before a year is flagged.
MISSING_PARENT_TOLERANCE = 0.95

# Coverage tables whose absence for a terminal game is always a DEFECT.
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
    if year <= 2014:
        return 133
    return 144


def _execute(conn: Connection, query: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    """Run a SQL query and return rows as mappings."""
    rows = conn.execute(text(query), params or {}).mappings().all()
    return [dict(row) for row in rows]


def check_missing_parent_games(conn: Connection, start_year: int, end_year: int) -> list[dict[str, Any]]:
    """Detect likely missing parent games via an expected-game heuristic.

    Expected total = active teams (distinct home_team that year) * games-per-team / 2.
    Years below the tolerance are reported as DEFECT_NEEDS_REVIEW.
    """
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
                    "detail": f"terminal={terminal} ~ expected~{expected}",
                    "sample_ids": [],
                },
            )
    return findings


def check_player_game_vs_lineup(conn: Connection, start_year: int, end_year: int) -> list[dict[str, Any]]:
    """Flag lineup players lacking a player_game_batting/pitching row."""
    findings: list[dict[str, Any]] = []
    for year in range(start_year, end_year + 1):
        prefix = f"{year}%"
        lineup = _execute(
            conn,
            """
            SELECT DISTINCT CAST(game_id AS TEXT) AS game_id,
                   player_id, standard_position
            FROM game_lineups
            WHERE CAST(game_id AS TEXT) LIKE :prefix
            """,
            {"prefix": prefix},
        )
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
            is_pitcher = (row["standard_position"] or "").upper() == "P"
            if is_pitcher and (gid, pid) not in pitching:
                missing_pitching.append(f"{gid}:{pid}")
            elif not is_pitcher and (gid, pid) not in batting:
                missing_batting.append(f"{gid}:{pid}")
        if missing_batting or missing_pitching:
            findings.append(
                {
                    "dimension": "player_game_vs_lineup",
                    "year": year,
                    "classification": "DEFECT",
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


def run_coverage_audit(conn: Connection, start_year: int, end_year: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Run the historical coverage report and classify its findings.

    Returns the raw report plus a flattened list of per-year-per-table findings.
    """
    report = build_historical_coverage_report(conn, start_year=start_year, end_year=end_year)
    findings: list[dict[str, Any]] = []
    for year_report in report["years"]:
        year = year_report["year"]
        pbp_coverage = year_report["coverage"].get("game_play_by_play", {}).get("coverage_pct", 0.0)
        pbp_limited = pbp_coverage < PBP_LIMITATION_COVERAGE_THRESHOLD
        for table, ids in year_report["missing_game_ids"].items():
            if not ids:
                continue
            if table in PBP_TABLES:
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
        except Exception as exc:  # audit must survive per-year failures
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
        except Exception as exc:  # audit must survive per-year failures
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
            findings.append(
                {
                    "dimension": f"quality_gate:{category}",
                    "year": year,
                    "classification": "DEFECT",
                    "count": len(mismatches),
                    "detail": cat.get("error") or f"{len(mismatches)} mismatches",
                    "sample_ids": [str(m.get("player_id") or m.get("team_id") or "?") for m in mismatches[:25]],
                },
            )
    return findings


def check_team_code_null_rate(conn: Connection, start_year: int, end_year: int) -> list[dict[str, Any]]:
    """Report player_season_batting.team_code NULL rate per year."""
    findings: list[dict[str, Any]] = []
    for year in range(start_year, end_year + 1):
        rows = _execute(
            conn,
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN team_code IS NULL THEN 1 ELSE 0 END) AS nulls
            FROM player_season_batting
            WHERE season = :year
            """,
            {"year": year},
        )
        row = rows[0]
        total = int(row["total"] or 0)
        nulls = int(row["nulls"] or 0)
        if total == 0 or nulls == 0:
            continue
        rate = nulls / total
        classification = "DEFECT" if rate > TEAM_CODE_NULL_ALERT_RATE else "KNOWN_LIMITATION"
        findings.append(
            {
                "dimension": "season_team_code_null",
                "year": year,
                "classification": classification,
                "count": nulls,
                "detail": f"NULL team_code={nulls}/{total} ({rate:.1%})",
                "sample_ids": [],
            },
        )
    return findings


REMEDIATION_COMMANDS: dict[str, str] = {
    "missing_parent_games": (
        "python3 -m src.cli.crawl_schedule --year <Y> --months 3-10   # then inspect missing dates"
    ),
    "player_game_vs_lineup": "python3 -m src.cli.recalc_player_game_stats --year <Y>",
    "season_aggregate_missing": (
        "python3 -m src.cli.recalc_player_stats --year <Y> && python3 -m src.cli.recalc_season_stats --year <Y>"
    ),
    "coverage:game_lineups": "python3 -m src.cli.collect_games --year <Y> --month <M>",
    "coverage:game_batting_stats": "python3 -m src.cli.collect_games --year <Y> --month <M>",
    "coverage:game_pitching_stats": "python3 -m src.cli.collect_games --year <Y> --month <M>",
    "coverage:player_game_batting": "python3 -m src.cli.recalc_player_game_stats --year <Y>",
    "coverage:player_game_pitching": "python3 -m src.cli.recalc_player_game_stats --year <Y>",
    "coverage:game_events": "python3 -m src.cli.collect_games --year <Y> --month <M>  # PBP source may be unavailable for early years",
    "coverage:game_play_by_play": "python3 -m src.cli.collect_games --year <Y> --month <M>  # relay source may be unavailable for early years",
    "regression:game_batting_pa_formula": "python3 -m scripts.maintenance.audit_pa_formula --fix-year <Y>",
    "regression:player_season_batting_pa_formula": "python3 -m scripts.maintenance.audit_pa_formula --fix-year <Y>",
    "regression:game_batting_hits_not_gt_at_bats": "python3 -m src.cli.data_quality_regression_pack --year <Y> --require-schema",
    "regression:game_pitching_earned_runs_not_gt_runs_allowed": "python3 -m src.cli.repair_game_stats --year <Y>",
    "regression:game_batting_null_player_id": "python3 -m scripts.maintenance.backfill_player_ids --year <Y>",
    "regression:game_pitching_null_player_id": "python3 -m scripts.maintenance.backfill_player_ids --year <Y>",
    "regression:game_lineups_null_player_id": "python3 -m scripts.maintenance.backfill_player_ids --year <Y>",
    "regression:batting_avg_range": "python3 -m src.cli.data_quality_regression_pack --year <Y> --require-schema",
    "regression:era_range": "python3 -m src.cli.data_quality_regression_pack --year <Y> --require-schema",
    "regression:hits_lte_at_bats_season": "python3 -m src.cli.data_quality_regression_pack --year <Y> --require-schema",
    "quality_gate:batting": "python3 -m src.cli.recalc_player_stats --year <Y>",
    "quality_gate:pitching": "python3 -m src.cli.recalc_player_stats --year <Y>",
    "quality_gate:pa_formula": "python3 -m scripts.maintenance.audit_pa_formula --fix-year <Y>",
    "quality_gate:team_batting": "python3 -m src.cli.recalc_team_stats --year <Y>",
    "quality_gate:team_pitching": "python3 -m src.cli.recalc_team_stats --year <Y>",
    "quality_gate:futures_batting": "python3 -m src.cli.recalc_player_stats --year <Y>",
    "quality_gate:futures_pitching": "python3 -m src.cli.recalc_player_stats --year <Y>",
    "season_team_code_null": "python3 -m scripts.maintenance.backfill_season_team_codes --year <Y>",
}


def _remediation_for(dimension: str) -> str:
    """Return a remediation command template for a finding dimension."""
    return REMEDIATION_COMMANDS.get(dimension, "manual review")


def build_master_report(
    conn: Connection,
    session_factory: Any,
    *,
    start_year: int,
    end_year: int,
) -> dict[str, Any]:
    """Run every audit dimension and assemble a single structured report."""
    coverage_report, coverage_findings = run_coverage_audit(conn, start_year, end_year)
    findings: list[dict[str, Any]] = []
    findings.extend(coverage_findings)
    findings.extend(check_missing_parent_games(conn, start_year, end_year))
    findings.extend(check_player_game_vs_lineup(conn, start_year, end_year))
    findings.extend(check_season_aggregates(conn, start_year, end_year))
    findings.extend(check_team_code_null_rate(conn, start_year, end_year))
    findings.extend(run_regression_audit(conn, start_year, end_year))
    findings.extend(run_quality_gate_audit(session_factory, start_year, end_year))

    defects = [f for f in findings if f["classification"] in {"DEFECT", "ERROR"}]
    known = [f for f in findings if f["classification"] == "KNOWN_LIMITATION"]
    ok_count = sum(1 for f in findings if f["classification"] == "OK")

    for finding in findings:
        finding["remediation"] = _remediation_for(finding["dimension"])

    return {
        "metadata": {
            "start_year": start_year,
            "end_year": end_year,
            "league_types": "all (0-5)",
            "read_only": True,
            "pbp_limitation_threshold_pct": PBP_LIMITATION_COVERAGE_THRESHOLD,
            "team_code_null_alert_rate": TEAM_CODE_NULL_ALERT_RATE,
        },
        "summary": {
            "total_checks": len(findings),
            "ok": ok_count,
            "defects": len(defects),
            "known_limitations": len(known),
            "defect_counts_by_dimension": _count_by_dimension(defects),
        },
        "coverage_report": coverage_report,
        "findings": findings,
        "defects": defects,
        "known_limitations": known,
    }


def _count_by_dimension(findings: Sequence[dict[str, Any]]) -> dict[str, int]:
    """Tally defect counts grouped by dimension."""
    counts: dict[str, int] = defaultdict(int)
    for finding in findings:
        counts[finding["dimension"]] += finding["count"]
    return dict(sorted(counts.items()))


def render_markdown(report: dict[str, Any]) -> str:
    """Render a compact markdown summary of the audit report."""
    meta = report["metadata"]
    summary = report["summary"]
    lines = [
        "# KBO 데이터 전수 검증 리포트",
        "",
        f"- 범위: {meta['start_year']}–{meta['end_year']} (리그타입 {meta['league_types']})",
        "- 모드: read-only (DB 미변경)",
        "",
        "## 요약",
        f"- 전체 점검: {summary['total_checks']}",
        f"- 정상(OK): {summary['ok']}",
        f"- 결함(DEFECT/ERROR): {summary['defects']}",
        f"- 한계(KNOWN_LIMITATION): {summary['known_limitations']}",
        "",
        "### 결함 차원별 건수",
    ]
    for dim, count in summary["defect_counts_by_dimension"].items():
        lines.append(f"- {dim}: {count}")
    lines.append("")
    lines.append("## 결함 상세 (연도별)")
    for finding in report["defects"]:
        lines.append(
            f"- [{finding['year']}] {finding['dimension']} "
            f"({finding['classification']}, {finding['count']}): {finding['detail']}",
        )
        lines.append(f"    - 복구: {finding['remediation']}")
    lines.append("")
    lines.append("## 한계(KNOWN_LIMITATION) 요약")
    for finding in report["known_limitations"]:
        lines.append(f"- [{finding['year']}] {finding['dimension']}: {finding['detail']}")
    return "\n".join(lines)


def write_defect_csvs(report: dict[str, Any], output_dir: Path) -> list[Path]:
    """Write per-dimension CSV files listing defect sample IDs."""
    written: list[Path] = []
    by_dimension: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for finding in report["defects"]:
        if finding["sample_ids"]:
            by_dimension[finding["dimension"]].append(finding)
    for dimension, items in by_dimension.items():
        safe_name = dimension.replace(":", "_")
        path = output_dir / f"missing_{safe_name}.csv"
        lines = ["year,classification,count,detail,id"]
        for finding in items:
            for sid in finding["sample_ids"]:
                lines.append(
                    f'{finding["year"]},{finding["classification"]},{finding["count"]},"{finding["detail"]}",{sid}',
                )
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        written.append(path)
    return written


def main(argv: Sequence[str] | None = None) -> int:
    """Run the completeness audit CLI."""
    parser = argparse.ArgumentParser(description="Audit 2009-2025 KBO data completeness (read-only)")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    parser.add_argument("--end-year", type=int, default=DEFAULT_END_YEAR)
    parser.add_argument("--database-url", default=None, help="Local SQLite URL (defaults to DATABASE_URL)")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--json", action="store_true", help="Print the JSON report to stdout")
    args = parser.parse_args(argv)

    if args.start_year > args.end_year:
        parser.error("--start-year must not exceed --end-year")

    database_url = args.database_url or os.getenv("DATABASE_URL")
    if not database_url:
        parser.error("database URL is required via --database-url or DATABASE_URL")

    engine: Engine = create_engine_for_url(database_url)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)

    with engine.connect() as conn:
        report = build_master_report(
            conn,
            session_factory,
            start_year=args.start_year,
            end_year=args.end_year,
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.output_dir / "completeness_2009_2025_report.json"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path = args.output_dir / "completeness_2009_2025_summary.md"
    md_path.write_text(render_markdown(report), encoding="utf-8")
    csv_paths = write_defect_csvs(report, args.output_dir)

    if args.json:
        sys.stdout.write(json.dumps(report, ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(render_markdown(report) + "\n")
    sys.stdout.write(
        f"\nReports written: {json_path} | {md_path} | {len(csv_paths)} CSV file(s)\n",
    )

    summary = report["summary"]
    sys.stdout.write(
        f"RESULT: checks={summary['total_checks']} ok={summary['ok']} "
        f"defects={summary['defects']} known_limitations={summary['known_limitations']}\n",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
