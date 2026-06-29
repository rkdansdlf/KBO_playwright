"""Small invariant pack for crawler data quality regressions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import inspect, text

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.engine import Connection
    from sqlalchemy.engine.reflection import Inspector


@dataclass(frozen=True)
class QualityRegressionResult:
    """QualityRegressionResult class."""

    check_id: str
    description: str
    status: str
    violation_count: int
    message: str
    sample_ids: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """
        Handle the to dict operation.

        Returns:
            Dictionary result.

        """
        return {
            "check_id": self.check_id,
            "description": self.description,
            "status": self.status,
            "violation_count": self.violation_count,
            "message": self.message,
            "sample_ids": list(self.sample_ids),
        }


@dataclass(frozen=True)
class QualityRegressionReport:
    """QualityRegressionReport class."""

    results: tuple[QualityRegressionResult, ...]

    @property
    def ok(self) -> bool:
        """
        Handle the ok operation.

        Returns:
            True if successful, False otherwise.

        """
        return all(result.status != "fail" for result in self.results)

    @property
    def check_count(self) -> int:
        """
        Check count.

        Returns:
            Integer result.

        """
        return len(self.results)

    @property
    def failure_count(self) -> int:
        """
        Handle the failure count operation.

        Returns:
            Integer result.

        """
        return sum(1 for result in self.results if result.status == "fail")

    def to_dict(self) -> dict[str, Any]:
        """
        Handle the to dict operation.

        Returns:
            Dictionary result.

        """
        return {
            "ok": self.ok,
            "check_count": self.check_count,
            "failure_count": self.failure_count,
            "results": [result.to_dict() for result in self.results],
        }


@dataclass(frozen=True)
class _SqlCheck:
    check_id: str
    description: str
    table: str
    required_columns: tuple[str, ...]
    count_sql: str
    sample_sql: str


_CHECKS: tuple[_SqlCheck, ...] = (
    _SqlCheck(
        check_id="game_batting_pa_formula",
        description="Game batting PA equals AB + BB + HBP + SH + SF",
        table="game_batting_stats",
        required_columns=(
            "game_id",
            "player_id",
            "plate_appearances",
            "at_bats",
            "walks",
            "hbp",
            "sacrifice_hits",
            "sacrifice_flies",
        ),
        count_sql="""
            SELECT COUNT(*)
            FROM game_batting_stats
            WHERE COALESCE(plate_appearances, 0) !=
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
        """,
        sample_sql="""
            SELECT COALESCE(CAST(game_id AS TEXT), '')
            FROM game_batting_stats
            WHERE COALESCE(plate_appearances, 0) !=
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="player_season_batting_pa_formula",
        description="Season batting PA equals AB + BB + HBP + SH + SF",
        table="player_season_batting",
        required_columns=(
            "player_id",
            "plate_appearances",
            "at_bats",
            "walks",
            "hbp",
            "sacrifice_hits",
            "sacrifice_flies",
            "league",
            "source",
        ),
        count_sql="""
            SELECT COUNT(*)
            FROM player_season_batting
            WHERE league = 'REGULAR' AND source = 'AGGREGATED'
              AND COALESCE(plate_appearances, 0) !=
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
        """,
        sample_sql="""
            SELECT COALESCE(CAST(player_id AS TEXT), 'NULL')
            FROM player_season_batting
            WHERE league = 'REGULAR' AND source = 'AGGREGATED'
              AND COALESCE(plate_appearances, 0) !=
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="game_batting_hits_not_gt_at_bats",
        description="Game batting hits do not exceed at-bats",
        table="game_batting_stats",
        required_columns=("game_id", "player_id", "hits", "at_bats"),
        count_sql="""
            SELECT COUNT(*)
            FROM game_batting_stats
            WHERE COALESCE(hits, 0) > COALESCE(at_bats, 0)
        """,
        sample_sql="""
            SELECT COALESCE(CAST(game_id AS TEXT), '')
            FROM game_batting_stats
            WHERE COALESCE(hits, 0) > COALESCE(at_bats, 0)
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="game_pitching_earned_runs_not_gt_runs_allowed",
        description="Game pitching earned runs do not exceed runs allowed",
        table="game_pitching_stats",
        required_columns=("game_id", "player_id", "earned_runs", "runs_allowed"),
        count_sql="""
            SELECT COUNT(*)
            FROM game_pitching_stats
            WHERE COALESCE(earned_runs, 0) > COALESCE(runs_allowed, 0)
        """,
        sample_sql="""
            SELECT COALESCE(CAST(game_id AS TEXT), '')
            FROM game_pitching_stats
            WHERE COALESCE(earned_runs, 0) > COALESCE(runs_allowed, 0)
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="game_batting_null_player_id",
        description="Game batting stat rows should resolve player_id",
        table="game_batting_stats",
        required_columns=("game_id", "player_id"),
        count_sql="SELECT COUNT(*) FROM game_batting_stats WHERE player_id IS NULL",
        sample_sql="""
            SELECT COALESCE(CAST(game_id AS TEXT), '')
            FROM game_batting_stats
            WHERE player_id IS NULL
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="game_pitching_null_player_id",
        description="Game pitching stat rows should resolve player_id",
        table="game_pitching_stats",
        required_columns=("game_id", "player_id"),
        count_sql="SELECT COUNT(*) FROM game_pitching_stats WHERE player_id IS NULL",
        sample_sql="""
            SELECT COALESCE(CAST(game_id AS TEXT), '')
            FROM game_pitching_stats
            WHERE player_id IS NULL
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="game_lineups_null_player_id",
        description="Lineup rows should resolve player_id",
        table="game_lineups",
        required_columns=("game_id", "player_id"),
        count_sql="SELECT COUNT(*) FROM game_lineups WHERE player_id IS NULL",
        sample_sql="""
            SELECT COALESCE(CAST(game_id AS TEXT), '')
            FROM game_lineups
            WHERE player_id IS NULL
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="batting_avg_range",
        description="Season batting average should be between 0.0 and 1.0",
        table="player_season_batting",
        required_columns=("batting_avg",),
        count_sql="SELECT COUNT(*) FROM player_season_batting WHERE batting_avg < 0 OR batting_avg > 1.0",
        sample_sql="""
            SELECT COALESCE(CAST(player_id AS TEXT), ''), batting_avg
            FROM player_season_batting
            WHERE batting_avg < 0 OR batting_avg > 1.0
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="era_range",
        description="Season ERA should be between 0.0 and 30.0",
        table="player_season_pitching",
        required_columns=("era",),
        count_sql="SELECT COUNT(*) FROM player_season_pitching WHERE era < 0 OR era > 30.0",
        sample_sql="""
            SELECT COALESCE(CAST(player_id AS TEXT), ''), era
            FROM player_season_pitching
            WHERE era < 0 OR era > 30.0
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="hits_lte_at_bats_season",
        description="Season hits should not exceed at-bats",
        table="player_season_batting",
        required_columns=("hits", "at_bats"),
        count_sql="SELECT COUNT(*) FROM player_season_batting WHERE hits > at_bats AND at_bats > 0",
        sample_sql="""
            SELECT COALESCE(CAST(player_id AS TEXT), ''), hits, at_bats
            FROM player_season_batting
            WHERE hits > at_bats AND at_bats > 0
            LIMIT 5
        """,
    ),
)


def run_regression_pack(conn: Connection, checks: Sequence[_SqlCheck] = _CHECKS) -> QualityRegressionReport:
    """
    Run data quality invariants against a SQLAlchemy connection.

    Args:
        conn: Conn.
        checks: Checks.
        conn: Conn.
        checks: Checks.

    """
    inspector = inspect(conn)

    table_names = set(inspector.get_table_names())
    results = tuple(_run_check(conn, inspector, table_names, check) for check in checks)
    return QualityRegressionReport(results=results)


def render_regression_report(report: QualityRegressionReport) -> str:
    """
    Report render regression.

    Args:
        report: Report.
        report: Report.
        report: Report.

    Returns:
        String result.

    """
    lines = [
        f"Data quality regression pack: {'PASS' if report.ok else 'FAIL'}",
        f"Checks: {report.check_count}",
        f"Failures: {report.failure_count}",
    ]
    for result in report.results:
        lines.append(f"- {result.check_id}: {result.status} ({result.violation_count} violation(s))")
        if result.message:
            lines.append(f"  {result.message}")
        if result.sample_ids:
            lines.append(f"  Samples: {', '.join(result.sample_ids)}")
    return "\n".join(lines)


def report_to_json(report: QualityRegressionReport) -> str:
    """
    Report to json.

    Args:
        report: Report.
        report: Report.
        report: Report.

    Returns:
        String result.

    """
    return json.dumps(report.to_dict(), ensure_ascii=False)


def _run_check(
    conn: Connection,
    inspector: Inspector,
    table_names: set[str],
    check: _SqlCheck,
) -> QualityRegressionResult:
    if check.table not in table_names:
        return QualityRegressionResult(
            check_id=check.check_id,
            description=check.description,
            status="skipped",
            violation_count=0,
            message=f"missing table: {check.table}",
        )

    columns = {column["name"] for column in inspector.get_columns(check.table)}
    missing_columns = [column for column in check.required_columns if column not in columns]
    if missing_columns:
        return QualityRegressionResult(
            check_id=check.check_id,
            description=check.description,
            status="skipped",
            violation_count=0,
            message=f"missing columns on {check.table}: {', '.join(missing_columns)}",
        )

    violation_count = int(conn.execute(text(check.count_sql)).scalar_one())
    sample_ids = tuple(str(row[0]) for row in conn.execute(text(check.sample_sql)).all())
    status = "pass" if violation_count == 0 else "fail"
    message = "ok" if status == "pass" else f"{violation_count} violation(s) found"
    return QualityRegressionResult(
        check_id=check.check_id,
        description=check.description,
        status=status,
        violation_count=violation_count,
        message=message,
        sample_ids=sample_ids,
    )
