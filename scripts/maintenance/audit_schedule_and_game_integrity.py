"""P2 Data Integrity Audit Script for KBO Crawled Schedule and Game Data.

Performs cross-validation across:
1. Monthly game counts and status distribution per season.
2. Cancelled and suspended games preservation (CANCELLED, SUSPENDED).
3. Team code normalization and season brand alignment.
4. Game and GameIdAlias duplication and referential integrity.
5. Crawl execution ledger failure classification (EMPTY vs FAILURE).

Read-only: does not modify the database. Emits markdown and json reports.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
import json
import logging
from pathlib import Path
import sys
from typing import Any

from sqlalchemy import extract, select
from sqlalchemy.orm import Session

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal
from src.models.crawl_execution import CrawlExecutionRun
from src.models.game import Game, GameIdAlias
from src.utils.game_status import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_POSTPONED,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_SUSPENDED,
)
from src.utils.team_codes import season_team_code

logger = logging.getLogger(__name__)


@dataclass
class MonthSummary:
    """Summary of games for a specific year and month."""

    year: int
    month: int
    total_games: int = 0
    completed: int = 0
    scheduled: int = 0
    cancelled: int = 0
    suspended: int = 0
    postponed: int = 0
    other: int = 0


@dataclass
class AuditReport:
    """Consolidated findings of the P2 data integrity audit."""

    audit_timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    target_year: int | None = None
    total_games_inspected: int = 0
    monthly_summaries: list[dict[str, Any]] = field(default_factory=list)
    cancelled_games_count: int = 0
    suspended_games_count: int = 0
    cancelled_with_invalid_scores: list[str] = field(default_factory=list)
    duplicate_games: list[dict[str, Any]] = field(default_factory=list)
    dangling_aliases: list[str] = field(default_factory=list)
    self_referencing_aliases: list[str] = field(default_factory=list)
    team_code_mismatches: list[dict[str, Any]] = field(default_factory=list)
    crawl_execution_summary: dict[str, Any] = field(default_factory=dict)
    anomalies: list[str] = field(default_factory=list)
    ok: bool = True


def _record_status(game: Game, summary: MonthSummary) -> tuple[int, int, bool]:
    status = (game.game_status or "").upper()
    is_cancelled = status == GAME_STATUS_CANCELLED
    is_suspended = status == GAME_STATUS_SUSPENDED
    invalid_score = False

    if status == GAME_STATUS_COMPLETED:
        summary.completed += 1
    elif status == GAME_STATUS_SCHEDULED:
        summary.scheduled += 1
    elif is_cancelled:
        summary.cancelled += 1
        invalid_score = game.home_score is not None and game.away_score is not None
    elif is_suspended:
        summary.suspended += 1
    elif status == GAME_STATUS_POSTPONED:
        summary.postponed += 1
    else:
        summary.other += 1

    return int(is_cancelled), int(is_suspended), invalid_score


def audit_monthly_counts_and_status(
    session: Session,
    year: int | None,
) -> tuple[list[MonthSummary], list[str], int, int, list[str]]:
    """Audit monthly distribution, cancelled/suspended preservation, and score integrity."""
    query = select(Game)
    if year:
        query = query.where(extract("year", Game.game_date) == year)

    games = session.scalars(query.order_by(Game.game_date)).all()

    month_buckets: dict[tuple[int, int], MonthSummary] = {}
    cancelled_count = 0
    suspended_count = 0
    cancelled_with_invalid_scores: list[str] = []
    anomalies: list[str] = []

    for game in games:
        gdate = game.game_date
        key = (gdate.year, gdate.month)
        if key not in month_buckets:
            month_buckets[key] = MonthSummary(year=gdate.year, month=gdate.month)

        summary = month_buckets[key]
        summary.total_games += 1

        is_c, is_s, invalid = _record_status(game, summary)
        cancelled_count += is_c
        suspended_count += is_s
        if invalid:
            cancelled_with_invalid_scores.append(game.game_id)

    # In-season plausibility check (April through September)
    for (y, m), s in sorted(month_buckets.items()):
        if 4 <= m <= 9 and s.total_games == 0:
            anomalies.append(f"In-season month with 0 games detected: {y}-{m:02d}")

    summaries = [month_buckets[k] for k in sorted(month_buckets.keys())]
    return summaries, anomalies, cancelled_count, suspended_count, cancelled_with_invalid_scores


def audit_team_code_mappings(session: Session, year: int | None) -> list[dict[str, Any]]:
    """Verify that team codes match their season-specific canonical brand."""
    query = select(Game)
    if year:
        query = query.where(extract("year", Game.game_date) == year)

    games = session.scalars(query).all()
    mismatches: list[dict[str, Any]] = []

    for game in games:
        season_year = game.game_date.year
        home = game.home_team
        away = game.away_team

        norm_home = season_team_code(home, season_year)
        norm_away = season_team_code(away, season_year)

        if home and norm_home and home != norm_home:
            mismatches.append(
                {
                    "game_id": game.game_id,
                    "date": game.game_date.isoformat(),
                    "side": "home",
                    "stored": home,
                    "expected": norm_home,
                }
            )
        if away and norm_away and away != norm_away:
            mismatches.append(
                {
                    "game_id": game.game_id,
                    "date": game.game_date.isoformat(),
                    "side": "away",
                    "stored": away,
                    "expected": norm_away,
                }
            )

    return mismatches


def audit_game_and_alias_duplicates(
    session: Session, year: int | None
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    """Check for duplicate games and GameIdAlias referential integrity."""
    # 1. Duplicate games by (date, home, away, doubleheader suffix)
    query = select(Game)
    if year:
        query = query.where(extract("year", Game.game_date) == year)

    games = session.scalars(query).all()
    fixture_groups: dict[tuple[date, str | None, str | None, str], list[str]] = defaultdict(list)

    for game in games:
        dh_suffix = game.game_id[-1] if len(game.game_id) >= 11 else "0"
        key = (game.game_date, game.home_team, game.away_team, dh_suffix)
        fixture_groups[key].append(game.game_id)

    duplicates: list[dict[str, Any]] = []
    for (gdate, home, away, dh), gids in fixture_groups.items():
        if len(gids) > 1:
            duplicates.append(
                {
                    "date": gdate.isoformat(),
                    "home": home,
                    "away": away,
                    "doubleheader": dh,
                    "game_ids": gids,
                }
            )

    # 2. Alias integrity checks
    alias_query = select(GameIdAlias)
    aliases = session.scalars(alias_query).all()
    all_game_ids = {g.game_id for g in games}

    dangling_aliases: list[str] = []
    self_referencing_aliases: list[str] = []

    for alias in aliases:
        if alias.alias_game_id == alias.canonical_game_id:
            self_referencing_aliases.append(alias.alias_game_id)
        if alias.canonical_game_id not in all_game_ids and year is None:
            dangling_aliases.append(f"{alias.alias_game_id} -> {alias.canonical_game_id}")

    return duplicates, dangling_aliases, self_referencing_aliases


def audit_crawl_executions(session: Session, year: int | None) -> dict[str, Any]:
    """Audit crawl execution history and error classification from crawl_execution_runs."""
    query = select(CrawlExecutionRun)
    if year:
        query = query.where(CrawlExecutionRun.season == year)

    runs = session.scalars(query).all()
    by_status: Counter[str] = Counter()
    by_crawler: Counter[str] = Counter()
    failures_by_reason: Counter[str] = Counter()
    total_written = 0
    total_failed = 0

    for r in runs:
        by_status[r.status] += 1
        by_crawler[r.crawler] += 1
        total_written += r.records_written
        total_failed += r.records_failed
        if r.error_code:
            failures_by_reason[r.error_code] += 1
        elif r.records_failed > 0:
            failures_by_reason["unspecified_record_failure"] += 1

    return {
        "total_runs": len(runs),
        "by_status": dict(by_status),
        "by_crawler": dict(by_crawler),
        "failures_by_reason": dict(failures_by_reason),
        "total_records_written": total_written,
        "total_records_failed": total_failed,
    }


def run_full_audit(year: int | None = None) -> AuditReport:
    """Execute all P2 integrity audit checks."""
    report = AuditReport(target_year=year)

    with SessionLocal() as session:
        (
            monthly_summaries,
            month_anomalies,
            cancelled_count,
            suspended_count,
            cancelled_with_invalid_scores,
        ) = audit_monthly_counts_and_status(session, year)

        report.monthly_summaries = [asdict(m) for m in monthly_summaries]
        report.total_games_inspected = sum(m.total_games for m in monthly_summaries)
        report.cancelled_games_count = cancelled_count
        report.suspended_games_count = suspended_count
        report.cancelled_with_invalid_scores = cancelled_with_invalid_scores
        report.anomalies.extend(month_anomalies)

        report.team_code_mismatches = audit_team_code_mappings(session, year)
        if report.team_code_mismatches:
            report.anomalies.append(f"Found {len(report.team_code_mismatches)} team code brand mismatches")

        duplicates, dangling, self_ref = audit_game_and_alias_duplicates(session, year)
        report.duplicate_games = duplicates
        report.dangling_aliases = dangling
        report.self_referencing_aliases = self_ref
        if duplicates:
            report.anomalies.append(f"Found {len(duplicates)} duplicate game groups")
        if dangling:
            report.anomalies.append(f"Found {len(dangling)} dangling aliases")

        report.crawl_execution_summary = audit_crawl_executions(session, year)

    if report.cancelled_with_invalid_scores or report.dangling_aliases or report.duplicate_games:
        report.ok = False

    return report


def format_markdown_report(report: AuditReport) -> str:
    """Format the audit report as Markdown for operators and runbooks."""
    lines: list[str] = [
        "# P2 Data Integrity & Cross-Validation Audit Report",
        "",
        f"- **Audit Timestamp**: `{report.audit_timestamp}`",
        f"- **Target Year**: `{report.target_year or 'All Years'}`",
        f"- **Total Games Inspected**: `{report.total_games_inspected}`",
        f"- **Audit Status**: **{'PASSED' if report.ok else 'ATTENTION REQUIRED'}**",
        "",
        "## 1. Monthly Game Distribution & Status",
        "",
        "| Year | Month | Total | Completed | Scheduled | Cancelled | Suspended | Postponed | Other |",
        "| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ]

    for m in report.monthly_summaries:
        lines.append(
            f"| {m['year']} | {m['month']:02d} | {m['total_games']} | {m['completed']} | "
            f"{m['scheduled']} | {m['cancelled']} | {m['suspended']} | {m['postponed']} | {m['other']} |"
        )

    lines.extend(
        [
            "",
            "## 2. Cancelled & Suspended Games Preservation",
            "",
            f"- **Total Cancelled Games**: `{report.cancelled_games_count}`",
            f"- **Total Suspended Games**: `{report.suspended_games_count}`",
            f"- **Cancelled Games with Invalid Completed Scores**: `{len(report.cancelled_with_invalid_scores)}`",
        ]
    )
    if report.cancelled_with_invalid_scores:
        lines.append("  - Violating game IDs: " + ", ".join(report.cancelled_with_invalid_scores[:10]))

    lines.extend(
        [
            "",
            "## 3. Duplicate Games & Alias Referential Integrity",
            "",
            f"- **Duplicate Game Fixture Groups**: `{len(report.duplicate_games)}`",
            f"- **Dangling GameIdAliases**: `{len(report.dangling_aliases)}`",
            f"- **Self-Referencing Aliases**: `{len(report.self_referencing_aliases)}`",
            f"- **Team Code Brand Mismatches**: `{len(report.team_code_mismatches)}`",
            "",
            "## 4. Crawl Execution Ledger Summary",
            "",
            f"- **Total Crawl Runs Recorded**: `{report.crawl_execution_summary.get('total_runs', 0)}`",
            f"- **Runs by Status**: `{json.dumps(report.crawl_execution_summary.get('by_status', {}))}`",
            f"- **Failure Reasons**: `{json.dumps(report.crawl_execution_summary.get('failures_by_reason', {}))}`",
            f"- **Records Written**: `{report.crawl_execution_summary.get('total_records_written', 0)}`",
            f"- **Records Failed**: `{report.crawl_execution_summary.get('total_records_failed', 0)}`",
        ]
    )

    if report.anomalies:
        lines.extend(
            [
                "",
                "## 5. Detected Anomalies",
                "",
            ]
        )
        for a in report.anomalies:
            lines.append(f"- ⚠️ {a}")
    else:
        lines.extend(
            [
                "",
                "## 5. Detected Anomalies",
                "",
                "No data integrity anomalies detected.",
            ]
        )

    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for running the P2 integrity audit."""
    parser = argparse.ArgumentParser(description="KBO P2 Data Integrity & Cross-Validation Audit")
    parser.add_argument("--year", type=int, default=None, help="Optional season year filter (e.g. 2025)")
    parser.add_argument("--json", action="store_true", help="Output results in JSON format")
    parser.add_argument("--report", type=str, default=None, help="Path to write markdown report file")

    args = parser.parse_args(argv)
    report = run_full_audit(args.year)

    if args.json:
        print(json.dumps(asdict(report), indent=2, ensure_ascii=False))
    else:
        md = format_markdown_report(report)
        print(md)

    if args.report:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(format_markdown_report(report), encoding="utf-8")
        logger.info("Report written to %s", report_path)

    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
