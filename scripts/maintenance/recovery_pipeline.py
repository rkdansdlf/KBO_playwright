"""Automated recovery pipeline for 2009-2025 KBO data defects.

This orchestrates the remediation commands surfaced by
``audit_completeness_2009_2025`` in dependency order:

1. schedule      - backfill missing parent games (crawl_schedule)
2. crawl_detail  - live-crawl lineups/boxscores for missing coverage (collect_games)
3. player_game   - recalculate player-game stats (recalc_player_game_stats)
4. backfill_ids  - resolve NULL player_ids (backfill_player_ids / backfill_pbp_player_ids)
5. season        - recalc season + team aggregates and quality gate (recalc_*_stats)
6. pa_formula    - fix PA formula gaps (audit_pa_formula / backfill_sh_sf_from_pbp)
7. verify        - re-run the completeness audit and report residual defects

Phases run sequentially and are resumable via a state file. By default the
pipeline runs in dry-run mode (it only prints the commands it would execute);
pass ``--apply`` to actually run them. All underlying commands are idempotent
(UPSERT), so re-running is safe.

Usage:
    python3 -m scripts.maintenance.recovery_pipeline --dry-run
    python3 -m scripts.maintenance.recovery_pipeline --apply --only-defect-years
    python3 -m scripts.maintenance.recovery_pipeline --apply --phase 2
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

DEFAULT_START_YEAR = 2009
DEFAULT_END_YEAR = 2025

AUDIT_JSON = Path("data/audit/completeness_2009_2025_report.json")
STATE_PATH = Path("data/recovery_state.json")

# Coverage tables whose gaps are filled by live-crawling collect_games.
COVERAGE_CRAWL_TABLES = ("game_lineups", "game_batting_stats", "game_pitching_stats")

PHASES: tuple[tuple[str, str], ...] = (
    ("schedule", "Backfill missing parent games (crawl_schedule)"),
    ("crawl_detail", "Live-crawl lineups/boxscores for missing coverage (collect_games)"),
    ("player_game", "Recalculate player-game stats (recalc_player_game_stats)"),
    ("backfill_ids", "Resolve NULL player_ids (backfill_player_ids / backfill_pbp_player_ids)"),
    ("season", "Recalc season + team aggregates (recalc_*_stats)"),
    ("pa_formula", "Fix PA formula gaps (audit_pa_formula / backfill_sh_sf_from_pbp)"),
    ("verify", "Re-run completeness audit and report residual defects"),
)


def _run_cli(cmd: Sequence[str], *, dry_run: bool, capture: bool = True) -> int:
    """Run a CLI module via the current interpreter.

    Args:
        cmd: Module path and arguments, e.g. ``["src.cli.collect_games", "--year", "2024"]``.
        dry_run: When True, only log the command instead of executing it.
        capture: When False, stream the subprocess output directly.

    Returns:
        The subprocess return code (0 when skipped under dry-run).

    """
    full = [sys.executable, "-m", *cmd]
    if dry_run:
        logger.info("[dry-run] would run: %s", " ".join(full))
        return 0
    logger.info("running: %s", " ".join(full))
    result = subprocess.run(full, check=False, capture_output=capture, text=True)
    if result.returncode != 0:
        tail = (result.stderr or result.stdout or "")[-800:]
        logger.error("FAILED (rc=%s): %s", result.returncode, tail)
    elif capture and result.stdout and result.stdout.strip():
        last = result.stdout.strip().splitlines()[-1]
        logger.info("ok: %s", last)
    return result.returncode


def _load_audit_report() -> dict[str, Any] | None:
    """Load the most recent completeness audit JSON if present."""
    if AUDIT_JSON.exists():
        return json.loads(AUDIT_JSON.read_text(encoding="utf-8"))
    return None


def _defect_years(report: dict[str, Any] | None) -> set[int]:
    """Return the set of years that have at least one defect."""
    years: set[int] = set()
    if not report:
        return years
    for finding in report.get("defects", []):
        try:
            years.add(int(finding["year"]))
        except (KeyError, TypeError, ValueError):
            continue
    return years


def _schedule_defect_years(report: dict[str, Any] | None) -> set[int]:
    """Return years flagged with missing_parent_games defects."""
    years: set[int] = set()
    if not report:
        return years
    for finding in report.get("defects", []):
        if finding.get("dimension") == "missing_parent_games":
            try:
                years.add(int(finding["year"]))
            except (KeyError, TypeError, ValueError):
                continue
    return years


def _crawl_months(report: dict[str, Any] | None, years: set[int]) -> dict[int, set[int]]:
    """Map each year to the set of months that have missing coverage rows."""
    months: dict[int, set[int]] = {}
    if not report:
        return months
    for year_report in report.get("coverage_report", {}).get("years", []):
        year = int(year_report["year"])
        if year not in years:
            continue
        for table in COVERAGE_CRAWL_TABLES:
            for game_id in year_report.get("missing_game_ids", {}).get(table, []):
                if len(game_id) >= 6 and game_id[:4].isdigit() and game_id[4:6].isdigit():
                    months.setdefault(year, set()).add(int(game_id[4:6]))
    return months


def phase_schedule(years: set[int], dry_run: bool, report: dict[str, Any] | None) -> int:
    """Phase 1: backfill missing parent games."""
    target = _schedule_defect_years(report) & years
    if not target:
        logger.info("schedule: no missing_parent_games defects in scope; skipping")
        return 0
    rc = 0
    for year in sorted(target):
        rc = _run_cli(["src.cli.crawl_schedule", "--year", str(year), "--months", "3-10"], dry_run=dry_run)
        if rc != 0:
            return rc
    return 0


def phase_crawl_detail(years: set[int], dry_run: bool, report: dict[str, Any] | None) -> int:
    """Phase 2: live-crawl lineups/boxscores for months with missing coverage."""
    months = _crawl_months(report, years)
    if not months:
        logger.info("crawl_detail: no missing-coverage months resolved from audit; skipping")
        return 0
    rc = 0
    for year in sorted(months):
        for month in sorted(months[year]):
            rc = _run_cli(
                ["src.cli.collect_games", "--year", str(year), "--month", str(month)],
                dry_run=dry_run,
                capture=False,
            )
            if rc != 0:
                return rc
    return 0


def phase_player_game(years: set[int], dry_run: bool, _report: dict[str, Any] | None) -> int:
    """Phase 3: recalculate player-game stats."""
    rc = 0
    for year in sorted(years):
        cmd = ["src.cli.recalc_player_game_stats", "--season", str(year)]
        if dry_run:
            cmd.append("--dry-run")
        rc = _run_cli(cmd, dry_run=dry_run)
        if rc != 0:
            return rc
    return 0


def phase_backfill_ids(years: set[int], dry_run: bool, _report: dict[str, Any] | None) -> int:
    """Phase 4: resolve NULL player_ids in game and relay tables."""
    if not years:
        return 0
    start = min(years)
    end = max(years)
    for module in ("scripts.maintenance.backfill_player_ids", "scripts.maintenance.backfill_pbp_player_ids"):
        cmd = [module, "--start", str(start), "--end", str(end)]
        if dry_run:
            cmd.append("--dry-run")
        rc = _run_cli(cmd, dry_run=dry_run)
        if rc != 0:
            return rc
    return 0


def phase_season(years: set[int], dry_run: bool, _report: dict[str, Any] | None) -> int:
    """Phase 5: recalculate season and team aggregates."""
    rc = 0
    for year in sorted(years):
        rc = _run_cli(["src.cli.recalc_player_stats", "--season", str(year)], dry_run=dry_run)
        if rc != 0:
            return rc
        rc = _run_cli(["src.cli.recalc_season_stats", "--year", str(year), "--save"], dry_run=dry_run)
        if rc != 0:
            return rc
        rc = _run_cli(["src.cli.recalc_team_stats", "--season", str(year)], dry_run=dry_run)
        if rc != 0:
            return rc
    return 0


def phase_pa_formula(years: set[int], dry_run: bool, _report: dict[str, Any] | None) -> int:
    """Phase 6: fix PA formula gaps from PBP and ratio-based fallback."""
    rc = 0
    for year in sorted(years):
        rc = _run_cli(
            ["scripts.maintenance.backfill_sh_sf_from_pbp", "--year", str(year)],
            dry_run=dry_run,
        )
        if rc != 0:
            return rc
        cmd = ["scripts.maintenance.audit_pa_formula", "--fix-year", str(year)]
        if dry_run:
            cmd.append("--dry-run")
        rc = _run_cli(cmd, dry_run=dry_run)
        if rc != 0:
            return rc
    return 0


def phase_verify(years: set[int], dry_run: bool, _report: dict[str, Any] | None) -> int:
    """Phase 7: re-run the completeness audit and report residual defects."""
    cmd = [
        "scripts.maintenance.audit_completeness_2009_2025",
        "--start-year",
        str(min(years)),
        "--end-year",
        str(max(years)),
        "--output-dir",
        "data/audit",
    ]
    return _run_cli(cmd, dry_run=dry_run, capture=False)


PHASE_FUNCS = {
    "schedule": phase_schedule,
    "crawl_detail": phase_crawl_detail,
    "player_game": phase_player_game,
    "backfill_ids": phase_backfill_ids,
    "season": phase_season,
    "pa_formula": phase_pa_formula,
    "verify": phase_verify,
}


def _load_state() -> dict[str, Any]:
    """Load the recovery state file, returning an empty state if absent."""
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning("state file corrupted; starting fresh")
    return {"completed": []}


def _save_state(state: dict[str, Any]) -> None:
    """Persist the recovery state file."""
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _scheduler_clear(force: bool) -> bool:
    """Return True if it is safe to run (no active scheduler), unless forced."""
    if force:
        return True
    result = subprocess.run(
        [sys.executable, "-m", "scripts.diagnose_scheduler_locks"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return True
    logger.error(
        "scheduler locks detected (diagnose_scheduler_locks rc=%s). Stop the scheduler or pass --force. stderr=%s",
        result.returncode,
        (result.stderr or "")[-400:],
    )
    return False


def _resolve_years(args: argparse.Namespace, report: dict[str, Any] | None) -> set[int]:
    """Resolve the set of years the pipeline will operate on."""
    all_years = set(range(args.start_year, args.end_year + 1))
    if not args.only_defect_years:
        return all_years
    defect = _defect_years(report)
    scoped = all_years & defect
    logger.info("only-defect-years: scoped to %d year(s)", len(scoped))
    return scoped


def _run_planned_phases(
    planned: list[str],
    years: set[int],
    dry_run: bool,
    state: dict[str, Any],
    resume: bool,
) -> int:
    """Execute the planned phases in order, updating state on success."""
    logger.info("pipeline start: phases=%s dry_run=%s years=%s", planned, dry_run, sorted(years))
    for phase in planned:
        if resume and phase in state.get("completed", []):
            logger.info("phase %s already completed; skipping", phase)
            continue
        logger.info("=== phase: %s ===", phase)
        rc = PHASE_FUNCS[phase](years, dry_run, _load_audit_report())
        if rc != 0:
            logger.error("phase %s failed (rc=%s); aborting", phase, rc)
            return rc
        if not dry_run:
            state.setdefault("completed", [])
            if phase not in state["completed"]:
                state["completed"].append(phase)
            _save_state(state)
        logger.info("phase %s done", phase)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the recovery pipeline CLI."""
    parser = argparse.ArgumentParser(description="Remediate 2009-2025 KBO data defects (dependency-ordered)")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    parser.add_argument("--end-year", type=int, default=DEFAULT_END_YEAR)
    parser.add_argument(
        "--phase", type=str, default=None, choices=[p[0] for p in PHASES], help="Run a single phase only"
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--apply", action="store_true", help="Actually execute (default is dry-run)")
    mode_group.add_argument("--dry-run", action="store_true", help="Print the planned commands without executing")
    parser.add_argument("--only-defect-years", action="store_true", help="Restrict to years with defects in the audit")
    parser.add_argument("--reset-state", action="store_true", help="Clear completed-phase state before running")
    parser.add_argument("--force", action="store_true", help="Run even if the scheduler holds locks")
    args = parser.parse_args(argv)

    if args.start_year > args.end_year:
        parser.error("--start-year must not exceed --end-year")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    dry_run = not args.apply

    report = _load_audit_report()
    if report is None:
        logger.warning("audit report %s not found; run audit_completeness_2009_2025 first", AUDIT_JSON)

    years = _resolve_years(args, report)
    if not _scheduler_clear(args.force):
        return 1

    state = _load_state()
    if args.reset_state:
        state["completed"] = []
    planned = [args.phase] if args.phase else [p[0] for p in PHASES]
    resume = not args.phase and not args.reset_state

    rc = _run_planned_phases(planned, years, dry_run, state, resume)
    logger.info("pipeline finished (dry_run=%s, rc=%s)", dry_run, rc)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
