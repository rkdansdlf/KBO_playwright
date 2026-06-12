#!/usr/bin/env python3
"""
Quality gate for local + OCI KBO game data.

Checks:
  - Baseline thresholds (no degradation)
  - Local/OCI metric parity
  - Local/OCI past-missing game-id set parity
  - No past SCHEDULED rows

CSV snapshots are written by default. Use --no-write for CI or agent
sessions that should not create artifact directories.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


import argparse
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.maintenance.full_audit import collect_audit_metrics, flatten_gate_metrics
from src.db.engine import SessionLocal

BASELINE_KEYS = (
    "past_missing_runs_max",
    "batting_null_player_id_max",
    "pitching_null_player_id_max",
    "lineups_null_player_id_max",
    "unresolved_missing_max",
    "orphaned_batting_stats_max",
    "orphaned_pitching_stats_max",
    "orphaned_lineups_max",
    "missing_player_profiles_max",
    "game_batting_duplicate_player_groups_max",
    "game_pitching_duplicate_player_groups_max",
    "game_lineups_duplicate_player_team_groups_max",
    "game_batting_player_team_collisions_max",
    "game_pitching_player_team_collisions_max",
    "game_lineups_player_team_collisions_max",
    "batting_hits_gt_at_bats_max",
    "batting_at_bats_gt_plate_appearances_max",
    "pitching_earned_runs_gt_runs_allowed_max",
    "pseudo_player_profiles_max",
)

STRICT_ZERO_KEYS = tuple(key.removesuffix("_max") for key in BASELINE_KEYS)


def load_baseline(path: Path) -> dict[str, int]:
    data = json.loads(path.read_text(encoding="utf-8"))
    missing = [key for key in BASELINE_KEYS if key not in data]
    if missing:
        raise ValueError(f"Baseline file missing keys: {missing}")
    return {k: int(v) for k, v in data.items()}


def collect_metrics(session_or_conn) -> dict[str, int]:
    metrics_sql = {
        "past_missing_runs": """
            SELECT COUNT(*) FROM game
            WHERE (home_score IS NULL OR away_score IS NULL)
              AND game_date < CURRENT_DATE
              AND COALESCE(game_status, '') NOT IN ('CANCELLED', 'POSTPONED')
        """,
        "batting_null_player_id": "SELECT COUNT(*) FROM game_batting_stats WHERE player_id IS NULL",
        "pitching_null_player_id": "SELECT COUNT(*) FROM game_pitching_stats WHERE player_id IS NULL",
        "lineups_null_player_id": "SELECT COUNT(*) FROM game_lineups WHERE player_id IS NULL",
        "orphaned_batting_stats": "SELECT COUNT(*) FROM game_batting_stats WHERE game_id NOT IN (SELECT game_id FROM game)",
        "orphaned_pitching_stats": "SELECT COUNT(*) FROM game_pitching_stats WHERE game_id NOT IN (SELECT game_id FROM game)",
        "missing_player_profiles": """
            WITH season_players AS (
                SELECT player_id FROM player_season_batting
                UNION
                SELECT player_id FROM player_season_pitching
            )
            SELECT COUNT(DISTINCT sp.player_id)
            FROM season_players sp
            LEFT JOIN player_basic p ON sp.player_id = p.player_id
            WHERE p.player_id IS NULL
               OR UPPER(TRIM(COALESCE(p.name, ''))) LIKE 'UNKNOWN %'
        """,
    }
    metrics: dict[str, int] = {}
    for key, sql in metrics_sql.items():
        metrics[key] = int(session_or_conn.execute(text(sql)).scalar() or 0)

    audit_report = collect_audit_metrics(session_or_conn)
    metrics.update(flatten_gate_metrics(audit_report))

    has_game_status = True
    try:
        session_or_conn.execute(text("SELECT game_status FROM game LIMIT 1")).fetchall()
    except BaseException:  # noqa: BLE001
        has_game_status = False

    metrics["game_status_column_present"] = int(has_game_status)
    if has_game_status:
        metrics["unresolved_missing"] = int(
            session_or_conn.execute(text("SELECT COUNT(*) FROM game WHERE game_status = 'UNRESOLVED_MISSING'")).scalar()
            or 0
        )
        metrics["past_scheduled"] = int(
            session_or_conn.execute(
                text("SELECT COUNT(*) FROM game WHERE game_status = 'SCHEDULED' AND game_date < CURRENT_DATE")
            ).scalar()
            or 0
        )
        metrics["live_no_evidence"] = int(
            session_or_conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM game g
                    WHERE g.game_status = 'LIVE'
                      AND NOT EXISTS (SELECT 1 FROM game_inning_scores gis WHERE gis.game_id = g.game_id)
                      AND NOT EXISTS (SELECT 1 FROM game_events ge WHERE ge.game_id = g.game_id)
                      AND NOT EXISTS (SELECT 1 FROM game_play_by_play pbp WHERE pbp.game_id = g.game_id)
                    """
                )
            ).scalar()
            or 0
        )
    else:
        metrics["unresolved_missing"] = 0
        metrics["past_scheduled"] = 0
    return metrics


def fetch_past_missing_game_ids(session_or_conn) -> set[str]:
    rows = session_or_conn.execute(
        text(
            """
            SELECT game_id
            FROM game
            WHERE (home_score IS NULL OR away_score IS NULL)
              AND game_date < CURRENT_DATE
              AND COALESCE(game_status, '') NOT IN ('CANCELLED', 'POSTPONED')
            """
        )
    ).fetchall()
    return {str(row[0]) for row in rows}


def evaluate_quality_gate(
    *,
    local_metrics: dict[str, int],
    oci_metrics: dict[str, int],
    baseline: dict[str, int],
    local_missing_ids: set[str],
    oci_missing_ids: set[str],
    strict_zero: bool = False,
) -> list[str]:
    failures: list[str] = []

    threshold_map = {
        "past_missing_runs_max": "past_missing_runs",
        "batting_null_player_id_max": "batting_null_player_id",
        "pitching_null_player_id_max": "pitching_null_player_id",
        "lineups_null_player_id_max": "lineups_null_player_id",
        "unresolved_missing_max": "unresolved_missing",
        "orphaned_batting_stats_max": "orphaned_batting_stats",
        "orphaned_pitching_stats_max": "orphaned_pitching_stats",
        "orphaned_lineups_max": "orphaned_lineups",
        "missing_player_profiles_max": "missing_player_profiles",
        "game_batting_duplicate_player_groups_max": "game_batting_duplicate_player_groups",
        "game_pitching_duplicate_player_groups_max": "game_pitching_duplicate_player_groups",
        "game_lineups_duplicate_player_team_groups_max": "game_lineups_duplicate_player_team_groups",
        "game_batting_player_team_collisions_max": "game_batting_player_team_collisions",
        "game_pitching_player_team_collisions_max": "game_pitching_player_team_collisions",
        "game_lineups_player_team_collisions_max": "game_lineups_player_team_collisions",
        "batting_hits_gt_at_bats_max": "batting_hits_gt_at_bats",
        "batting_at_bats_gt_plate_appearances_max": "batting_at_bats_gt_plate_appearances",
        "pitching_earned_runs_gt_runs_allowed_max": "pitching_earned_runs_gt_runs_allowed",
        "pseudo_player_profiles_max": "pseudo_player_profiles",
    }

    for baseline_key, metric_key in threshold_map.items():
        limit = int(baseline[baseline_key])
        local_val = int(local_metrics.get(metric_key, 0))
        oci_val = int(oci_metrics.get(metric_key, 0))
        if local_val > limit:
            failures.append(f"local {metric_key}={local_val} exceeds baseline {limit}")
        if oci_val > limit:
            failures.append(f"oci {metric_key}={oci_val} exceeds baseline {limit}")

    parity_keys = (
        "past_missing_runs",
        "batting_null_player_id",
        "pitching_null_player_id",
        "lineups_null_player_id",
        "unresolved_missing",
        "orphaned_batting_stats",
        "orphaned_pitching_stats",
        "orphaned_lineups",
        "missing_player_profiles",
        "live_no_evidence",
        "game_batting_duplicate_player_groups",
        "game_pitching_duplicate_player_groups",
        "game_lineups_duplicate_player_team_groups",
        "game_batting_player_team_collisions",
        "game_pitching_player_team_collisions",
        "game_lineups_player_team_collisions",
        "batting_hits_gt_at_bats",
        "batting_at_bats_gt_plate_appearances",
        "pitching_earned_runs_gt_runs_allowed",
        "pseudo_player_profiles",
    )
    for key in parity_keys:
        if int(local_metrics.get(key, 0)) != int(oci_metrics.get(key, 0)):
            failures.append(
                f"metric mismatch for {key}: local={local_metrics.get(key, 0)} oci={oci_metrics.get(key, 0)}"
            )

    if int(local_metrics.get("game_status_column_present", 1)) == 0:
        failures.append("local game_status column is missing")
    if int(oci_metrics.get("game_status_column_present", 1)) == 0:
        failures.append("oci game_status column is missing")

    if int(local_metrics.get("past_scheduled", 0)) > 0:
        failures.append(f"local past_scheduled={local_metrics['past_scheduled']} must be 0")
    if int(oci_metrics.get("past_scheduled", 0)) > 0:
        failures.append(f"oci past_scheduled={oci_metrics['past_scheduled']} must be 0")

    if int(local_metrics.get("live_no_evidence", 0)) > 0:
        failures.append(f"local live_no_evidence={local_metrics['live_no_evidence']} must be 0")
    if int(oci_metrics.get("live_no_evidence", 0)) > 0:
        failures.append(f"oci live_no_evidence={oci_metrics['live_no_evidence']} must be 0")

    if local_missing_ids != oci_missing_ids:
        failures.append(
            f"past missing game-id set mismatch: local_only={len(local_missing_ids - oci_missing_ids)} "
            f"oci_only={len(oci_missing_ids - local_missing_ids)}"
        )

    if strict_zero:
        for key in STRICT_ZERO_KEYS:
            local_val = int(local_metrics.get(key, 0))
            oci_val = int(oci_metrics.get(key, 0))
            if local_val > 0:
                failures.append(f"local {key}={local_val} must be 0 in strict-zero mode")
            if oci_val > 0:
                failures.append(f"oci {key}={oci_val} must be 0 in strict-zero mode")

    return failures


def _write_snapshot(path: Path, rows: Sequence[tuple[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(("metric", "count"))
        for key, value in rows:
            writer.writerow((key, value))


def _write_set_diff(path: Path, local_only: set[str], oci_only: set[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(("scope", "game_id"))
        for gid in sorted(local_only):
            writer.writerow(("local_only", gid))
        for gid in sorted(oci_only):
            writer.writerow(("oci_only", gid))


def run_quality_gate(
    *,
    baseline_path: Path,
    output_dir: Path,
    oci_url: str | None,
    skip_oci: bool = False,
    oci_only: bool = False,
    write_artifacts: bool = True,
    strict_zero: bool = False,
) -> dict[str, Any]:
    baseline = load_baseline(baseline_path)

    if oci_only:
        if not oci_url:
            raise RuntimeError("OCI_DB_URL is required for --oci-only")
        oci_engine = create_engine(oci_url)
        with oci_engine.connect() as oci_conn:
            oci_metrics = collect_metrics(oci_conn)
            oci_missing_ids = fetch_past_missing_game_ids(oci_conn)

        # In oci_only mode, we treat local as identical to OCI so parity checks pass
        local_metrics = dict(oci_metrics)
        local_missing_ids = set(oci_missing_ids)
    else:
        with SessionLocal() as local_session:
            local_metrics = collect_metrics(local_session)
            local_missing_ids = fetch_past_missing_game_ids(local_session)

        if skip_oci:
            oci_metrics = dict(local_metrics)
            oci_missing_ids = set(local_missing_ids)
        else:
            if not oci_url:
                raise RuntimeError("OCI_DB_URL is required unless --skip-oci is used")
            oci_engine = create_engine(oci_url)
            with oci_engine.connect() as oci_conn:
                oci_metrics = collect_metrics(oci_conn)
                oci_missing_ids = fetch_past_missing_game_ids(oci_conn)

    failures = evaluate_quality_gate(
        local_metrics=local_metrics,
        oci_metrics=oci_metrics,
        baseline=baseline,
        local_missing_ids=local_missing_ids,
        oci_missing_ids=oci_missing_ids,
        strict_zero=strict_zero,
    )

    local_snapshot: Path | None = None
    oci_snapshot: Path | None = None
    set_diff_csv: Path | None = None
    if write_artifacts:
        output_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_snapshot = output_dir / f"quality_gate_local_{stamp}.csv"
        oci_snapshot = output_dir / f"quality_gate_oci_{stamp}.csv"
        set_diff_csv = output_dir / f"quality_gate_missing_set_diff_{stamp}.csv"
        _write_snapshot(local_snapshot, list(local_metrics.items()))
        _write_snapshot(oci_snapshot, list(oci_metrics.items()))
        _write_set_diff(set_diff_csv, local_missing_ids - oci_missing_ids, oci_missing_ids - local_missing_ids)

    return {
        "ok": len(failures) == 0,
        "strict_zero": strict_zero,
        "failures": failures,
        "artifacts_written": write_artifacts,
        "local_snapshot": str(local_snapshot) if local_snapshot else None,
        "oci_snapshot": str(oci_snapshot) if oci_snapshot else None,
        "set_diff_csv": str(set_diff_csv) if set_diff_csv else None,
        "local_metrics": local_metrics,
        "oci_metrics": oci_metrics,
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    parser = argparse.ArgumentParser(description="Run local/OCI data quality gate")
    parser.add_argument(
        "--baseline",
        default="Docs/quality_gate_baseline.json",
        help="Path to baseline JSON file",
    )
    parser.add_argument("--output-dir", default="data", help="Directory to write output snapshots")
    parser.add_argument("--oci-url", default=None, help="Override OCI DB URL")
    parser.add_argument("--skip-oci", action="store_true", help="Run gate against local DB only")
    parser.add_argument("--oci-only", action="store_true", help="Run gate against OCI DB only (useful for CI)")
    parser.add_argument(
        "--no-write",
        "--no-artifacts",
        dest="write_artifacts",
        action="store_false",
        help="Run checks without creating output directories or CSV snapshot artifacts",
    )
    parser.add_argument("--strict-zero", action="store_true", help="Require all baseline-managed metrics to be zero")
    args = parser.parse_args()

    if args.skip_oci and args.oci_only:
        logger.error("Error: cannot use both --skip-oci and --oci-only")
        sys.exit(1)

    load_dotenv()
    oci_url = args.oci_url or os.getenv("OCI_DB_URL")
    result = run_quality_gate(
        baseline_path=Path(args.baseline),
        output_dir=Path(args.output_dir),
        oci_url=oci_url,
        skip_oci=args.skip_oci,
        oci_only=args.oci_only,
        write_artifacts=args.write_artifacts,
        strict_zero=args.strict_zero,
    )

    logger.info("✅ Quality gate finished")
    if result["artifacts_written"]:
        logger.info(f"   local snapshot: {result['local_snapshot']}")
        logger.info(f"   oci snapshot: {result['oci_snapshot']}")
        logger.info(f"   missing-set diff: {result['set_diff_csv']}")
    else:
        logger.info("   artifacts: disabled (--no-write)")
    if result["ok"]:
        logger.info("   status: PASS")
        return

    logger.info("   status: FAIL")
    for failure in result["failures"]:
        logger.info(f"   - {failure}")
    sys.exit(1)


if __name__ == "__main__":
    main()
