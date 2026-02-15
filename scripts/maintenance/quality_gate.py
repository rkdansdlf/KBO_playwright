#!/usr/bin/env python3
"""
Quality gate for local + OCI KBO game data.

Checks:
  - Baseline thresholds (no degradation)
  - Local/OCI metric parity
  - Local/OCI past-missing game-id set parity
  - No past SCHEDULED rows
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal

BASELINE_KEYS = (
    "past_missing_runs_max",
    "batting_null_player_id_max",
    "pitching_null_player_id_max",
    "lineups_null_player_id_max",
    "unresolved_missing_max",
)


def load_baseline(path: Path) -> Dict[str, int]:
    data = json.loads(path.read_text(encoding="utf-8"))
    missing = [key for key in BASELINE_KEYS if key not in data]
    if missing:
        raise ValueError(f"Baseline file missing keys: {missing}")
    return {k: int(v) for k, v in data.items()}


def collect_metrics(session_or_conn) -> Dict[str, int]:
    metrics_sql = {
        "past_missing_runs": """
            SELECT COUNT(*) FROM game
            WHERE (home_score IS NULL OR away_score IS NULL)
              AND game_date <= CURRENT_DATE
        """,
        "batting_null_player_id": "SELECT COUNT(*) FROM game_batting_stats WHERE player_id IS NULL",
        "pitching_null_player_id": "SELECT COUNT(*) FROM game_pitching_stats WHERE player_id IS NULL",
        "lineups_null_player_id": "SELECT COUNT(*) FROM game_lineups WHERE player_id IS NULL",
    }
    metrics: Dict[str, int] = {}
    for key, sql in metrics_sql.items():
        metrics[key] = int(session_or_conn.execute(text(sql)).scalar() or 0)

    has_game_status = True
    try:
        session_or_conn.execute(text("SELECT game_status FROM game LIMIT 1")).fetchall()
    except Exception:
        has_game_status = False

    metrics["game_status_column_present"] = int(has_game_status)
    if has_game_status:
        metrics["unresolved_missing"] = int(
            session_or_conn.execute(text("SELECT COUNT(*) FROM game WHERE game_status = 'UNRESOLVED_MISSING'")).scalar()
            or 0
        )
        metrics["past_scheduled"] = int(
            session_or_conn.execute(
                text("SELECT COUNT(*) FROM game WHERE game_status = 'SCHEDULED' AND game_date <= CURRENT_DATE")
            ).scalar()
            or 0
        )
    else:
        metrics["unresolved_missing"] = 0
        metrics["past_scheduled"] = 0
    return metrics


def fetch_past_missing_game_ids(session_or_conn) -> Set[str]:
    rows = session_or_conn.execute(
        text(
            """
            SELECT game_id
            FROM game
            WHERE (home_score IS NULL OR away_score IS NULL)
              AND game_date <= CURRENT_DATE
            """
        )
    ).fetchall()
    return {str(row[0]) for row in rows}


def evaluate_quality_gate(
    *,
    local_metrics: Dict[str, int],
    oci_metrics: Dict[str, int],
    baseline: Dict[str, int],
    local_missing_ids: Set[str],
    oci_missing_ids: Set[str],
) -> List[str]:
    failures: List[str] = []

    threshold_map = {
        "past_missing_runs_max": "past_missing_runs",
        "batting_null_player_id_max": "batting_null_player_id",
        "pitching_null_player_id_max": "pitching_null_player_id",
        "lineups_null_player_id_max": "lineups_null_player_id",
        "unresolved_missing_max": "unresolved_missing",
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

    if local_missing_ids != oci_missing_ids:
        failures.append(
            f"past missing game-id set mismatch: local_only={len(local_missing_ids - oci_missing_ids)} "
            f"oci_only={len(oci_missing_ids - local_missing_ids)}"
        )

    return failures


def _write_snapshot(path: Path, rows: Sequence[Tuple[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(("metric", "count"))
        for key, value in rows:
            writer.writerow((key, value))


def _write_set_diff(path: Path, local_only: Set[str], oci_only: Set[str]) -> None:
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
) -> Dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    baseline = load_baseline(baseline_path)

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

    local_rows = list(local_metrics.items())
    oci_rows = list(oci_metrics.items())
    local_snapshot = output_dir / f"quality_gate_local_{stamp}.csv"
    oci_snapshot = output_dir / f"quality_gate_oci_{stamp}.csv"
    _write_snapshot(local_snapshot, local_rows)
    _write_snapshot(oci_snapshot, oci_rows)

    failures = evaluate_quality_gate(
        local_metrics=local_metrics,
        oci_metrics=oci_metrics,
        baseline=baseline,
        local_missing_ids=local_missing_ids,
        oci_missing_ids=oci_missing_ids,
    )

    set_diff_csv = output_dir / f"quality_gate_missing_set_diff_{stamp}.csv"
    _write_set_diff(set_diff_csv, local_missing_ids - oci_missing_ids, oci_missing_ids - local_missing_ids)

    return {
        "ok": len(failures) == 0,
        "failures": failures,
        "local_snapshot": str(local_snapshot),
        "oci_snapshot": str(oci_snapshot),
        "set_diff_csv": str(set_diff_csv),
        "local_metrics": local_metrics,
        "oci_metrics": oci_metrics,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run local/OCI data quality gate")
    parser.add_argument(
        "--baseline",
        default="Docs/quality_gate_baseline.json",
        help="Path to baseline JSON file",
    )
    parser.add_argument("--output-dir", default="data", help="Directory to write output snapshots")
    parser.add_argument("--oci-url", default=None, help="Override OCI DB URL")
    parser.add_argument("--skip-oci", action="store_true", help="Run gate against local DB only")
    args = parser.parse_args()

    load_dotenv()
    oci_url = args.oci_url or os.getenv("OCI_DB_URL")
    result = run_quality_gate(
        baseline_path=Path(args.baseline),
        output_dir=Path(args.output_dir),
        oci_url=oci_url,
        skip_oci=args.skip_oci,
    )

    print("✅ Quality gate finished")
    print(f"   local snapshot: {result['local_snapshot']}")
    print(f"   oci snapshot: {result['oci_snapshot']}")
    print(f"   missing-set diff: {result['set_diff_csv']}")
    if result["ok"]:
        print("   status: PASS")
        return

    print("   status: FAIL")
    for failure in result["failures"]:
        print(f"   - {failure}")
    sys.exit(1)


if __name__ == "__main__":
    main()
