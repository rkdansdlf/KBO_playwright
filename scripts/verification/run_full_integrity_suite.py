#!/usr/bin/env python3
"""
KBO Unified Data Integrity Audit Runner.
Executes referential, logical, and statistical checks across local SQLite DB
(and optionally remote OCI PostgreSQL DB) and compiles results into a Markdown report.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import logging  # noqa: E402

from scripts.legacy.maintenance.quality_gate import run_quality_gate  # noqa: E402

from scripts.verification.audit_game_logic import audit_game_logic  # noqa: E402
from scripts.verification.check_orphan_data import collect_report  # noqa: E402
from src.db.engine import SessionLocal  # noqa: E402
from src.models.game import Game  # noqa: E402
from src.validators.standings_integrity import validate_standings_integrity  # noqa: E402

logger = logging.getLogger(__name__)


def get_latest_game_date() -> date | None:
    with SessionLocal() as session:
        latest_game = session.query(Game).order_by(Game.game_date.desc()).first()
        return latest_game.game_date if latest_game else None


def format_report_md(
    timestamp: datetime,
    orphan_results: dict[str, Any],
    logic_violations: list[dict[str, Any]],
    qgate_results: dict[str, Any],
    standings_results: list[dict[str, Any]],
    strict_mode: bool,
) -> str:
    md = []
    md.append("# KBO Data Integrity Verification Report")
    md.append(f"Generated at: `{timestamp.strftime('%Y-%m-%d %H:%M:%S')}`")
    md.append(f"Strict Mode: `{'ON' if strict_mode else 'OFF'}`")
    md.append("")

    # Summary Table
    logic_ok = len(logic_violations) == 0
    orphan_ok = orphan_results.get("ok", False)
    qgate_ok = qgate_results.get("ok", False)
    standings_ok = all(s.get("ok", False) for s in standings_results)

    overall_ok = logic_ok and orphan_ok and qgate_ok and standings_ok

    md.append("## Executive Summary")
    status_emoji = "✅ PASS" if overall_ok else "❌ FAIL"
    md.append(f"**Overall Status**: {status_emoji}")
    md.append("")
    md.append("| Verification Module | Status | Details |")
    md.append("| --- | --- | --- |")
    md.append(
        f"| Referential Gaps (Orphans) | {'✅ PASS' if orphan_ok else '❌ FAIL'} | {len(orphan_results.get('checks', []))} checks executed |"
    )
    md.append(
        f"| Mathematical Game Logic | {'✅ PASS' if logic_ok else '❌ FAIL'} | {len(logic_violations)} violations detected |"
    )
    md.append(
        f"| Quality Gate Baseline | {'✅ PASS' if qgate_ok else '❌ FAIL'} | {len(qgate_results.get('failures', []))} threshold violations |"
    )
    md.append(
        f"| Standings Rollup Integrity | {'✅ PASS' if standings_ok else '❌ FAIL'} | Checked standings on {len(standings_results)} dates |"
    )
    md.append("")

    md.append("---")

    # 1. Referential Gaps
    md.append("## 1. Referential & Orphan Data Gaps")
    md.append(f"Target Database: `{orphan_results.get('database')}`")
    md.append("")
    md.append("| Check Name | Status | Rows Count | Distinct Count | Severity |")
    md.append("| --- | --- | --- | --- | --- |")
    for check in orphan_results.get("checks", []):
        chk_status = "✅ PASS" if check["status"] == "PASS" else ("⚠️ WARN" if check["status"] == "WARN" else "❌ FAIL")
        md.append(
            f"| {check['name']} | {chk_status} | {check['row_count']} | {check['distinct_count']} | {check['severity']} |"
        )
    md.append("")

    # Show orphan samples
    failed_checks = [c for c in orphan_results.get("checks", []) if c["status"] in ("FAIL", "ERROR")]
    if failed_checks:
        md.append("### Orphan Samples")
        for check in failed_checks:
            md.append(f"- **{check['name']}**: {check['row_count']} row(s) found.")
            if check.get("samples"):
                md.append("  Samples: " + ", ".join(f"`{s}`" for s in check["samples"][:10]))
        md.append("")

    md.append("---")

    # 2. Game Logic Violations
    md.append("## 2. Mathematical Game Logic Violations")
    if logic_violations:
        md.append("| Game ID | Date | Failure Reason |")
        md.append("| --- | --- | --- |")
        for v in logic_violations:
            md.append(f"| `{v['game_id']}` | {v['game_date']} | {v['reason']} |")
    else:
        md.append("✅ No game logic violations (Score totals vs innings, PA formula bounds, or ER bounds) detected.")
    md.append("")

    md.append("---")

    # 3. Quality Gate
    md.append("## 3. Quality Gate Thresholds")
    if qgate_results.get("failures"):
        md.append("### ❌ Quality Gate Failures")
        for f in qgate_results["failures"]:
            md.append(f"- {f}")
    else:
        md.append("✅ All database size & profile metrics are within historical quality gate thresholds.")
    md.append("")

    md.append("---")

    # 4. Standings Rollup
    md.append("## 4. Team Standings Rollup Integrity")
    md.append("Compares KBO daily standings snapshot tables against rolled-up results from the `game` table.")
    md.append("")

    for s_res in standings_results:
        dt = s_res["checked_date"]
        s_emoji = "✅ PASS" if s_res["ok"] else "❌ FAIL"
        md.append(f"### Date: `{dt}` - Status: {s_emoji}")
        if s_res.get("note"):
            md.append(f"_{s_res['note']}_")

        if s_res.get("mismatches"):
            md.append("| Team | Issue | Differences / Details |")
            md.append("| --- | --- | --- |")
            for m in s_res["mismatches"]:
                team = m["team_code"]
                issue = m["issue"]
                if issue == "value_mismatch":
                    diff_strs = []
                    for f, d in m["differences"].items():
                        diff_strs.append(f"{f}: Expected {d['expected']} vs Actual {d['actual']}")
                    md.append(f"| `{team}` | Value Mismatch | {', '.join(diff_strs)} |")
                else:
                    md.append(f"| `{team}` | {issue} | - |")
        md.append("")

    return "\n".join(md)


def main():
    parser = argparse.ArgumentParser(description="Run Full KBO Data Integrity Verification Suite")
    parser.add_argument("--year", type=int, help="Limit check to a specific year/season")
    parser.add_argument("--strict-zero", action="store_true", help="Require all baseline metrics to be zero")
    parser.add_argument("--skip-oci", action="store_true", help="Skip remote OCI database comparison")
    parser.add_argument(
        "--standings-days", type=int, default=5, help="Number of random historic days to verify standings integrity"
    )
    args = parser.parse_args()

    load_dotenv()
    timestamp = datetime.now()

    logger.info("🚀 Running KBO Data Integrity Audit Suite...")
    logger.info("-" * 50)

    db_path = Path("data/kbo_dev.db")

    # 1. Referential & Orphan Checks
    logger.info("\n1️⃣ Running Referential & Orphan Checks...")
    orphan_results = collect_report(db_path, sample_limit=20)
    logger.info(f"   Status: {'PASS' if orphan_results['ok'] else 'FAIL'}")

    # 2. Game Logic Checks
    logger.info("\n2️⃣ Running Game Logic Checks...")
    logic_violations = audit_game_logic(year=args.year)
    logger.info(f"   Status: {'PASS' if not logic_violations else 'FAIL'} ({len(logic_violations)} violations)")

    # 3. Quality Gate Checks
    logger.info("\n3️⃣ Running Quality Gate Checks...")
    baseline_path = Path("Docs/quality_gate_baseline.json")
    oci_url = os.getenv("OCI_DB_URL")

    qgate_results = run_quality_gate(
        baseline_path=baseline_path,
        output_dir=Path("data"),
        oci_url=oci_url,
        skip_oci=args.skip_oci or not oci_url,
        oci_only=False,
        write_artifacts=True,
        strict_zero=args.strict_zero,
    )
    logger.info(f"   Status: {'PASS' if qgate_results['ok'] else 'FAIL'}")

    # 4. Standings Rollup Checks
    logger.info("\n4️⃣ Running Standings Rollup Integrity...")
    standings_results = []

    # Get latest game date
    latest_dt = get_latest_game_date()
    if latest_dt:
        logger.info(f"   - Validating standings for latest game date: {latest_dt.isoformat()}")
        with SessionLocal() as session:
            latest_res = validate_standings_integrity(session, latest_dt)
            standings_results.append(latest_res)

            # Select random completed game dates for sample checking
            all_game_dates = [r[0] for r in session.query(Game.game_date).distinct().all() if r[0] != latest_dt]
            if all_game_dates and args.standings_days > 0:
                sample_count = min(len(all_game_dates), args.standings_days)
                sampled_dates = random.sample(all_game_dates, sample_count)
                logger.info(f"   - Validating standings for {sample_count} random past dates...")
                for d in sampled_dates:
                    res = validate_standings_integrity(session, d)
                    standings_results.append(res)
    else:
        logger.info("   ⚠️ No games found in database to perform standings validation.")

    # 5. Format & Save Report
    logger.info("\n📝 Generating report...")
    report_md = format_report_md(
        timestamp=timestamp,
        orphan_results=orphan_results,
        logic_violations=logic_violations,
        qgate_results=qgate_results,
        standings_results=standings_results,
        strict_mode=args.strict_zero,
    )

    report_dir = Path("reports")
    report_dir.mkdir(parents=True, exist_ok=True)
    report_filename = f"integrity_report_{timestamp.strftime('%Y%m%d_%H%M%S')}.md"
    report_path = report_dir / report_filename

    report_path.write_text(report_md, encoding="utf-8")

    logger.info("-" * 50)
    logger.info("🎉 Audit Suite completed!")
    logger.info(f"💾 Report saved to: {report_path}")
    logger.info("-" * 50)

    # Exit code based on overall status
    overall_ok = (
        orphan_results.get("ok", False)
        and len(logic_violations) == 0
        and qgate_results.get("ok", False)
        and all(s.get("ok", False) for s in standings_results)
    )

    if not overall_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
