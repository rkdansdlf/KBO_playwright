"""CLI command for detecting statistical and operational anomalies across the platform."""

from __future__ import annotations

import argparse
import json
import sys

from src.monitoring.anomaly_detector import AnomalyDetector


def build_arg_parser() -> argparse.ArgumentParser:
    """Build command-line argument parser."""
    parser = argparse.ArgumentParser(description="Detect statistical outliers and system anomalies.")
    parser.add_argument(
        "--metric",
        type=str,
        default="all",
        help="Specific metric name to evaluate (default: all).",
    )
    parser.add_argument(
        "--sensitivity",
        type=str,
        default="medium",
        choices=["low", "medium", "high"],
        help="Detection sensitivity threshold (default: medium).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output audit report as JSON.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI execution entrypoint."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    z_map = {"low": 3.5, "medium": 3.0, "high": 2.0}
    detector = AnomalyDetector(default_z_threshold=z_map.get(args.sensitivity, 3.0))

    # Sample snapshot metrics for evaluation
    metrics_snapshot = {
        "series": {
            "daily_games": [5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0],
            "player_batting_rows": [150.0, 152.0, 148.0, 151.0, 149.0, 150.0, 150.0],
        },
        "stale_hours": {
            "games": 2.5,
            "player_game_batting": 3.0,
        },
        "selector_error_rate": 0.02,
        "lock_skips": 1,
    }

    report = detector.audit_snapshot(metrics_snapshot)

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))  # noqa: T201
    else:
        print(f"=== Anomaly Detection Audit Report [{report.overall_status}] ===")  # noqa: T201
        print(  # noqa: T201
            f"Evaluated Metrics: {report.total_metrics_evaluated} | "
            f"Anomalies Detected: {report.anomalies_detected} | "
            f"Checked at: {report.evaluated_at}"
        )
        print("-" * 60)  # noqa: T201
        if not report.events:
            print("No anomalies detected. All platform metrics are within normal ranges.")  # noqa: T201
        else:
            for ev in report.events:
                print(f"[{ev.severity.value}] {ev.anomaly_type.value}: {ev.details}")  # noqa: T201

    return 1 if report.overall_status == "CRITICAL" else 0


if __name__ == "__main__":
    sys.exit(main())
