"""
Quality metric trend tracker.
Reads daily quality report JSONs to compute trends over time.
Detects metric degradation (recent trend worsening).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

QUALITY_REPORT_DIR = Path("logs/quality_reports")


class TrendTracker:
    def __init__(self, report_dir: str | Path = QUALITY_REPORT_DIR):
        self.report_dir = Path(report_dir)

    def load_reports(self, days: int = 30) -> list[dict]:
        reports = []
        cutoff = datetime.now() - timedelta(days=days)
        if not self.report_dir.exists():
            return reports
        for f in sorted(self.report_dir.glob("*.json")):
            try:
                report = json.loads(f.read_text(encoding="utf-8"))
                report_date = datetime.strptime(f.stem, "%Y%m%d")
                if report_date >= cutoff:
                    reports.append(report)
            except (json.JSONDecodeError, ValueError):
                continue
        return reports

    def get_trend(self, metric_key: str, days: int = 7) -> dict:
        reports = self.load_reports(days=days)
        values = []
        for r in reports:
            val = self._resolve_key(r, metric_key)
            if val is not None:
                values.append({"date": r.get("metrics", {}).get("date"), "value": val})

        direction = "stable"
        if len(values) >= 3:
            recent = [v["value"] for v in values[-3:]]
            if all(recent[i] <= recent[i + 1] for i in range(len(recent) - 1)):
                direction = "increasing"
            elif all(recent[i] >= recent[i + 1] for i in range(len(recent) - 1)):
                direction = "decreasing"

        return {"metric": metric_key, "values": values, "direction": direction}

    def detect_degradations(self, threshold_map: dict[str, float]) -> list[dict]:
        alerts = []
        reports = self.load_reports(days=14)
        if len(reports) < 2:
            return alerts

        first = reports[0]
        last = reports[-1]

        for metric_key, threshold in threshold_map.items():
            first_val = self._resolve_key(first, metric_key)
            last_val = self._resolve_key(last, metric_key)
            if first_val is not None and last_val is not None:
                pct_change = (last_val - first_val) / max(abs(first_val), 1) * 100
                if pct_change > threshold:
                    alerts.append(
                        {
                            "metric": metric_key,
                            "first": first_val,
                            "last": last_val,
                            "pct_change": round(pct_change, 1),
                            "severity": "WARN" if pct_change > threshold else "INFO",
                        }
                    )
        return alerts

    def _resolve_key(self, report: dict, dotted_key: str) -> float | None:
        parts = dotted_key.split(".")
        val: Any = report
        for p in parts:
            if isinstance(val, dict):
                val = val.get(p)
            else:
                return None
        if isinstance(val, (int, float)):
            return float(val)
        return None

    def print_trend_summary(self, days: int = 14):
        reports = self.load_reports(days=days)
        if not reports:
            print(f"[TrendTracker] No quality reports found in last {days} days.")
            return

        print(f"\n{'=' * 60}")
        print(f"  Quality Metric Trends (last {days} days)")
        print(f"{'=' * 60}")

        keys_to_track = [
            "metrics.completed_count",
            "metrics.relay_integrity.recent_missing_count",
            "metrics.relay_integrity.current_season_missing_count",
            "metrics.standings_integrity.ok",
            "quality_gate.ok",
        ]

        for key in keys_to_track:
            trend = self.get_trend(key, days=days)
            vals = trend["values"]
            if vals:
                first = vals[0]
                last = vals[-1]
                direction = (
                    "↑" if trend["direction"] == "increasing" else "↓" if trend["direction"] == "decreasing" else "→"
                )
                print(f"  {key:<50} {first['value']} → {last['value']} {direction}")

        deg = self.detect_degradations(
            {
                "metrics.relay_integrity.recent_missing_count": 50.0,
                "metrics.completed_count": -20.0,
            }
        )
        if deg:
            print("\n  ⚠️  Degradations detected:")
            for d in deg:
                print(f"    {d['metric']}: {d['pct_change']:+.1f}% ({d['severity']})")
        print()
