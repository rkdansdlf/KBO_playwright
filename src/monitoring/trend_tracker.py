"""
Quality metric trend tracker.
Reads daily quality report JSONs to compute trends over time.
Detects metric degradation (recent trend worsening).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src.utils.alerting import SlackWebhookClient

logger = logging.getLogger(__name__)

QUALITY_REPORT_DIR = Path("logs/quality_reports")


class TrendTracker:
    def __init__(self, report_dir: str | Path = QUALITY_REPORT_DIR) -> None:
        self.report_dir = Path(report_dir)

    def load_reports(self, days: int = 30) -> list[dict]:
        reports_by_date: dict[str, dict] = {}
        cutoff = datetime.now() - timedelta(days=days)
        if not self.report_dir.exists():
            return []
        for f in sorted(self.report_dir.glob("*.json")):
            try:
                report = json.loads(f.read_text(encoding="utf-8"))
                report_date = self._extract_report_date(report, f)
                if report_date >= cutoff:
                    report["_report_date"] = report_date
                    report["_report_path"] = str(f)
                    key = report_date.strftime("%Y%m%d")
                    current = reports_by_date.get(key)
                    if current is None or self._generated_at_key(report) >= self._generated_at_key(current):
                        reports_by_date[key] = report
            except (json.JSONDecodeError, ValueError):
                continue
        return sorted(reports_by_date.values(), key=lambda r: r["_report_date"])

    def get_trend(self, metric_key: str, days: int = 7) -> dict[str, Any]:
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

    def detect_degradations(self, threshold_map: dict[str, float], days: int = 14) -> list[dict]:
        alerts = []
        reports = self.load_reports(days=days)
        if len(reports) < 2:
            return alerts

        first = reports[0]
        last = reports[-1]

        for metric_key, threshold in threshold_map.items():
            first_val = self._resolve_key(first, metric_key)
            last_val = self._resolve_key(last, metric_key)
            if first_val is not None and last_val is not None:
                pct_change = (last_val - first_val) / max(abs(first_val), 1) * 100
                is_degraded = (threshold > 0 and pct_change > threshold) or (threshold < 0 and pct_change < threshold)
                if is_degraded:
                    alerts.append(
                        {
                            "metric": metric_key,
                            "first": first_val,
                            "last": last_val,
                            "pct_change": round(pct_change, 1),
                            "severity": "WARN",
                        },
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

    def _extract_report_date(self, report: dict, path: Path) -> datetime:
        raw_date = (report.get("metrics") or {}).get("date")
        if isinstance(raw_date, str):
            normalized = raw_date.replace("-", "")
            if len(normalized) == 8 and normalized.isdigit():
                return datetime.strptime(normalized, "%Y%m%d")
        return datetime.strptime(path.stem, "%Y%m%d")

    def _generated_at_key(self, report: dict) -> str:
        generated_at = report.get("generated_at")
        if isinstance(generated_at, str):
            return generated_at
        return ""

    def print_trend_summary(self, days: int = 14) -> None:
        reports = self.load_reports(days=days)
        if not reports:
            logger.info("[TrendTracker] No quality reports found in last %s days.", days)
            return

        logger.info(f"\n{'=' * 60}")
        logger.info("  Quality Metric Trends (last %s days)", days)
        logger.info(f"{'=' * 60}")

        keys_to_track = [
            "metrics.completed_count",
            "metrics.relay_integrity.recent_missing_count",
            "metrics.relay_integrity.current_season_missing_count",
            "metrics.standings_integrity.ok",
            "metrics.pa_formula_integrity.violation_count",
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
                logger.info(f"  {key:<50} {first['value']} → {last['value']} {direction}")

        deg = self.detect_degradations(
            {
                "metrics.relay_integrity.recent_missing_count": 50.0,
            },
            days=days,
        )
        if deg:
            logger.warning("\n  ⚠️  Degradations detected:")
            for d in deg:
                logger.info(f"    {d['metric']}: {d['pct_change']:+.1f}% ({d['severity']})")
        logger.info("")

    def send_degradation_alert(self, days: int = 14) -> None:
        """
        Detect metric degradations over the last `days` days and send an alert
        via Telegram/Slack if any are found. Stays quiet when everything is healthy.
        """
        default_thresholds = {
            "metrics.relay_integrity.recent_missing_count": 50.0,  # +50% increase in missing PBP
            "metrics.pa_formula_integrity.violation_count": 0.0,  # Any increase in violations
        }
        degradations = self.detect_degradations(default_thresholds, days=days)
        if not degradations:
            return  # Nothing to alert about

        lines = ""
        for d in degradations:
            arrow = "↗" if d["pct_change"] > 0 else "↘"
            lines += (
                f"  {arrow} {d['metric']}\n    {d['first']} → {d['last']} ({d['pct_change']:+.1f}%) [{d['severity']}]\n"
            )

        msg = f"<b>📉 KBO 데이터 품질 열화 감지 (최근 {days}일)</b>\n\n{lines}\n상세 확인이 필요합니다."
        SlackWebhookClient.send_alert(msg)
