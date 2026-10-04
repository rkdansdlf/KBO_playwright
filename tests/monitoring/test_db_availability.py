"""Tests for the Prometheus database availability/latency collector."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from sqlalchemy import create_engine

from src.monitoring.db_availability import DatabaseAvailabilityCollector, probe_engine

ROOT = Path(__file__).resolve().parents[2]


class _BrokenEngine:
    def connect(self):
        raise OSError("connection refused")


class TestProbeEngine:
    def test_available_engine_reports_available(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        available, latency = probe_engine(engine)
        assert available is True
        assert latency >= 0.0

    def test_broken_engine_reports_unavailable(self) -> None:
        available, latency = probe_engine(_BrokenEngine())  # type: ignore[arg-type]
        assert available is False
        assert latency >= 0.0


class TestCollector:
    def test_collect_yields_both_gauges(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        collector = DatabaseAvailabilityCollector(engine)

        families = {family.name: family for family in collector.collect()}

        assert families["kbo_db_available"].samples[0].value == 1.0
        assert families["kbo_db_ping_latency_seconds"].samples[0].value >= 0.0

    def test_collect_reports_zero_when_down(self) -> None:
        collector = DatabaseAvailabilityCollector(_BrokenEngine())  # type: ignore[arg-type]
        families = {family.name: family for family in collector.collect()}
        assert families["kbo_db_available"].samples[0].value == 0.0


class TestPrometheusConfig:
    def test_database_alert_rules_present(self) -> None:
        rules = yaml.safe_load((ROOT / "monitoring/prometheus/alert_rules.yml").read_text(encoding="utf-8"))
        names = {rule["alert"] for group in rules["groups"] for rule in group["rules"]}
        assert "KBODatabaseUnavailable" in names
        assert "KBODatabaseLatencyHigh" in names

    def test_api_server_is_scraped(self) -> None:
        config = yaml.safe_load((ROOT / "monitoring/prometheus/prometheus.yml").read_text(encoding="utf-8"))
        jobs = {job["job_name"] for job in config["scrape_configs"]}
        assert "kbo-api-server" in jobs

    def test_alertmanager_sends_resolved(self) -> None:
        config = yaml.safe_load((ROOT / "monitoring/alertmanager/alertmanager.yml").read_text(encoding="utf-8"))
        for receiver in config["receivers"]:
            for telegram in receiver["telegram_configs"]:
                assert telegram["send_resolved"] is True


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
